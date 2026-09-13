//! Shutdown signal: a watch channel workers observe to unwind in-flight work
//! on purpose, rather than being dropped mid-await.
//!
//! Dropping a future stops it wherever it sits, leaving a poll's gauge
//! incremented and its outcome unrecorded. A watch channel fits: tokio ships
//! it, the latest value wins, and a late reader still sees it.

use tokio::sync::watch;

/// The sending half. Dropping it also triggers, so a forgotten sender cannot
/// leave workers waiting for a signal that will never come.
#[derive(Debug)]
pub struct Trigger {
    /// Set to true on shutdown.
    tx: watch::Sender<bool>,
}

/// The receiving half, cloneable into every worker.
#[derive(Clone, Debug)]
pub struct Shutdown {
    /// Watches for the signal.
    rx: watch::Receiver<bool>,
}

impl Trigger {
    /// A fresh trigger and the first receiver.
    #[must_use]
    pub fn new() -> (Self, Shutdown) {
        let (tx, rx) = watch::channel(false);
        (Self { tx }, Shutdown { rx })
    }

    /// Ask everyone holding a [`Shutdown`] to stop.
    ///
    /// Idempotent, because more than one thing can notice a process is ending
    /// and none of them should have to coordinate about who says so.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }
}

impl Drop for Trigger {
    fn drop(&mut self) {
        self.trigger();
    }
}

impl Shutdown {
    /// A shutdown that never fires, for callers that do not manage a lifecycle.
    ///
    /// Tests and one-shot tools have no signal to give and should not invent one.
    #[must_use]
    pub fn never() -> Self {
        let (_, rx) = watch::channel(false);
        Self { rx }
    }

    /// Whether the signal has already been given.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolve once the signal is given, immediately if it already was.
    ///
    /// Safe to race in a `select!`: it holds nothing across the await, so losing
    /// the race costs nothing and the arm can be polled again.
    pub async fn triggered(&self) {
        // Err means every sender is gone, which is itself shutdown.
        drop(self.rx.clone().wait_for(|&fired| fired).await);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_worker_started_before_the_signal_sees_it() {
        let (trigger, shutdown) = Trigger::new();
        assert!(!shutdown.is_triggered(), "quiet to begin with");
        let waiting = tokio::spawn(async move { shutdown.triggered().await });
        trigger.trigger();
        assert!(waiting.await.is_ok(), "the waiter woke");
    }

    #[tokio::test]
    async fn a_worker_started_after_the_signal_sees_it_too() {
        // The race that makes a naive notify wrong: a task spawned during
        // shutdown would otherwise wait forever for an edge it missed.
        let (trigger, shutdown) = Trigger::new();
        trigger.trigger();
        assert!(shutdown.is_triggered(), "the level is still set");
        shutdown.triggered().await;
    }

    #[tokio::test]
    async fn triggering_twice_is_harmless() {
        let (trigger, shutdown) = Trigger::new();
        trigger.trigger();
        trigger.trigger();
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn dropping_the_trigger_counts_as_shutdown() {
        // Otherwise a process that unwinds without saying so leaves every
        // worker waiting on a signal nobody is left to send.
        let (trigger, shutdown) = Trigger::new();
        drop(trigger);
        shutdown.triggered().await;
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn a_shutdown_that_never_fires_does_not_report_one() {
        assert!(!Shutdown::never().is_triggered());
    }
}
