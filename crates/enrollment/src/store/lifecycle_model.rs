//! Random walks of identity lifecycle events against the memory store, with the
//! lifecycle invariants checked after every step.
#![expect(
    clippy::expect_used,
    clippy::panic,
    clippy::struct_excessive_bools,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    reason = "a test model: a broken invariant fails the walk with its trace"
)]

use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};

use super::{Inner, Issued, MemoryStore, NewSiteToken, Renewal, SeedRecord, StoreError, renewal::RECORD_SKEW};

/// Leaf lifetime.
const LIFETIME: Duration = Duration::days(30);
/// How far a leaf's `notBefore` is backdated.
const BACKDATE: Duration = Duration::minutes(5);
/// The token-enrolled site.
const SPOKE: &str = "spoke";
/// The reserved site.
const HUB: &str = "hub";
/// Walks.
const WALKS: u64 = 20_000;
/// Events per walk.
const STEPS: usize = 40;

/// A leaf, by key.
#[derive(Clone, Debug)]
struct Leaf {
    key: String,
    not_before: OffsetDateTime,
    not_after: OffsetDateTime,
    /// The grid CA that issued it.
    ca: u64,
}

/// One site: what its real holder has, and what a thief has.
#[derive(Debug, Default)]
struct Site {
    /// The real holder's current leaf.
    leaf: Option<Leaf>,
    /// The real holder's renewal in flight.
    pending: Option<String>,
    /// Leaves a thief holds.
    stolen: Vec<Leaf>,
    /// A thief holds a leaf that is not from before the last recovery.
    compromised: bool,
    /// A database restore or a lapse stranded it, which has a documented recovery.
    stranded: bool,
    /// When the last documented recovery ran.
    recovered_at: Option<OffsetDateTime>,
    /// A hub re-issue whose seed is not applied yet, and when bootstrap issued it.
    awaiting: Option<u64>,
    reissued_at: Option<OffsetDateTime>,
    /// Someone replayed an old seed since the last re-issue applied, which can delay it.
    replayed: bool,
    /// The highest seed generation seen on its record.
    generation: Option<u64>,
}

/// The model.
struct World {
    store: MemoryStore,
    rng: u64,
    keys: u64,
    sites: BTreeMap<&'static str, Site>,
    /// The seed the service would read.
    configmap: Option<SeedRecord>,
    /// Every seed bootstrap ever signed.
    signed: Vec<SeedRecord>,
    snapshot: Option<Inner>,
    trace: Vec<String>,
    /// The grid CA the service signs with and the TLS layer trusts.
    ca: u64,
    /// The CA whose key the key Secret holds, `None` once it is lost.
    ca_key: Option<u64>,
    /// The CA distributed in the bundle.
    bundle: Option<u64>,
}

impl World {
    fn new(walk: u64) -> Self {
        let mut world = Self {
            store: MemoryStore::default(),
            rng: walk.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            keys: 0,
            sites: BTreeMap::from([(SPOKE, Site::default()), (HUB, Site::default())]),
            configmap: None,
            signed: Vec::new(),
            snapshot: None,
            trace: Vec::new(),
            ca: 1,
            ca_key: Some(1),
            bundle: Some(1),
        };
        world.enroll(SPOKE);
        world.reissue_hub();
        world.apply_seed();
        world
    }

    fn now(&self) -> OffsetDateTime {
        self.store.now()
    }

    fn next(&mut self, bound: u64) -> u64 {
        self.rng ^= self.rng.wrapping_shl(13);
        self.rng ^= self.rng.wrapping_shr(7);
        self.rng ^= self.rng.wrapping_shl(17);
        self.rng.checked_rem(bound).unwrap_or(0)
    }

    fn fresh_key(&mut self) -> String {
        self.keys = self.keys.wrapping_add(1);
        format!("{:064x}", self.keys)
    }

    fn leaf(&self, key: String) -> Leaf {
        let now = self.now();
        Leaf {
            key,
            not_before: now.saturating_sub(BACKDATE),
            not_after: now.saturating_add(LIFETIME),
            ca: self.ca,
        }
    }

    fn site(&mut self, name: &str) -> &mut Site {
        self.sites.get_mut(name).expect("site")
    }

    fn record(&self, name: &str) -> Option<(super::Held, bool)> {
        self.store.snapshot().issued_names.get(name).cloned()
    }

    fn frozen(&self, name: &str) -> bool {
        self.record(name).is_some_and(|(held, _)| held.frozen)
    }

    fn fail(&self, why: &str) -> ! {
        panic!("{why}\nwalk:\n  {}", self.trace.join("\n  "));
    }

    /// A token enrollment, the spoke's first and its documented recovery.
    fn enroll(&mut self, name: &'static str) {
        let key = self.fresh_key();
        let digest = self.fresh_key();
        let now = self.now();
        self.store
            .mint_site_token(NewSiteToken {
                token_sha256: digest.clone(),
                site_name: name.to_owned(),
                grid_network_ref: "grid".to_owned(),
                issued_by: "model".to_owned(),
                expires_at: now.saturating_add(Duration::hours(1)),
            })
            .expect("mint");
        let issued = key.clone();
        self.store
            .redeem_and_issue(&digest, |_pin| Ok(cert(&issued)))
            .expect("redeem");
        let leaf = self.leaf(key);
        let site = self.site(name);
        site.leaf = Some(leaf);
        site.pending = None;
    }

    /// Bootstrap issues a new hub identity and signs its seed, as on install or after
    /// the identity Secret is deleted.
    fn reissue_hub(&mut self) {
        let key = self.fresh_key();
        let leaf = self.leaf(key.clone());
        let generation = self.next_generation();
        let seed = SeedRecord {
            site_name: HUB.to_owned(),
            key_sha256: key,
            generation,
            issued_at: leaf.not_before,
        };
        self.configmap = Some(seed.clone());
        self.signed.push(seed);
        let site = self.site(HUB);
        site.reissued_at = Some(leaf.not_before.saturating_add(BACKDATE));
        site.leaf = Some(leaf);
        site.pending = None;
        site.awaiting = Some(generation);
    }

    /// Bootstrap's generation: past the seed it holds and past the clock. Bootstrap
    /// runs one Job at a time, at least a second apart.
    fn next_generation(&self) -> u64 {
        self.store.advance(Duration::seconds(1));
        let held = self.configmap.as_ref().map_or(0, |seed| seed.generation);
        let now_ms = u64::try_from(self.now().unix_timestamp_nanos() / 1_000_000).unwrap_or(0);
        held.saturating_add(1).max(now_ms)
    }

    /// The service's reapply: the seed the mounted Secret holds, for the reserved name.
    fn apply_seed(&mut self) {
        let Some(seed) = self.configmap.clone() else {
            return;
        };
        self.store.seed_reserved(&seed).expect("seed");
        let Some((held, _)) = self.record(HUB) else {
            self.fail("the reserved record vanished");
        };
        let hub = self.sites.get(HUB).expect("hub");
        if hub.generation.is_some_and(|seen| held.seed_generation < Some(seen)) {
            self.fail("the seed generation moved backwards");
        }
        let (awaiting, key) = (hub.awaiting, hub.leaf.as_ref().map(|leaf| leaf.key.clone()));
        if awaiting.is_some() && held.seed_generation == awaiting {
            if held.frozen || Some(&held.current_key) != key.as_ref() {
                self.fail("a hub re-issue did not end unfrozen with its fresh key");
            }
            let hub = self.site(HUB);
            hub.awaiting = None;
            hub.replayed = false;
            let at = hub.reissued_at.unwrap_or_else(|| self.now());
            self.recovered(HUB, at);
        }
        self.site(HUB).generation = held.seed_generation;
    }

    /// The real holder renews if its leaf is due, the way the operator does.
    fn real_renew(&mut self, name: &'static str, lose_response: bool) {
        let now = self.now();
        let Some(leaf) = self.sites[name].leaf.clone() else {
            return;
        };
        if now >= leaf.not_after {
            self.site(name).stranded = true;
            return;
        }
        if now < leaf.not_after.saturating_sub(LIFETIME / 3) {
            return;
        }
        if leaf.ca != self.ca {
            let site = &self.sites[name];
            if !site.compromised && !site.stranded {
                self.fail(&format!("a healthy {name} was cut off by a CA change"));
            }
            return;
        }
        let requested = match self.sites[name].pending.clone() {
            Some(pending) => pending,
            None => self.fresh_key(),
        };
        self.site(name).pending = Some(requested.clone());
        let renewal = Renewal {
            site_name: name.to_owned(),
            presented_key: leaf.key,
            requested_key: requested.clone(),
            presented_not_before: leaf.not_before,
        };
        let issued = requested.clone();
        match self.store.renew_and_issue(&renewal, || Ok(cert(&issued))) {
            Ok(_) if lose_response => {},
            Ok(_) => {
                let renewed = self.leaf(requested);
                let site = self.site(name);
                site.leaf = Some(renewed);
                site.pending = None;
            },
            Err(StoreError::Refused(reason)) => {
                let site = &self.sites[name];
                let healthy = !site.compromised && !site.stranded;
                // A replayed seed can keep a hub re-issue from registering: denial, not takeover.
                if healthy && !site.replayed {
                    self.fail(&format!("a healthy {name} was refused: {reason:?}"));
                }
                let thief_holds = self
                    .record(name)
                    .is_some_and(|(held, _)| site.stolen.iter().any(|leaf| leaf.key == held.current_key));
                // A seed replayer can block the hub's re-issue; that residual is documented.
                if !site.stranded && !site.replayed && thief_holds && !self.frozen(name) {
                    self.fail(&format!("{name} was displaced without a freeze: {reason:?}"));
                }
            },
            Err(other) => self.fail(&format!("renewal failed: {other}")),
        }
    }

    /// A thief copies the real holder's current leaf and key.
    fn steal(&mut self, name: &'static str) {
        let now = self.now();
        if let Some(leaf) = self.sites[name].leaf.clone().filter(|leaf| now < leaf.not_after) {
            let site = self.site(name);
            site.stolen.push(leaf);
            site.compromised = true;
        }
    }

    /// A thief renews with one of its leaves.
    fn thief_renew(&mut self, name: &'static str) {
        let now = self.now();
        let usable: Vec<Leaf> = self.sites[name]
            .stolen
            .iter()
            .filter(|leaf| now < leaf.not_after && leaf.ca == self.ca)
            .cloned()
            .collect();
        let Some(leaf) = usable
            .get(usize::try_from(self.next(u64::try_from(usable.len().max(1)).unwrap_or(1))).unwrap_or(0))
            .cloned()
        else {
            return;
        };
        let requested = self.fresh_key();
        let renewal = Renewal {
            site_name: name.to_owned(),
            presented_key: leaf.key.clone(),
            requested_key: requested.clone(),
            presented_not_before: leaf.not_before,
        };
        let frozen_before = self.frozen(name);
        let issued = requested.clone();
        let renewed = self.store.renew_and_issue(&renewal, || Ok(cert(&issued)));
        // A leaf from well before a recovery must neither renew nor freeze the recovered record.
        let predates = self.sites[name]
            .recovered_at
            .is_some_and(|recovered| leaf.not_before.saturating_add(RECORD_SKEW).saturating_add(BACKDATE) < recovered);
        if predates && !self.sites[name].stranded {
            if renewed.is_ok() {
                self.fail(&format!("a leaf from before {name}'s recovery renewed"));
            }
            if !frozen_before && self.frozen(name) {
                self.fail(&format!("a leaf from before {name}'s recovery froze it again"));
            }
        }
        if renewed.is_ok() {
            let leaf = self.leaf(requested);
            self.site(name).stolen.push(leaf);
        }
    }

    /// After a recovery, the thief's leaves count only if they are not from well before it.
    fn recovered(&mut self, name: &'static str, at: OffsetDateTime) {
        let now = self.now();
        let site = self.site(name);
        site.recovered_at = Some(at);
        site.stranded = false;
        site.compromised = site.stolen.iter().any(|leaf| {
            now < leaf.not_after && leaf.not_before.saturating_add(RECORD_SKEW).saturating_add(BACKDATE) >= at
        });
    }

    /// The documented spoke recovery: a grid-admin deletes the enrollment, the site re-enrolls.
    fn recover_spoke(&mut self) {
        match self.store.delete_enrollment(SPOKE) {
            Ok(()) | Err(StoreError::NotFound) => {},
            Err(other) => self.fail(&format!("delete failed: {other}")),
        }
        self.enroll(SPOKE);
        let now = self.now();
        self.recovered(SPOKE, now);
        let key = self.sites[SPOKE].leaf.as_ref().map(|leaf| leaf.key.clone());
        match self.record(SPOKE) {
            Some((held, _)) if !held.frozen && Some(&held.current_key) == key.as_ref() => {},
            _ => self.fail("a spoke recovery did not end unfrozen with its fresh key"),
        }
    }

    /// The documented hub recovery: delete its identity Secret, helm upgrade re-issues it.
    fn recover_hub(&mut self) {
        self.reissue_hub();
    }

    /// A helm upgrade with the seed Secret gone: bootstrap re-signs the kept identity.
    fn reseed_kept(&mut self) {
        let Some(leaf) = self.sites[HUB].leaf.clone() else {
            return;
        };
        self.configmap = None;
        let seed = SeedRecord {
            site_name: HUB.to_owned(),
            key_sha256: leaf.key,
            generation: self.next_generation(),
            issued_at: leaf.not_before,
        };
        self.configmap = Some(seed.clone());
        self.signed.push(seed);
    }

    /// Someone with write on the seed Secret puts back a seed bootstrap signed earlier.
    fn replay_seed(&mut self) {
        if self.signed.is_empty() {
            return;
        }
        let pick = usize::try_from(self.next(u64::try_from(self.signed.len()).unwrap_or(1))).unwrap_or(0);
        self.configmap = self.signed.get(pick).cloned();
        self.site(HUB).replayed = true;
    }

    /// The enrollment database goes back to an earlier snapshot.
    fn restore(&mut self) {
        let Some(snapshot) = self.snapshot.clone() else {
            return;
        };
        self.store.restore(snapshot);
        for site in self.sites.values_mut() {
            site.stranded = true;
            site.generation = None;
            site.awaiting = None;
        }
    }

    /// The CA key Secret is deleted.
    fn lose_ca_key(&mut self) {
        self.ca_key = None;
    }

    /// The key Secret is restored from the wrong backup: another CA's key.
    fn restore_wrong_ca_key(&mut self) {
        self.ca_key = Some(self.ca.wrapping_add(1000));
    }

    /// What bootstrap reads: the key's CA and the distributed CA.
    fn ca_inputs(&self) -> (Option<String>, Vec<crate::CaCopy>) {
        let copies = self
            .bundle
            .map(|ca| crate::CaCopy::Holds(std::collections::BTreeSet::from([ca.to_string()])))
            .into_iter()
            .collect();
        (self.ca_key.map(|ca| ca.to_string()), copies)
    }

    /// An install or upgrade runs bootstrap, which decides the CA.
    fn sync(&mut self) {
        let (key, distributed) = self.ca_inputs();
        let before = self.ca;
        match crate::ca_action(key.as_deref(), false, &distributed) {
            crate::CaAction::Mint => {
                self.ca = self.ca.wrapping_add(1);
                self.bundle = Some(self.ca);
                self.ca_key = Some(self.ca);
            },
            // Load signs with whatever CA the key Secret holds and rewrites the bundle.
            crate::CaAction::Load => {
                self.ca = self.ca_key.unwrap_or(self.ca);
                self.bundle = Some(self.ca);
            },
            crate::CaAction::Refuse(_) => {},
        }
        if before != self.ca {
            self.fail("the grid CA changed without ca.forceRegenerate");
        }
    }

    /// A grid-admin starts a new grid CA on purpose: every site re-enrolls.
    fn force_regenerate(&mut self) {
        let (key, distributed) = self.ca_inputs();
        if crate::ca_action(key.as_deref(), true, &distributed) != crate::CaAction::Mint {
            self.fail("forceRegenerate did not mint");
        }
        self.ca = self.ca.wrapping_add(1);
        self.bundle = Some(self.ca);
        self.ca_key = Some(self.ca);
        for site in self.sites.values_mut() {
            site.stranded = true;
        }
        // Seeds the old CA signed no longer verify, and bootstrap re-issues the hub.
        self.configmap = None;
        self.signed.clear();
        self.reissue_hub();
    }

    /// Time passes. The service reapplies the mounted seed every minute.
    fn tick(&mut self, by: Duration) {
        self.store.advance(by);
        self.apply_seed();
    }

    fn step(&mut self) {
        let site = if self.next(2) == 0 { SPOKE } else { HUB };
        let event = self.next(18);
        self.trace.push(format!("{event:>2} {site}"));
        match event {
            0 => self.tick(Duration::minutes(20)),
            1 => self.tick(Duration::days(1)),
            2 => self.tick(Duration::days(7)),
            3 => self.real_renew(site, false),
            4 => self.real_renew(site, true),
            5 => self.steal(site),
            6 => self.thief_renew(site),
            7 => self.recover_spoke(),
            8 => self.recover_hub(),
            9 => self.reseed_kept(),
            10 => self.apply_seed(),
            11 => self.replay_seed(),
            12 => self.snapshot = Some(self.store.snapshot()),
            13 => self.restore(),
            14 => self.lose_ca_key(),
            17 => self.restore_wrong_ca_key(),
            15 => self.sync(),
            _ => self.force_regenerate(),
        }
        for (name, site) in &self.sites {
            // A re-issue clears a freeze once the service applies its seed.
            if !site.compromised && !site.stranded && !site.replayed && site.awaiting.is_none() && self.frozen(name) {
                self.fail(&format!("a healthy {name} froze"));
            }
        }
    }
}

/// A stand-in for the signed leaf, certifying `key`.
fn cert(key: &str) -> Issued {
    Issued {
        certificate: String::new(),
        spiffe_id: String::new(),
        public_key_sha256: key.to_owned(),
    }
}

#[test]
fn lifecycle_invariants_hold_over_random_walks() {
    for walk in 0..WALKS {
        let mut world = World::new(walk);
        for _ in 0..STEPS {
            world.step();
        }
    }
}

/// Bootstrap re-signs the kept hub key while the hub renews away from it, and the
/// service applies that seed afterwards: the hub must keep renewing.
#[test]
fn a_re_seed_racing_a_hub_renewal_does_not_freeze_the_hub() {
    let mut world = World::new(1);
    world.store.advance(Duration::days(21));
    world.reseed_kept();
    world.real_renew(HUB, false);
    world.apply_seed();
    world.store.advance(Duration::days(21));
    world.apply_seed();
    world.real_renew(HUB, false);
    assert!(!world.frozen(HUB), "the hub froze");
    assert!(world.sites[HUB].pending.is_none(), "the hub's renewal was refused");
}
