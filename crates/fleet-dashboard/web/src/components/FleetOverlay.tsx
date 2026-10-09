export type FleetOverlayKind = 'loading' | 'waiting' | 'error' | 'empty'

export interface FleetOverlayProps {
  kind: FleetOverlayKind
  /** Plain-words failure message for kind "error". */
  message?: string
  onRetry?: () => void
}

/** The one-line register command from the README, shown when the fleet is empty. */
export const REGISTER_COMMAND =
  'hack/register-site.sh <site-name> --spoke-kubeconfig <spoke.kubeconfig> --hub-kubeconfig <hub.kubeconfig> --region <region> --display-name <name> --dc <dc>'

function Spinner() {
  return (
    <span
      aria-hidden="true"
      className="inline-block h-5 w-5 animate-spin rounded-full border-2 border-line border-t-accent motion-reduce:animate-none"
    />
  )
}

/** Covers the map area while there is nothing to show: first load, the collector's first poll, a failure, or an empty fleet. */
export default function FleetOverlay({ kind, message, onRetry }: FleetOverlayProps) {
  return (
    <div role="status" className="absolute inset-0 z-[1001] flex flex-col items-center justify-center gap-3 bg-bg/80 px-6 text-center text-sm text-ink-2">
      {kind === 'loading' ? (
        <>
          <Spinner />
          <span>Connecting to hub</span>
        </>
      ) : null}
      {kind === 'waiting' ? (
        <>
          <Spinner />
          <span>Waiting for the first poll</span>
        </>
      ) : null}
      {kind === 'error' ? (
        <>
          <span className="text-ink">Fleet data unavailable</span>
          {message ? <span>{message}</span> : null}
          <button
            type="button"
            onClick={onRetry}
            className="h-8 rounded border border-line bg-surface px-3 text-xs text-ink hover:bg-surface-2"
          >
            Retry
          </button>
        </>
      ) : null}
      {kind === 'empty' ? (
        <>
          <span className="text-ink">No sites registered yet</span>
          <span>Register the first site from a workstation with access to the hub and the spoke:</span>
          <code className="max-w-[640px] rounded border border-line bg-surface px-3 py-2 text-left font-mono text-xs text-ink">{REGISTER_COMMAND}</code>
        </>
      ) : null}
    </div>
  )
}
