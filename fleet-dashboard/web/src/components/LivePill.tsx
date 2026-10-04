import { useAgeTicker } from '../hooks/useAgeTicker'
import { formatAge } from '../lib/format'
import type { StreamStatus } from '../lib/fleetState'

export interface LivePillProps {
  status: StreamStatus
  lastUpdate: number | null
}

const STYLES: Record<StreamStatus, { dot: string; text: string; label: string; agePrefix: string }> = {
  live: { dot: 'bg-healthy', text: 'text-healthy', label: 'LIVE', agePrefix: 'updated' },
  reconnecting: { dot: 'bg-degraded', text: 'text-degraded', label: 'RECONNECTING', agePrefix: '' },
  stale: { dot: 'bg-down', text: 'text-down', label: 'STALE', agePrefix: 'last update' },
}

/**
 * Only the state word lives in the live region: announcing "updated 6s ago"
 * every second would make a screen reader unusable.
 */
export default function LivePill({ status, lastUpdate }: LivePillProps) {
  const ageSeconds = useAgeTicker(lastUpdate)
  const style = STYLES[status]
  const showAge = style.agePrefix !== '' && ageSeconds !== null
  return (
    <span className={`inline-flex h-7 items-center gap-2 rounded-full border border-line bg-surface px-2.5 text-xs font-medium ${style.text}`}>
      <span className={`h-2 w-2 rounded-full ${style.dot}`} aria-hidden="true" />
      <span role="status" aria-live="polite">
        {style.label}
      </span>
      {showAge ? <span className="tabular-nums text-ink-2">{` · ${style.agePrefix} ${formatAge(ageSeconds)}`}</span> : null}
    </span>
  )
}
