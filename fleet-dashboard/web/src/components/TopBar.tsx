import type { FleetSummary, Health, Hub } from '../api/types'
import type { StreamStatus } from '../lib/fleetState'
import HubChip from './HubChip'
import LivePill from './LivePill'
import StatusPill from './StatusPill'

export interface TopBarProps {
  hub: Hub | null
  version: string
  status: StreamStatus
  lastUpdate: number | null
  summary: FleetSummary | null
  healthFilter: Health | null
  onHealthFilterChange: (health: Health | null) => void
  /** Signed-in user from oauth-proxy; null hides the name and the sign-out link. */
  user: string | null
}

export const SIGN_OUT_URL = '/oauth/sign_out'

export default function TopBar({ hub, version, status, lastUpdate, summary, healthFilter, onHealthFilterChange, user }: TopBarProps) {
  return (
    <header className="flex h-12 items-center gap-3 border-b border-line bg-surface px-4 text-sm">
      <span className="font-semibold tracking-[0.18em] text-ink">AI GRID FLEET</span>
      <HubChip hub={hub} version={version} />
      <StatusPill summary={summary} filter={healthFilter} onFilterChange={onHealthFilterChange} />
      <div className="ml-auto flex items-center gap-3">
        <LivePill status={status} lastUpdate={lastUpdate} />
        {user ? (
          <span className="flex items-center gap-2 text-xs text-ink-2">
            <span className="text-ink">{user}</span>
            <a href={SIGN_OUT_URL} className="underline hover:text-ink">
              Sign out
            </a>
          </span>
        ) : null}
      </div>
    </header>
  )
}
