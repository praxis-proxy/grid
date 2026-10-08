import type { Site } from '../api/types'
import { ageSeconds, formatAge, formatPct } from '../lib/format'
import { rosterOptionId } from '../lib/roster'
import HealthShape from './HealthShape'

export interface RosterRowProps {
  site: Site
  selected: boolean
  /** Keyboard cursor (aria-activedescendant target); visually outlined. */
  active: boolean
  /** Epoch ms for the "last seen" age of down sites. */
  now: number
  onSelect: (name: string) => void
}

const BAR_TONE: Record<Site['health'], string> = { green: 'bg-accent', yellow: 'bg-degraded', red: 'bg-down' }
const TEXT_TONE: Record<Site['health'], string> = { green: 'text-ink-2', yellow: 'text-degraded', red: 'text-down' }

function statusLine(site: Site, now: number): string | null {
  if (site.health === 'red') {
    const age = ageSeconds(site.lastSeen, now)
    return `unreachable · ${age === null ? 'never seen' : `last seen ${formatAge(age)}`}`
  }
  if (site.health === 'yellow') return site.reasons[0] ?? 'degraded'
  return null
}

export default function RosterRow({ site, selected, active, now, onSelect }: RosterRowProps) {
  const util = site.gpus.utilPct
  const width = util === null ? 0 : Math.max(0, Math.min(100, util))
  const status = statusLine(site, now)
  return (
    <li
      id={rosterOptionId(site.name)}
      role="option"
      aria-selected={selected}
      data-active={active || undefined}
      onClick={() => onSelect(site.name)}
      className="cursor-pointer border-b border-line px-3 py-2 text-xs hover:bg-surface-2 aria-selected:bg-surface-2 data-active:outline data-active:outline-1 data-active:-outline-offset-1 data-active:outline-accent"
    >
      <div className="flex items-center gap-2">
        <HealthShape health={site.health} size={9} />
        <span className="min-w-0 flex-1 truncate text-sm font-medium text-ink">{site.displayName || site.name}</span>
        <span className="shrink-0 tabular-nums text-ink-2">{site.gpus.total} GPU</span>
      </div>
      <div className="mt-0.5 flex items-center gap-2 pl-[17px] text-ink-2">
        <span className="truncate">{site.region}</span>
        {!site.placed ? <span className="shrink-0 rounded border border-line px-1 text-ink-3">no position</span> : null}
      </div>
      <div className="mt-1 flex items-center gap-2 pl-[17px]">
        <span className="h-1 flex-1 overflow-hidden rounded bg-line" role="img" aria-label={`utilization ${formatPct(util)}`}>
          <span className={`block h-full ${BAR_TONE[site.health]}`} style={{ width: `${width}%` }} />
        </span>
        <span className="w-8 shrink-0 text-right tabular-nums text-ink-2">{formatPct(util)}</span>
      </div>
      {status ? <div className={`mt-0.5 truncate pl-[17px] ${TEXT_TONE[site.health]}`}>{status}</div> : null}
    </li>
  )
}
