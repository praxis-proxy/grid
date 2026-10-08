import type { FleetSummary, Health } from '../api/types'
import { HEALTH_ORDER, healthWord } from '../lib/health'
import HealthShape from './HealthShape'

export interface StatusPillProps {
  summary: FleetSummary | null
  /** Health the roster is currently filtered to; null shows every site. */
  filter: Health | null
  onFilterChange: (health: Health | null) => void
}

const COUNT_KEY: Record<Health, keyof FleetSummary> = { green: 'sitesGreen', yellow: 'sitesYellow', red: 'sitesRed' }
const TONE: Record<Health, string> = { green: 'text-healthy', yellow: 'text-degraded', red: 'text-down' }
const DISPLAY: Health[] = [...HEALTH_ORDER].reverse()

/** Healthy / degraded / down counts. Each count filters the roster; pressing it again clears the filter. */
export default function StatusPill({ summary, filter, onFilterChange }: StatusPillProps) {
  return (
    <div role="group" aria-label="Fleet status" className="inline-flex h-7 items-center overflow-hidden rounded-full border border-line bg-surface text-xs">
      {DISPLAY.map((health) => {
        const count = summary ? summary[COUNT_KEY[health]] : null
        const pressed = filter === health
        return (
          <button
            key={health}
            type="button"
            aria-pressed={pressed}
            aria-label={`${count === null ? '--' : count} ${healthWord(health)}`}
            title={pressed ? 'Show all sites' : `Show only ${healthWord(health)} sites`}
            onClick={() => onFilterChange(pressed ? null : health)}
            className={`flex h-full items-center gap-1.5 px-2.5 hover:bg-surface-2 aria-pressed:bg-surface-2 ${TONE[health]}`}
          >
            <HealthShape health={health} size={9} label="" />
            <span className="font-semibold tabular-nums">{count === null ? '--' : count}</span>
            <span className="text-ink-2">{healthWord(health)}</span>
          </button>
        )
      })}
    </div>
  )
}
