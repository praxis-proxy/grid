import type { Site } from '../api/types'
import { useNow } from '../hooks/useAgeTicker'
import { attentionSites } from '../lib/attention'
import { ageSeconds, formatAge } from '../lib/format'
import HealthShape from './HealthShape'

export interface AttentionStripProps {
  sites: Site[]
  selected: string | null
  onSelect: (name: string) => void
}

/** One chip per unhealthy site, over the top of the map. Hidden when the fleet is all healthy. */
export default function AttentionStrip({ sites, selected, onSelect }: AttentionStripProps) {
  const needing = attentionSites(sites)
  const now = useNow(needing.some((site) => site.health === 'red'))
  if (needing.length === 0) return null
  return (
    <ul aria-label="Sites needing attention" className="flex flex-wrap gap-2">
      {needing.map((site) => {
        const age = site.health === 'red' ? ageSeconds(site.lastSeen, now) : null
        const tone = site.health === 'red' ? 'border-down/60 text-down' : 'border-degraded/60 text-degraded'
        return (
          <li key={site.name}>
            <button
              type="button"
              onClick={() => onSelect(site.name)}
              aria-pressed={site.name === selected}
              className={`flex h-7 max-w-[360px] items-center gap-2 rounded-full border bg-surface/95 px-2.5 text-xs hover:bg-surface-2 aria-pressed:bg-surface-2 ${tone}`}
            >
              <HealthShape health={site.health} size={9} />
              <span className="truncate font-medium text-ink">{site.displayName || site.name}</span>
              {site.reasons[0] ? <span className="truncate">{site.reasons[0]}</span> : null}
              {site.health === 'red' ? (
                <span className="shrink-0 text-ink-2">{age === null ? 'never seen' : formatAge(age)}</span>
              ) : null}
            </button>
          </li>
        )
      })}
    </ul>
  )
}
