import { useAgeTicker } from '../hooks/useAgeTicker'
import { formatAge } from '../lib/format'

export interface StaleBannerProps {
  lastUpdate: number | null
}

/**
 * Across the top of the map while no snapshot has arrived for three poll
 * intervals. The ticking age lives in a sibling span outside the alert
 * region, not inside it — otherwise a screen reader would re-announce the
 * whole banner every second as the age counts up.
 */
export default function StaleBanner({ lastUpdate }: StaleBannerProps) {
  const age = useAgeTicker(lastUpdate)
  return (
    <div className="flex h-8 items-center justify-center border-b border-down/50 bg-down/15 text-xs text-down">
      <span role="alert">Data is stale</span>
      {age !== null ? <span>{`, last update ${formatAge(age)}`}</span> : null}
    </div>
  )
}
