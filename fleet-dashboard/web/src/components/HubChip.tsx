import type { Hub } from '../api/types'
import { hexagonPoints } from '../lib/glyph'
import { HUB_COLOR } from '../lib/health'

export interface HubChipProps {
  hub: Hub | null
  version: string
}

/** Hexagon plus hub name and region; the build version lives in the tooltip. */
export default function HubChip({ hub, version }: HubChipProps) {
  const text = hub ? `${hub.name}${hub.region ? ` · ${hub.region}` : ''}` : 'hub not configured'
  const title = [hub ? `hub ${hub.name}` : 'hub not configured', version ? `version ${version}` : '']
    .filter(Boolean)
    .join(' · ')
  return (
    <span title={title} className="inline-flex h-7 items-center gap-1.5 rounded-full border border-line bg-surface px-2.5 text-xs text-ink-2">
      <svg width={12} height={12} viewBox="0 0 12 12" aria-hidden="true">
        <polygon points={hexagonPoints(6, 6, 5.5)} fill={HUB_COLOR} fillOpacity={0.35} stroke={HUB_COLOR} strokeWidth={1.5} />
      </svg>
      {hub ? (
        <>
          <span className="font-medium text-ink">{hub.name}</span>
          {hub.region ? <span>{hub.region}</span> : null}
        </>
      ) : (
        <span>{text}</span>
      )}
    </span>
  )
}
