import type { FleetSummary, SeriesResponse, Site } from '../api/types'
import {
  fleetP50,
  fleetQueueDepth,
  formatPctDelta,
  formatPointsDelta,
  placedCounts,
  seriesDelta,
  tenantLeader,
  topQueue,
  totalRunning,
} from '../lib/fleetStats'
import { formatCompact, formatMs, formatPct } from '../lib/format'
import HealthShape from './HealthShape'
import SummaryTile from './SummaryTile'

export interface SummaryTilesProps {
  summary: FleetSummary | null
  sites: Site[]
  /** The 1h fleet series; deltas compare its first and last points. */
  hourly: SeriesResponse | null
  /** Skeleton tiles while the first snapshot loads. */
  loading?: boolean
}

export const TILE_LABELS = ['GPUs', 'Utilization', 'Tokens/s', 'Requests/s', 'Queue depth', 'Models', 'Tenants', 'Sites'] as const

function SiteCounts({ summary }: { summary: FleetSummary }) {
  return (
    <span className="flex items-center gap-2">
      <span className="flex items-center gap-1 text-healthy">
        <HealthShape health="green" size={8} label="" />
        {summary.sitesGreen}
      </span>
      <span className="flex items-center gap-1 text-degraded">
        <HealthShape health="yellow" size={8} label="" />
        {summary.sitesYellow}
      </span>
      <span className="flex items-center gap-1 text-down">
        <HealthShape health="red" size={8} label="" />
        {summary.sitesRed}
      </span>
    </span>
  )
}

/** Eight fleet tiles in a 4 by 2 grid (spec section 3.5). */
export default function SummaryTiles({ summary, sites, hourly, loading = false }: SummaryTilesProps) {
  if (loading) {
    return (
      <div role="list" aria-label="Loading fleet summary" aria-busy="true" className="grid grid-cols-4 grid-rows-2 gap-2">
        {TILE_LABELS.map((label) => (
          <div key={label} role="listitem" className="rounded-md border border-line bg-surface-2 px-3 py-2">
            <div className="h-3 w-1/2 animate-pulse rounded bg-line" />
            <div className="mt-2 h-6 w-2/3 animate-pulse rounded bg-line" />
          </div>
        ))}
      </div>
    )
  }
  const util = hourly ? seriesDelta(hourly.points, 'gpuUtil') : null
  const tokens = hourly ? seriesDelta(hourly.points, 'tokensPerSec') : null
  const p50 = fleetP50(sites)
  const queueTop = topQueue(sites)
  const leader = tenantLeader(sites)
  const counts = placedCounts(sites)
  return (
    <div className="grid grid-cols-4 grid-rows-2 gap-2">
      <SummaryTile label="GPUs" value={formatCompact(summary?.gpuTotal ?? null)} sub={summary ? <SiteCounts summary={summary} /> : undefined} />
      <SummaryTile
        label="Utilization"
        value={formatPct(summary?.gpuUtilPct ?? null)}
        delta={util?.delta === null || util === null ? undefined : formatPointsDelta(util.delta)}
        sub={util?.delta === null || util === null ? 'GPU-weighted mean' : 'vs 1h ago'}
      />
      <SummaryTile
        label="Tokens/s"
        value={formatCompact(summary?.tokensPerSec ?? null)}
        delta={tokens?.pct === null || tokens === null ? undefined : formatPctDelta(tokens.pct)}
        sub={tokens?.pct === null || tokens === null ? undefined : 'vs 1h ago'}
      />
      <SummaryTile label="Requests/s" value={formatCompact(summary?.rps ?? null)} sub={p50 === null ? undefined : `fleet p50 ${formatMs(p50)}`} />
      <SummaryTile
        label="Queue depth"
        value={formatCompact(fleetQueueDepth(sites))}
        sub={queueTop ? `top ${queueTop.displayName || queueTop.name} · ${formatCompact(queueTop.queueDepth)}` : undefined}
      />
      <SummaryTile label="Models" value={formatCompact(summary?.activeModels ?? null)} sub={summary ? `${formatCompact(totalRunning(sites))} running` : undefined} />
      <SummaryTile label="Tenants" value={formatCompact(summary?.activeTenants ?? null)} sub={leader ? `leader ${leader.name}` : undefined} />
      <SummaryTile label="Sites" value={summary ? String(sites.length) : '--'} sub={summary ? `${counts.placed} placed · ${counts.unplaced} unplaced` : undefined} />
    </div>
  )
}
