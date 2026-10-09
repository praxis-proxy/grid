import type { SeriesPoint, Site } from '../api/types'
import type { SparklineField } from './sparkline'

export interface SeriesDelta {
  first: number | null
  last: number | null
  /** last - first; null unless both ends are known. */
  delta: number | null
  /** Percent change relative to first; null when first is 0 or unknown. */
  pct: number | null
}

/** Change across a series: first non-null point versus last non-null point. */
export function seriesDelta(points: readonly SeriesPoint[], field: SparklineField): SeriesDelta {
  const known = points.map((p) => p[field]).filter((v): v is number => v !== null && Number.isFinite(v))
  if (known.length < 2) return { first: known[0] ?? null, last: known[0] ?? null, delta: null, pct: null }
  const first = known[0]
  const last = known[known.length - 1]
  const delta = last - first
  return { first, last, delta, pct: first === 0 ? null : (100 * delta) / first }
}

/** "+3 pts", "-12 pts", "0 pts"; "--" when unknown. */
export function formatPointsDelta(delta: number | null): string {
  if (delta === null || !Number.isFinite(delta)) return '--'
  const n = Math.round(delta)
  return `${n > 0 ? '+' : ''}${n} pts`
}

/** "+12%", "-3%", "0%"; "--" when unknown. */
export function formatPctDelta(pct: number | null): string {
  if (pct === null || !Number.isFinite(pct)) return '--'
  const n = Math.round(pct)
  return `${n > 0 ? '+' : ''}${n}%`
}

/** GPU-weighted mean of site p50 latencies; sites without a p50 are skipped. */
export function fleetP50(sites: readonly Site[]): number | null {
  let weighted = 0
  let weight = 0
  for (const site of sites) {
    if (site.p50LatencyMs === null || site.gpus.total <= 0) continue
    weighted += site.p50LatencyMs * site.gpus.total
    weight += site.gpus.total
  }
  return weight === 0 ? null : weighted / weight
}

/** Sum of known site queue depths. */
export function fleetQueueDepth(sites: readonly Site[]): number | null {
  let sum = 0
  let known = false
  for (const site of sites) {
    if (site.queueDepth === null) continue
    sum += site.queueDepth
    known = true
  }
  return known ? sum : null
}

/** The site contributing the most queue depth; null when no site reports one. */
export function topQueue(sites: readonly Site[]): Site | null {
  let top: Site | null = null
  for (const site of sites) {
    if (site.queueDepth === null) continue
    if (top === null || (top.queueDepth ?? 0) < site.queueDepth) top = site
  }
  return top
}

export function totalRunning(sites: readonly Site[]): number {
  return sites.reduce((sum, site) => sum + site.models.reduce((s, m) => s + m.running, 0), 0)
}

export interface TenantLeader {
  name: string
  /** GPUs attributable to the tenant: sum over sites of share * site GPUs. */
  gpus: number
}

/** The tenant holding the largest GPU-weighted share across the fleet. */
export function tenantLeader(sites: readonly Site[]): TenantLeader | null {
  const totals = new Map<string, number>()
  for (const site of sites) {
    for (const tenant of site.tenants) {
      totals.set(tenant.name, (totals.get(tenant.name) ?? 0) + (tenant.sharePct / 100) * site.gpus.total)
    }
  }
  let leader: TenantLeader | null = null
  for (const [name, gpus] of totals) {
    if (leader === null || gpus > leader.gpus) leader = { name, gpus }
  }
  return leader
}

export function placedCounts(sites: readonly Site[]): { placed: number; unplaced: number } {
  const placed = sites.filter((site) => site.placed).length
  return { placed, unplaced: sites.length - placed }
}
