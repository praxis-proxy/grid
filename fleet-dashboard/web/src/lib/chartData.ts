import type { SeriesPoint } from '../api/types'

export interface ChartRow {
  t: number
  gpuUtil: number | null
  tokensPerSec: number | null
  queueDepth: number | null
}

/** Recharts needs numeric x values; nulls stay null so the lines show gaps. */
export function seriesToChartData(points: readonly SeriesPoint[]): ChartRow[] {
  const rows: ChartRow[] = []
  for (const p of points) {
    const t = Date.parse(p.t)
    if (Number.isNaN(t)) continue
    rows.push({ t, gpuUtil: p.gpuUtil, tokensPerSec: p.tokensPerSec, queueDepth: p.queueDepth })
  }
  return rows
}
