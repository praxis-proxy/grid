import type { ChartRow } from './chartData'
import { formatCompact } from './format'
import type { SparklineField } from './sparkline'

export const CHART_SERIES: ReadonlyArray<{ field: SparklineField; name: string }> = [
  { field: 'gpuUtil', name: 'GPU utilization' },
  { field: 'tokensPerSec', name: 'Tokens/s' },
  { field: 'queueDepth', name: 'Queue depth' },
]

/** "71 %", "3.1k tok/s", "42 queued"; "--" when unknown. */
export function formatChartValue(field: SparklineField, value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--'
  switch (field) {
    case 'gpuUtil':
      return `${Math.round(value)} %`
    case 'tokensPerSec':
      return `${formatCompact(value)} tok/s`
    case 'queueDepth':
      return `${formatCompact(value)} queued`
  }
}

export function isSparklineField(key: unknown): key is SparklineField {
  return key === 'gpuUtil' || key === 'tokensPerSec' || key === 'queueDepth'
}

/** Start, middle and end of the rows' time span, as epoch ms; empty for no rows. */
export function timeTicks(rows: readonly ChartRow[]): number[] {
  if (rows.length === 0) return []
  const start = rows[0].t
  const end = rows[rows.length - 1].t
  if (start === end) return [start]
  return [start, Math.round((start + end) / 2), end]
}

export function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}
