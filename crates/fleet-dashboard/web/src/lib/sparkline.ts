import { formatCompact, formatPct } from './format'

/** The three per-site series fields the API returns. */
export type SparklineField = 'gpuUtil' | 'queueDepth' | 'tokensPerSec'

export function formatSparkValue(field: SparklineField, value: number | null): string {
  return field === 'gpuUtil' ? formatPct(value) : formatCompact(value)
}
