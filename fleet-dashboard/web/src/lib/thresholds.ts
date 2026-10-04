import type { SeriesPoint } from '../api/types'
import type { SparklineField } from './sparkline'

/** True once a value has reached the warning threshold (the collector uses >=). Unknown values never warn. */
export function crossesThreshold(value: number | null | undefined, warnAt: number): boolean {
  return value !== null && value !== undefined && Number.isFinite(value) && value >= warnAt
}

export interface MinMax {
  min: number
  max: number
}

/** Smallest and largest non-null values of a series field; null when the series has none. */
export function seriesMinMax(points: readonly SeriesPoint[], field: SparklineField): MinMax | null {
  let min = Number.POSITIVE_INFINITY
  let max = Number.NEGATIVE_INFINITY
  for (const point of points) {
    const v = point[field]
    if (v === null || !Number.isFinite(v)) continue
    min = Math.min(min, v)
    max = Math.max(max, v)
  }
  return min === Number.POSITIVE_INFINITY ? null : { min, max }
}
