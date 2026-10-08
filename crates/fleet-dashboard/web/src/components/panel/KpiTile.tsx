import type { SeriesPoint } from '../../api/types'
import type { SparklineField } from '../../lib/sparkline'
import Sparkline from './Sparkline'

export interface KpiTileProps {
  label: string
  value: string
  /** Second line: "warn at 90%", "p50 812 ms", "64 GPUs". */
  hint: string
  /** Value has crossed its warning threshold; the tile turns amber. */
  warn: boolean
  /** Detail series for the sparkline; null when the metric has no series field. */
  points: SeriesPoint[] | null
  field: SparklineField
}

// Panel is 340 px: 12 px padding each side, two tiles with an 8 px gap, 8 px tile padding.
export const KPI_SPARK_WIDTH = 138
export const KPI_SPARK_HEIGHT = 28
const SPARK_COLOR = '#2fc4d1'
const SPARK_COLOR_WARN = '#e0a21c'

export default function KpiTile({ label, value, hint, warn, points, field }: KpiTileProps) {
  return (
    <div
      role="group"
      aria-label={label}
      data-warn={warn || undefined}
      className="rounded border border-line bg-surface-2 p-2 data-warn:border-degraded/60"
    >
      <div className="text-xs text-ink-2">{label}</div>
      <div className={`text-xl font-semibold tabular-nums ${warn ? 'text-degraded' : 'text-ink'}`}>{value}</div>
      <div className={`text-xs ${warn ? 'text-degraded' : 'text-ink-2'}`}>{hint}</div>
      {points ? (
        <div className="mt-1">
          <Sparkline points={points} field={field} color={warn ? SPARK_COLOR_WARN : SPARK_COLOR} width={KPI_SPARK_WIDTH} height={KPI_SPARK_HEIGHT} />
        </div>
      ) : null}
    </div>
  )
}
