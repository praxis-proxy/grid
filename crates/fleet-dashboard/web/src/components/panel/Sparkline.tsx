import { useMemo } from 'react'
import { Line, LineChart, YAxis } from 'recharts'
import type { SeriesPoint } from '../../api/types'
import { formatSparkValue, type SparklineField } from '../../lib/sparkline'

export interface SparklineProps {
  points: SeriesPoint[]
  field: SparklineField
  color: string
  width: number
  height: number
  /** When set, a header row shows the label and the latest value. */
  label?: string
}

// Module-level so identity is stable across renders; recharts treats a new
// object identity on these props as a reason to re-layout.
const CHART_MARGIN = { top: 2, right: 0, bottom: 0, left: 0 }
const PCT_DOMAIN: [number, number] = [0, 100]
const AUTO_DOMAIN: ['auto', 'auto'] = ['auto', 'auto']

export default function Sparkline({ points, field, color, width, height, label }: SparklineProps) {
  const data = useMemo(() => points.map((p) => ({ t: p.t, v: p[field] })), [points, field])
  const last = data.at(-1)?.v ?? null
  return (
    <div>
      {label ? (
        <div className="flex justify-between text-xs text-ink-2">
          <span>{label}</span>
          <span className="tabular-nums text-ink">{formatSparkValue(field, last)}</span>
        </div>
      ) : null}
      <LineChart width={width} height={height} data={data} margin={CHART_MARGIN}>
        <YAxis hide domain={field === 'gpuUtil' ? PCT_DOMAIN : AUTO_DOMAIN} />
        <Line type="monotone" dataKey="v" stroke={color} strokeWidth={1.5} dot={false} isAnimationActive={false} connectNulls={false} />
      </LineChart>
    </div>
  )
}
