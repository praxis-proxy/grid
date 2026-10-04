import { useMemo } from 'react'
import { Line, LineChart, XAxis, YAxis } from 'recharts'
import type { Series } from '../../api/types'
import { seriesToChartData } from '../../lib/chartData'
import { seriesMinMax } from '../../lib/thresholds'
import { formatSparkValue, type SparklineField } from '../../lib/sparkline'

export interface HistoryTabProps {
  series: Series | null
  loading: boolean
  error: string | null
}

// 340 px panel minus 12 px padding each side.
const WIDTH = 316
const HEIGHT = 56
const CHART_MARGIN = { top: 4, right: 4, bottom: 0, left: 4 }
const PCT_DOMAIN: [number, number] = [0, 100]
const AUTO_DOMAIN: ['auto', 'auto'] = ['auto', 'auto']
const AXIS_TICK_STYLE = { fontSize: 11, fill: '#8a99a6' }

// Matches FleetCharts' series colors: accent for GPU utilization, amber for
// tokens/s (also the warning-threshold color), ink-2 for queue depth.
const ROWS: ReadonlyArray<{ field: SparklineField; label: string; color: string }> = [
  { field: 'gpuUtil', label: 'GPU util', color: '#2fc4d1' },
  { field: 'queueDepth', label: 'Queue depth', color: '#8a99a6' },
  { field: 'tokensPerSec', label: 'Tokens/s', color: '#e0a21c' },
]

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

/** Three sparklines of the last 20 minutes on one time axis, with min and max per row. */
export default function HistoryTab({ series, loading, error }: HistoryTabProps) {
  const data = useMemo(() => (series ? seriesToChartData(series.points) : []), [series])
  if (loading) return <p className="text-xs text-ink-2">Loading 20 minute history</p>
  if (error) return <p className="text-xs text-down">{error}</p>
  if (!series || data.length === 0) return <p className="text-xs text-ink-2">No history yet</p>
  const start = data[0].t
  const end = data[data.length - 1].t
  const ticks = [start, Math.round((start + end) / 2), end]
  return (
    <div className="flex flex-col gap-2">
      <div className="text-xs text-ink-2">{`Last 20 minutes · step ${series.step} s`}</div>
      {ROWS.map((row, i) => {
        const extent = seriesMinMax(series.points, row.field)
        const last = i === ROWS.length - 1
        return (
          <div key={row.field}>
            <div className="flex justify-between text-xs text-ink-2">
              <span>{row.label}</span>
              <span className="tabular-nums">
                {extent ? `min ${formatSparkValue(row.field, extent.min)} · max ${formatSparkValue(row.field, extent.max)}` : 'no data'}
              </span>
            </div>
            <LineChart width={WIDTH} height={last ? HEIGHT + 18 : HEIGHT} data={data} margin={CHART_MARGIN}>
              <XAxis
                dataKey="t"
                type="number"
                domain={[start, end]}
                ticks={ticks}
                tickFormatter={formatTime}
                hide={!last}
                tick={AXIS_TICK_STYLE}
                tickLine={false}
                axisLine={false}
                height={18}
              />
              <YAxis hide domain={row.field === 'gpuUtil' ? PCT_DOMAIN : AUTO_DOMAIN} />
              <Line type="monotone" dataKey={row.field} stroke={row.color} strokeWidth={1.5} dot={false} isAnimationActive={false} connectNulls={false} />
            </LineChart>
          </div>
        )
      })}
    </div>
  )
}
