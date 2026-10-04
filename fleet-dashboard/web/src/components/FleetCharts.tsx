import { memo, useMemo } from 'react'
import {
  Area,
  AreaChart,
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { SeriesRange, SeriesResponse } from '../api/types'
import { seriesToChartData } from '../lib/chartData'
import { CHART_SERIES, formatChartValue, formatTime, isSparklineField, timeTicks } from '../lib/chartFormat'
import { formatCompact } from '../lib/format'
import Segmented from './Segmented'

export interface FleetChartsProps {
  series: SeriesResponse | null
  range: SeriesRange
  error: string | null
  onRangeChange: (range: SeriesRange) => void
  /** Drawn as a dashed amber line on the utilization chart. */
  gpuUtilWarn: number
}

const RANGES: ReadonlyArray<{ value: SeriesRange; label: string }> = [
  { value: '1h', label: '1h' },
  { value: '6h', label: '6h' },
  { value: '24h', label: '24h' },
]

const AXIS_COLOR = '#8a99a6'
const GRID_COLOR = '#243039'
const COLOR_GPU = '#2fc4d1'
const COLOR_TOKENS = '#e0a21c'
const COLOR_QUEUE = '#5b6b7a'
const COLOR_WARN = '#e0a21c'
const SYNC_ID = 'fleet-over-time'

// Module-level so these are stable across renders; recharts treats a new
// object identity on these props as a reason to re-layout.
const CHART_MARGIN = { top: 4, right: 4, bottom: 0, left: 0 }
const AXIS_TICK_STYLE = { fontSize: 11, fill: AXIS_COLOR }
const LEFT_AXIS_WIDTH = 40
const RIGHT_AXIS_WIDTH = 44
const PCT_DOMAIN: [number, number] = [0, 100]
const TIME_DOMAIN: ['dataMin', 'dataMax'] = ['dataMin', 'dataMax']

/** The subset of recharts' tooltip content props this renderer reads; recharts' own generics widen to it. */
interface ChartTooltipProps {
  active?: boolean
  label?: unknown
  payload?: ReadonlyArray<{ dataKey?: unknown; value?: unknown; color?: string }>
}

function ChartTooltip({ active, payload, label }: ChartTooltipProps) {
  if (!active || !payload || payload.length === 0) return null
  return (
    <div className="rounded border border-line bg-surface px-2 py-1 text-xs text-ink">
      <div className="text-ink-2">{typeof label === 'number' ? formatTime(label) : ''}</div>
      {payload.map((entry) => {
        const key = entry.dataKey
        if (!isSparklineField(key)) return null
        const value = typeof entry.value === 'number' ? entry.value : null
        return (
          <div key={key} className="flex justify-between gap-3">
            <span style={{ color: entry.color }}>{CHART_SERIES.find((s) => s.field === key)?.name}</span>
            <span className="tabular-nums">{formatChartValue(key, value)}</span>
          </div>
        )
      })}
    </div>
  )
}

// Module scope so these props keep a stable identity across renders instead
// of forcing recharts to see "new" props every time -- otherwise wrapping
// the component in memo below would not stop it from re-laying out anyway.
function renderTooltip(props: ChartTooltipProps) {
  return <ChartTooltip {...props} />
}
function pctTick(v: number): string {
  return `${v}%`
}
function compactTick(v: number): string {
  return formatCompact(v)
}

/** Two stacked small multiples on a shared time axis: GPU utilization; tokens/s with queue depth. */
function FleetCharts({ series, range, error, onRangeChange, gpuUtilWarn }: FleetChartsProps) {
  const data = useMemo(() => (series ? seriesToChartData(series.points) : []), [series])
  const ticks = useMemo(() => timeTicks(data), [data])
  return (
    <section aria-label="Fleet over time" className="flex min-h-0 flex-col">
      <div className="flex items-center justify-between pb-1">
        <span className="text-xs font-medium text-ink">{series ? `Fleet over time · step ${series.step} s` : 'Fleet over time'}</span>
        <Segmented label="Time range" options={RANGES} value={range} onChange={onRangeChange} size="md" />
      </div>
      {data.length === 0 ? (
        <p className="flex flex-1 items-center justify-center text-xs text-ink-2">{error ? `Series unavailable: ${error}` : 'No series data yet'}</p>
      ) : (
        <div className="flex min-h-0 flex-1 flex-col">
          <div className="min-h-0 flex-1" aria-label="GPU utilization %" role="img">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={data} margin={CHART_MARGIN} syncId={SYNC_ID}>
                <CartesianGrid stroke={GRID_COLOR} vertical={false} />
                <XAxis dataKey="t" type="number" domain={TIME_DOMAIN} ticks={ticks} hide />
                <YAxis yAxisId="pct" domain={PCT_DOMAIN} width={LEFT_AXIS_WIDTH} tick={AXIS_TICK_STYLE} tickLine={false} axisLine={false} tickFormatter={pctTick} />
                <YAxis yAxisId="spacer" orientation="right" width={RIGHT_AXIS_WIDTH} tick={false} tickLine={false} axisLine={false} />
                <Tooltip content={renderTooltip} cursor={{ stroke: GRID_COLOR }} />
                <ReferenceLine yAxisId="pct" y={gpuUtilWarn} stroke={COLOR_WARN} strokeDasharray="4 4" label={{ value: `warn ${gpuUtilWarn}%`, position: 'insideTopRight', fill: COLOR_WARN, fontSize: 11 }} />
                <Area yAxisId="pct" type="monotone" dataKey="gpuUtil" name="GPU utilization" stroke={COLOR_GPU} fill={COLOR_GPU} fillOpacity={0.18} strokeWidth={1.5} dot={false} isAnimationActive={false} connectNulls={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <div className="min-h-0 flex-1" aria-label="Tokens / s and queue depth" role="img">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={data} margin={CHART_MARGIN} syncId={SYNC_ID}>
                <CartesianGrid stroke={GRID_COLOR} vertical={false} />
                <XAxis dataKey="t" type="number" domain={TIME_DOMAIN} ticks={ticks} tickFormatter={formatTime} tick={AXIS_TICK_STYLE} tickLine={false} axisLine={false} height={18} />
                <YAxis yAxisId="tokens" width={LEFT_AXIS_WIDTH} tick={AXIS_TICK_STYLE} tickLine={false} axisLine={false} tickFormatter={compactTick} />
                <YAxis yAxisId="queue" orientation="right" width={RIGHT_AXIS_WIDTH} tick={AXIS_TICK_STYLE} tickLine={false} axisLine={false} tickFormatter={compactTick} />
                <Tooltip content={renderTooltip} cursor={{ stroke: GRID_COLOR }} />
                <Bar yAxisId="queue" dataKey="queueDepth" name="Queue depth" fill={COLOR_QUEUE} fillOpacity={0.7} isAnimationActive={false} />
                <Line yAxisId="tokens" type="monotone" dataKey="tokensPerSec" name="Tokens/s" stroke={COLOR_TOKENS} strokeWidth={1.5} dot={false} isAnimationActive={false} connectNulls={false} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </section>
  )
}

export default memo(FleetCharts)
