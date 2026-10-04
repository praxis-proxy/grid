import type { FleetSummary, SeriesRange, SeriesResponse, Site } from '../api/types'
import FleetCharts from './FleetCharts'
import SummaryTiles from './SummaryTiles'

export interface SummaryStripProps {
  summary: FleetSummary | null
  sites: Site[]
  /** The 1h series for tile deltas, independent of the chart's range. */
  hourly: SeriesResponse | null
  series: SeriesResponse | null
  seriesError: string | null
  range: SeriesRange
  onRangeChange: (range: SeriesRange) => void
  gpuUtilWarn: number
  loading?: boolean
  /** Desaturates the strip while the stream is stale. */
  stale?: boolean
}

export default function SummaryStrip({ summary, sites, hourly, series, seriesError, range, onRangeChange, gpuUtilWarn, loading = false, stale = false }: SummaryStripProps) {
  return (
    <footer
      aria-label="Fleet summary"
      data-stale={stale || undefined}
      className="grid h-[200px] grid-cols-[560px_minmax(0,1fr)] gap-3 border-t border-line bg-surface p-3 data-stale:[filter:saturate(0.4)]"
    >
      <SummaryTiles summary={summary} sites={sites} hourly={hourly} loading={loading} />
      <FleetCharts series={series} range={range} error={seriesError} onRangeChange={onRangeChange} gpuUtilWarn={gpuUtilWarn} />
    </footer>
  )
}
