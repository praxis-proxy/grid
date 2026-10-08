import { useMemo, useState } from 'react'
import type { Health, Hub, SeriesRange } from './api/types'
import AttentionStrip from './components/AttentionStrip'
import FleetMap from './components/FleetMap'
import FleetOverlay, { type FleetOverlayKind } from './components/FleetOverlay'
import RosterRail from './components/RosterRail'
import SitePanel from './components/SitePanel'
import StaleBanner from './components/StaleBanner'
import SummaryStrip from './components/SummaryStrip'
import TopBar from './components/TopBar'
import { useConfig } from './hooks/useConfig'
import { useFleet } from './hooks/useFleet'
import { NARROW_SCREEN_QUERY, useMediaQuery } from './hooks/useMediaQuery'
import { useSelectedSite } from './hooks/useSelectedSite'
import { useSeries } from './hooks/useSeries'
import { mainColumns } from './lib/layout'

export default function App() {
  const config = useConfig()
  const fleet = useFleet(config.pollIntervalSeconds)
  const { selected, source: selectedSource, select, clear } = useSelectedSite()
  const [range, setRange] = useState<SeriesRange>('1h')
  const [healthFilter, setHealthFilter] = useState<Health | null>(null)
  const [rosterCollapsed, setRosterCollapsed] = useState(false)
  const narrow = useMediaQuery(NARROW_SCREEN_QUERY)
  const { series, error: seriesError } = useSeries(range)
  // Tile deltas always compare against one hour ago, whatever the chart shows.
  const { series: hourly } = useSeries('1h')
  const snapshot = fleet.snapshot
  const hub: Hub | null = snapshot?.hub ?? config.hub
  const sites = useMemo(() => snapshot?.sites ?? [], [snapshot])
  const placed = useMemo(() => sites.filter((site) => site.placed), [sites])
  const selectedSite = useMemo(() => sites.find((site) => site.name === selected) ?? null, [sites, selected])
  const collapsed = narrow || rosterCollapsed
  const stale = fleet.status === 'stale'
  const overlay: FleetOverlayKind | null =
    snapshot === null
      ? fleet.errorStatus === 503
        ? 'waiting'
        : fleet.error !== null
          ? 'error'
          : 'loading'
      : sites.length === 0
        ? 'empty'
        : null

  return (
    <div className="grid h-screen grid-rows-[48px_minmax(0,1fr)_200px] bg-bg text-ink">
      <TopBar
        hub={hub}
        version={config.version}
        status={fleet.status}
        lastUpdate={fleet.lastUpdate}
        summary={snapshot?.summary ?? null}
        healthFilter={healthFilter}
        onHealthFilterChange={setHealthFilter}
        user={config.user}
      />
      <main className="grid min-h-0" style={{ gridTemplateColumns: mainColumns(collapsed, selectedSite !== null) }}>
        <RosterRail
          sites={sites}
          selected={selected}
          onSelect={select}
          onClear={clear}
          healthFilter={healthFilter}
          pollIntervalSeconds={config.pollIntervalSeconds}
          collapsed={collapsed}
          onCollapsedChange={setRosterCollapsed}
          forced={narrow}
          loading={overlay === 'loading' || overlay === 'waiting'}
        />
        <div className="relative flex min-h-0 min-w-0 flex-col">
          {stale ? <StaleBanner lastUpdate={fleet.lastUpdate} /> : null}
          <div className="relative min-h-0 flex-1">
          {/*
            Rendered before FleetMap (and its map tools/Leaflet container) so the
            attention chips are reached by Tab before the zoom control and map
            tools, even though they visually sit over the map's top-left corner
            via absolute positioning below -- capped so it never runs under the
            map tools at the top right.
          */}
          <div className="pointer-events-none absolute top-3 left-3 z-[1001] max-w-[calc(100%-460px)] [&>*]:pointer-events-auto">
            <AttentionStrip sites={sites} selected={selected} onSelect={select} />
          </div>
          <FleetMap sites={placed} hub={hub} routes={snapshot?.routes ?? []} selected={selected} selectedSource={selectedSource} onSelect={select} />
          {overlay ? <FleetOverlay kind={overlay} message={fleet.error ?? undefined} onRetry={fleet.retry} /> : null}
          </div>
        </div>
        {selectedSite ? (
          <aside className="min-h-0 border-l border-line bg-surface">
            <SitePanel site={selectedSite} onClose={clear} refreshKey={snapshot?.generatedAt} thresholds={config.thresholds} />
          </aside>
        ) : null}
      </main>
      <SummaryStrip
        summary={snapshot?.summary ?? null}
        sites={sites}
        hourly={hourly}
        series={series}
        seriesError={seriesError}
        range={range}
        onRangeChange={setRange}
        gpuUtilWarn={config.thresholds.gpuUtilWarn}
        loading={overlay === 'loading' || overlay === 'waiting'}
        stale={stale}
      />
    </div>
  )
}
