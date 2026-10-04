import { useCallback, useEffect, useRef, useState } from 'react'
import L from 'leaflet'
import type { FeatureCollection } from 'geojson'
import type { Hub, Route, Site } from '../api/types'
import type { SelectionSource } from '../hooks/useSelectedSite'
import { placedPoints, planFit } from '../lib/bounds'
import { GRATICULE_COLOR, graticuleLines } from '../lib/graticule'
import { COAST_COLOR, LAND_COLOR } from '../lib/health'
import type { LabelMode, LinkMode } from '../lib/mapModes'
import LinkLayer from './LinkLayer'
import MapLegend from './MapLegend'
import MapTools from './MapTools'
import NodeLayer from './NodeLayer'

export interface FleetMapProps {
  /** Only sites with placed === true; unplaced sites live in the roster with a "no position" tag. */
  sites: Site[]
  hub: Hub | null
  /** Registered routes from the snapshot (empty in v1); non-empty routes replace the hub-to-site links. */
  routes: Route[]
  selected: string | null
  /** Where the selection came from; the map pans to it unless it made the selection itself. */
  selectedSource: SelectionSource | null
  onSelect: (name: string, source: SelectionSource) => void
}

const WORLD_STYLE: L.PathOptions = { fillColor: LAND_COLOR, fillOpacity: 1, color: COAST_COLOR, weight: 1 }
const BASEMAP_PANE = 'basemap'
const INITIAL_CENTER: L.LatLngTuple = [20, 0]
const INITIAL_ZOOM = 2
const FIT_PADDING: L.PointTuple = [60, 60]
const FIT_MAX_ZOOM = 6
// Drawn on the basemap pane before the world loads, so land covers it and only the ocean shows the grid.
const GRATICULE_STYLE: L.PolylineOptions = { color: GRATICULE_COLOR, weight: 1, opacity: 1, interactive: false, pane: BASEMAP_PANE }

function fitKey(sites: Site[], hub: Hub | null): string {
  return [...sites.map((s) => s.name), hub?.name ?? ''].sort().join('|')
}

/** Frame every placed site and the hub. No-op when nothing is placed. */
function fitView(map: L.Map, sites: Site[], hub: Hub | null): void {
  const plan = planFit(placedPoints(sites, hub))
  if (plan.kind === 'single') {
    map.setView(plan.center, plan.zoom)
  } else if (plan.kind === 'bounds') {
    map.fitBounds(L.latLngBounds(plan.bounds[0], plan.bounds[1]), { padding: FIT_PADDING, maxZoom: FIT_MAX_ZOOM })
  }
}

export default function FleetMap({ sites, hub, routes, selected, selectedSource, onSelect }: FleetMapProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [map, setMap] = useState<L.Map | null>(null)
  const [links, setLinks] = useState<LinkMode>('all')
  const [labels, setLabels] = useState<LabelMode>('unhealthy')
  const fitKeyRef = useRef('')
  const handledRef = useRef<string | null>(null)

  // Create the map once the container is mounted.
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    // No tile layer means nothing to attribute; the vector world is bundled (Natural Earth, public domain).
    const created = L.map(el, { zoomControl: false, attributionControl: false, minZoom: 2, maxZoom: 12, worldCopyJump: true })
    // The base world map is a vector layer; give it a pane below the default
    // overlayPane (z-index 400) so hub->spoke lines drawn there render above the
    // country fills instead of being hidden by them. Markers sit higher still.
    created.createPane(BASEMAP_PANE)
    const basePane = created.getPane(BASEMAP_PANE)
    if (basePane) basePane.style.zIndex = '250'
    created.setView(INITIAL_CENTER, INITIAL_ZOOM)
    L.polyline(graticuleLines(), GRATICULE_STYLE).addTo(created)
    L.control.zoom({ position: 'bottomright' }).addTo(created)
    setMap(created)
    return () => {
      created.remove()
      setMap(null)
    }
  }, [])

  // The map column grows and shrinks as the panel column docks and undocks;
  // Leaflet only re-measures itself when told to.
  useEffect(() => {
    const el = containerRef.current
    if (!map || !el) return
    const observer = new ResizeObserver(() => map.invalidateSize())
    observer.observe(el)
    return () => observer.disconnect()
  }, [map])

  // Draw the bundled world as the permanent base map. No external tile service
  // is contacted; the ocean is the container background and the landmasses come
  // from the vector world shipped with the app.
  useEffect(() => {
    if (!map) return
    let cancelled = false
    let world: L.GeoJSON | null = null
    void import('../assets/world-110m.geo.json').then((mod) => {
      if (cancelled) return
      world = L.geoJSON(mod.default as FeatureCollection, {
        style: () => WORLD_STYLE,
        interactive: false,
        pane: BASEMAP_PANE,
      })
      world.addTo(map)
    })
    return () => {
      cancelled = true
      world?.remove()
    }
  }, [map])

  // Fit the view when the set of placed sites (or the hub) changes, not on every metric update.
  useEffect(() => {
    if (!map) return
    const key = fitKey(sites, hub)
    if (key === fitKeyRef.current || placedPoints(sites, hub).length === 0) return
    fitKeyRef.current = key
    fitView(map, sites, hub)
  }, [map, sites, hub])

  // Bring a site picked elsewhere (roster, attention strip, deep link) into view when it is off screen.
  // Keyed on selected+selectedSource so a rebuilt `sites` array from the next SSE
  // snapshot does not re-trigger the pan for a selection already handled.
  useEffect(() => {
    if (selected === null) {
      handledRef.current = null
      return
    }
    if (!map) return
    const key = `${selected}:${selectedSource}`
    if (handledRef.current === key) return
    const site = sites.find((s) => s.name === selected)
    if (!site || site.lat === null || site.lng === null) return
    if (selectedSource !== 'map') {
      const target = L.latLng(site.lat, site.lng)
      if (!map.getBounds().contains(target)) map.panTo(target)
    }
    handledRef.current = key
  }, [map, sites, selected, selectedSource])

  const fit = useCallback(() => {
    if (map) fitView(map, sites, hub)
  }, [map, sites, hub])

  return (
    <div className="relative h-full w-full bg-bg">
      <div ref={containerRef} className="h-full w-full bg-bg" role="region" aria-label="Fleet map" />
      <LinkLayer map={map} sites={sites} hub={hub} routes={routes} mode={links} selected={selected} />
      <NodeLayer map={map} sites={sites} hub={hub} selected={selected} onSelect={onSelect} labels={labels} />
      {/* Overlays sit above Leaflet's controls (z-index 1000). */}
      <div className="absolute top-3 right-3 z-[1001]">
        <MapTools links={links} onLinksChange={setLinks} labels={labels} onLabelsChange={setLabels} onFit={fit} />
      </div>
      <div className="absolute bottom-3 left-3 z-[1001]">
        <MapLegend />
      </div>
    </div>
  )
}
