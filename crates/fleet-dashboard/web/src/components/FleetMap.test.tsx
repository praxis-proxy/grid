import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, type Mock, vi } from 'vitest'
import { makeSite } from '../test/fixtures'

interface LayerMock {
  on: Mock
  addTo: Mock
  remove: Mock
}
interface MarkerMock extends LayerMock {
  setLatLng: Mock
  setIcon: Mock
  setZIndexOffset: Mock
  getElement: Mock
}
interface MapMock {
  setView: Mock
  fitBounds: Mock
  remove: Mock
  createPane: Mock
  getPane: Mock
  invalidateSize: Mock
  getBounds: Mock
  panTo: Mock
}

// Explicit annotations avoid TS7022 (circular inference) on objects whose methods return themselves.
const leaflet = vi.hoisted(() => {
  const geoLayer: LayerMock = { on: vi.fn(() => geoLayer), addTo: vi.fn(() => geoLayer), remove: vi.fn() }
  const geoData: unknown[] = []
  const geoOptions: Array<{ pane?: string }> = []
  const bounds = { contains: vi.fn(() => true) }
  const map: MapMock = {
    setView: vi.fn(),
    fitBounds: vi.fn(),
    remove: vi.fn(),
    createPane: vi.fn(() => ({ style: { zIndex: '' } })),
    getPane: vi.fn(() => undefined),
    invalidateSize: vi.fn(),
    getBounds: vi.fn(() => bounds),
    panTo: vi.fn(),
  }
  const zoomControl = { addTo: vi.fn() }
  const makeMarker = (): MarkerMock => {
    const marker: MarkerMock = {
      on: vi.fn(() => marker),
      addTo: vi.fn(() => marker),
      setLatLng: vi.fn(() => marker),
      setIcon: vi.fn(() => marker),
      setZIndexOffset: vi.fn(() => marker),
      getElement: vi.fn(() => undefined),
      remove: vi.fn(),
    }
    return marker
  }
  const makePolyline = (): LayerMock & { setLatLngs: Mock; setStyle: Mock } => {
    const line = { on: vi.fn(() => line), addTo: vi.fn(() => line), setLatLngs: vi.fn(() => line), setStyle: vi.fn(() => line), remove: vi.fn() }
    return line
  }
  const clusterGroup = {
    addLayer: vi.fn(),
    removeLayer: vi.fn(),
    addTo: vi.fn(),
    remove: vi.fn(),
    refreshClusters: vi.fn(),
    on: vi.fn(() => clusterGroup),
    _featureGroup: { eachLayer: vi.fn() },
  }
  const api = {
    map: vi.fn(() => map),
    markerClusterGroup: vi.fn(() => clusterGroup),
    tileLayer: vi.fn(),
    geoJSON: vi.fn((data: unknown, options?: { pane?: string }) => {
      geoData.push(data)
      geoOptions.push(options ?? {})
      return geoLayer
    }),
    latLngBounds: vi.fn((a: unknown, b: unknown) => [a, b]),
    latLng: vi.fn((lat: number, lng: number) => ({ lat, lng })),
    marker: vi.fn(() => makeMarker()),
    polyline: vi.fn(() => makePolyline()),
    divIcon: vi.fn((opts: unknown) => opts),
    control: { zoom: vi.fn(() => zoomControl) },
  }
  return { geoLayer, geoData, geoOptions, map, bounds, api }
})

/** Controllable ResizeObserver so the test can fire a resize. */
class FakeResizeObserver {
  static instances: FakeResizeObserver[] = []
  readonly callback: ResizeObserverCallback
  observed: Element[] = []
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback
    FakeResizeObserver.instances.push(this)
  }
  observe(el: Element): void {
    this.observed.push(el)
  }
  unobserve(): void {}
  disconnect(): void {}
  trigger(): void {
    this.callback([], this as unknown as ResizeObserver)
  }
}

vi.mock('leaflet', () => ({ default: leaflet.api }))
vi.mock('leaflet.markercluster', () => ({}))

import FleetMap from './FleetMap'

const hub = { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 }

function renderMap(sites = [makeSite()], hubValue: typeof hub | null = hub) {
  return render(<FleetMap sites={sites} hub={hubValue} routes={[]} selected={null} selectedSource={null} onSelect={() => {}} />)
}

describe('FleetMap', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    leaflet.geoData.length = 0
    leaflet.geoOptions.length = 0
    leaflet.bounds.contains.mockReturnValue(true)
    FakeResizeObserver.instances = []
    vi.stubGlobal('ResizeObserver', FakeResizeObserver)
  })
  afterEach(() => vi.unstubAllGlobals())

  it('re-measures the map when its container resizes', () => {
    renderMap()
    expect(FakeResizeObserver.instances).toHaveLength(1)
    FakeResizeObserver.instances[0].trigger()
    expect(leaflet.map.invalidateSize).toHaveBeenCalledTimes(1)
  })

  it('pans to a site selected from the roster only when it is off screen, never for map clicks', () => {
    const sites = [makeSite({ lat: 40, lng: -82 }), makeSite({ name: 'b', lat: 51, lng: 0 })]
    const { rerender } = render(<FleetMap sites={sites} hub={hub} routes={[]} selected={null} selectedSource={null} onSelect={() => {}} />)
    leaflet.bounds.contains.mockReturnValue(false)
    rerender(<FleetMap sites={sites} hub={hub} routes={[]} selected="b" selectedSource="roster" onSelect={() => {}} />)
    expect(leaflet.map.panTo).toHaveBeenCalledWith({ lat: 51, lng: 0 })
    rerender(<FleetMap sites={sites} hub={hub} routes={[]} selected="aigrid-ds-spoke1" selectedSource="map" onSelect={() => {}} />)
    expect(leaflet.map.panTo).toHaveBeenCalledTimes(1)
    leaflet.bounds.contains.mockReturnValue(true)
    rerender(<FleetMap sites={sites} hub={hub} routes={[]} selected="b" selectedSource="url" onSelect={() => {}} />)
    expect(leaflet.map.panTo).toHaveBeenCalledTimes(1)
  })

  it('does not re-pan on every snapshot once a roster/URL selection has been handled', () => {
    const sites = [makeSite({ lat: 40, lng: -82 }), makeSite({ name: 'b', lat: 51, lng: 0 })]
    leaflet.bounds.contains.mockReturnValue(false)
    const { rerender } = render(<FleetMap sites={sites} hub={hub} routes={[]} selected="b" selectedSource="roster" onSelect={() => {}} />)
    expect(leaflet.map.panTo).toHaveBeenCalledTimes(1)
    rerender(<FleetMap sites={[...sites]} hub={hub} routes={[]} selected="b" selectedSource="roster" onSelect={() => {}} />)
    expect(leaflet.map.panTo).toHaveBeenCalledTimes(1)
    rerender(<FleetMap sites={[...sites]} hub={hub} routes={[]} selected="aigrid-ds-spoke1" selectedSource="roster" onSelect={() => {}} />)
    expect(leaflet.map.panTo).toHaveBeenCalledTimes(2)
  })

  it('creates the map exactly once, without the attribution control', () => {
    renderMap()
    expect(leaflet.api.map).toHaveBeenCalledTimes(1)
    expect(leaflet.api.map).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ attributionControl: false }))
  })

  it('never contacts an external tile service', async () => {
    renderMap()
    await waitFor(() => expect(leaflet.api.geoJSON).toHaveBeenCalledTimes(1))
    expect(leaflet.api.tileLayer).not.toHaveBeenCalled()
  })

  it('draws the bundled world as the base map', async () => {
    renderMap()
    await waitFor(() => expect(leaflet.api.geoJSON).toHaveBeenCalledTimes(1))
    const data = leaflet.geoData[0] as { features: unknown[] }
    expect(data.features.length).toBe(177)
    expect(leaflet.geoLayer.addTo).toHaveBeenCalledWith(leaflet.map)
  })

  it('draws a 15 degree graticule on the basemap pane before the world', () => {
    renderMap()
    // The graticule is drawn first; hub-to-site links follow it.
    expect(leaflet.api.polyline).toHaveBeenCalled()
    const [lines, options] = leaflet.api.polyline.mock.calls[0] as unknown as [unknown[], { pane: string; color: string }]
    expect(lines).toHaveLength(35)
    expect(options.pane).toBe('basemap')
    expect(options.color).toBe('#141b21')
  })

  it('draws the world in a pane below the overlay so links stay visible', async () => {
    renderMap()
    await waitFor(() => expect(leaflet.api.geoJSON).toHaveBeenCalledTimes(1))
    expect(leaflet.map.createPane).toHaveBeenCalledWith('basemap')
    expect(leaflet.geoOptions[0].pane).toBe('basemap')
  })

  it('fits bounds around sites and hub with padding', () => {
    renderMap([makeSite({ lat: 40, lng: -82 }), makeSite({ name: 'b', lat: 51, lng: 0 })])
    expect(leaflet.api.latLngBounds).toHaveBeenCalledWith([38.95, -82], [51, 0])
    expect(leaflet.map.fitBounds).toHaveBeenCalledWith([[38.95, -82], [51, 0]], expect.objectContaining({ padding: [60, 60] }))
  })

  it('puts the zoom control bottom right and shows the tools and legend', () => {
    renderMap()
    expect(leaflet.api.control.zoom).toHaveBeenCalledWith({ position: 'bottomright' })
    expect(screen.getByRole('toolbar', { name: 'Map tools' })).toBeInTheDocument()
    expect(screen.getByRole('complementary', { name: 'Map legend' })).toBeInTheDocument()
  })

  it('re-frames the fleet when Fit is pressed', () => {
    renderMap([makeSite({ lat: 40, lng: -82 }), makeSite({ name: 'b', lat: 51, lng: 0 })])
    expect(leaflet.map.fitBounds).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Fit' }))
    expect(leaflet.map.fitBounds).toHaveBeenCalledTimes(2)
  })

  it('draws links to every site by default and only the selected one when switched to selected', () => {
    const sites = [makeSite({ lat: 40, lng: -82 }), makeSite({ name: 'b', lat: 51, lng: 0 })]
    const { rerender } = render(<FleetMap sites={sites} hub={hub} routes={[]} selected={null} selectedSource={null} onSelect={() => {}} />)
    // One polyline is the graticule; links come on top of that count.
    expect(leaflet.api.polyline).toHaveBeenCalledTimes(3)
    expect(within(screen.getByRole('group', { name: 'Links' })).getByRole('button', { name: 'all' })).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(within(screen.getByRole('group', { name: 'Links' })).getByRole('button', { name: 'selected' }))
    // The first polyline is the graticule; the two links are removed once nothing is selected.
    const links = leaflet.api.polyline.mock.results.slice(1).map((result) => result.value as { remove: Mock })
    expect(links).toHaveLength(2)
    for (const line of links) expect(line.remove).toHaveBeenCalledTimes(1)
    rerender(<FleetMap sites={sites} hub={hub} routes={[]} selected="b" selectedSource="roster" onSelect={() => {}} />)
    expect(leaflet.api.polyline).toHaveBeenCalledTimes(4)
  })

  it('centers at zoom 4 on a single point', () => {
    renderMap([makeSite({ lat: 40, lng: -82 })], null)
    expect(leaflet.map.setView).toHaveBeenLastCalledWith([40, -82], 4)
  })

  it('does not refit on every snapshot when the site set is unchanged', () => {
    const { rerender } = renderMap([makeSite({ lat: 40, lng: -82 }), makeSite({ name: 'b', lat: 51, lng: 0 })])
    expect(leaflet.map.fitBounds).toHaveBeenCalledTimes(1)
    rerender(
      <FleetMap
        sites={[makeSite({ lat: 40, lng: -82, gpus: { total: 64, utilPct: 70 } }), makeSite({ name: 'b', lat: 51, lng: 0 })]}
        hub={hub}
        routes={[]}
        selected={null}
        selectedSource={null}
        onSelect={() => {}}
      />,
    )
    expect(leaflet.map.fitBounds).toHaveBeenCalledTimes(1)
  })
})
