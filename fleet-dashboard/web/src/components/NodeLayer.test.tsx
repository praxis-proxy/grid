import { render } from '@testing-library/react'
import { beforeEach, describe, expect, it, type Mock, vi } from 'vitest'
import type L from 'leaflet'
import { makeSite } from '../test/fixtures'
import { clusterRadius } from '../lib/glyph'

interface MarkerMock {
  on: Mock
  addTo: Mock
  setLatLng: Mock
  setIcon: Mock
  setZIndexOffset: Mock
  getElement: Mock
  remove: Mock
  handlers: Record<string, () => void>
  latlng: unknown
  icon: unknown
  element: HTMLDivElement
}

interface ClusterMock {
  element: HTMLDivElement
  getElement: Mock
  getChildCount: Mock
  getAllChildMarkers: Mock
}

interface ClusterGroupMock {
  options: { iconCreateFunction: (cluster: { getAllChildMarkers: () => MarkerMock[] }) => { className: string }; maxClusterRadius: number }
  addLayer: Mock
  removeLayer: Mock
  addTo: Mock
  remove: Mock
  refreshClusters: Mock
  on: Mock
  handlers: Record<string, () => void>
  layers: MarkerMock[]
  // Fake cluster icons "rendered" on the group's internal feature group, as
  // the real plugin would create them; tests push onto this to simulate a
  // cluster forming.
  clusters: ClusterMock[]
  _featureGroup: { eachLayer: Mock }
}

const leaflet = vi.hoisted(() => {
  const markers: MarkerMock[] = []
  const groups: ClusterGroupMock[] = []
  const makeCluster = (childCount: number, childMarkers: MarkerMock[]): ClusterMock => {
    const element = document.createElement('div')
    return {
      element,
      getElement: vi.fn(() => element),
      getChildCount: vi.fn(() => childCount),
      getAllChildMarkers: vi.fn(() => childMarkers),
    }
  }
  const makeMarker = (latlng: unknown, opts: { icon?: unknown }): MarkerMock => {
    const marker: MarkerMock = {
      handlers: {},
      latlng,
      icon: opts.icon,
      // Mirrors real Leaflet: a DivIcon's html is injected into the marker's
      // element, both at creation and on setIcon, so tests can query it.
      element: document.createElement('div'),
      on: vi.fn((name: string, cb: () => void) => {
        marker.handlers[name] = cb
        return marker
      }),
      addTo: vi.fn(() => {
        marker.handlers.add?.()
        return marker
      }),
      setLatLng: vi.fn((next: unknown) => {
        marker.latlng = next
        return marker
      }),
      setIcon: vi.fn((next: unknown) => {
        marker.icon = next
        marker.element.innerHTML = (next as { html?: string }).html ?? ''
        return marker
      }),
      setZIndexOffset: vi.fn(() => marker),
      getElement: vi.fn(() => marker.element),
      remove: vi.fn(),
    }
    if (opts.icon) marker.element.innerHTML = (opts.icon as { html?: string }).html ?? ''
    markers.push(marker)
    return marker
  }
  const makeGroup = (options: ClusterGroupMock['options']): ClusterGroupMock => {
    const group: ClusterGroupMock = {
      options,
      layers: [],
      clusters: [],
      handlers: {},
      addLayer: vi.fn((marker: MarkerMock) => {
        group.layers.push(marker)
        marker.handlers.add?.()
        return group
      }),
      removeLayer: vi.fn((marker: MarkerMock) => {
        group.layers = group.layers.filter((m) => m !== marker)
        return group
      }),
      addTo: vi.fn(() => group),
      remove: vi.fn(),
      refreshClusters: vi.fn(() => group),
      on: vi.fn((name: string, cb: () => void) => {
        group.handlers[name] = cb
        return group
      }),
      _featureGroup: { eachLayer: vi.fn((fn: (layer: ClusterMock) => void) => group.clusters.forEach(fn)) },
    }
    groups.push(group)
    return group
  }
  const api = {
    marker: vi.fn(makeMarker),
    divIcon: vi.fn((opts: unknown) => opts),
    markerClusterGroup: vi.fn(makeGroup),
  }
  return { markers, groups, api, makeCluster }
})

vi.mock('leaflet', () => ({ default: leaflet.api }))
vi.mock('leaflet.markercluster', () => ({}))

import NodeLayer from './NodeLayer'

const map = {} as L.Map
const hub = { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 }

function iconClass(marker: MarkerMock): string {
  return (marker.icon as { className: string }).className
}

function renderLayer(props: Partial<React.ComponentProps<typeof NodeLayer>> = {}) {
  const all = { map, sites: [makeSite()], hub, selected: null, onSelect: () => {}, labels: 'unhealthy' as const, ...props }
  return { ...render(<NodeLayer {...all} />), props: all }
}

describe('NodeLayer', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    leaflet.markers.length = 0
    leaflet.groups.length = 0
  })

  it('renders nothing until the map exists', () => {
    renderLayer({ map: null })
    expect(leaflet.api.marker).not.toHaveBeenCalled()
    expect(leaflet.api.markerClusterGroup).not.toHaveBeenCalled()
  })

  it('creates one cluster group on the map with clustering options and animation on', () => {
    renderLayer()
    expect(leaflet.api.markerClusterGroup).toHaveBeenCalledTimes(1)
    expect(leaflet.api.markerClusterGroup).toHaveBeenCalledWith(
      expect.objectContaining({ showCoverageOnHover: false, zoomToBoundsOnClick: true, spiderfyOnMaxZoom: true, animate: true }),
    )
    expect(leaflet.groups[0].addTo).toHaveBeenCalledWith(map)
  })

  it('adds site markers to the cluster group and the hub straight to the map, labelling unhealthy sites only', () => {
    renderLayer({ sites: [makeSite(), makeSite({ name: 'b', displayName: 'B', lat: 51, lng: 0, health: 'red' })] })
    expect(leaflet.markers).toHaveLength(3)
    expect(iconClass(leaflet.markers[0])).toBe('fleet-node fleet-node--green')
    expect(iconClass(leaflet.markers[1])).toBe('fleet-node fleet-node--red fleet-node--labelled')
    expect(iconClass(leaflet.markers[2])).toBe('fleet-node fleet-node--hub fleet-node--labelled')
    expect(leaflet.groups[0].addLayer).toHaveBeenCalledWith(leaflet.markers[0])
    expect(leaflet.groups[0].addLayer).toHaveBeenCalledWith(leaflet.markers[1])
    expect(leaflet.markers[0].addTo).not.toHaveBeenCalled()
    expect(leaflet.markers[2].addTo).toHaveBeenCalledWith(map)
    expect(leaflet.markers[2].latlng).toEqual([38.95, -77.45])
  })

  it('builds cluster icons from the worst member health and sizes the radius from the fleet', () => {
    renderLayer({ sites: [makeSite(), makeSite({ name: 'b', lat: 51, lng: 0, health: 'yellow', gpus: { total: 4, utilPct: 10 } })] })
    const { options } = leaflet.groups[0]
    const icon = options.iconCreateFunction({ getAllChildMarkers: () => [leaflet.markers[0], leaflet.markers[1]] })
    expect(icon.className).toBe('fleet-cluster fleet-cluster--yellow')
    expect(options.maxClusterRadius).toBe(clusterRadius([64, 4]))
  })

  it('captures the radius at group-creation time and rebuilds the group when the fleet changes it', () => {
    const sites = [makeSite({ gpus: { total: 4, utilPct: 10 } }), makeSite({ name: 'b', lat: 51, lng: 0, gpus: { total: 4, utilPct: 10 } })]
    const { rerender } = renderLayer({ sites })
    expect(leaflet.groups).toHaveLength(1)
    expect(leaflet.groups[0].options.maxClusterRadius).toBe(clusterRadius([4, 4]))
    const bigger = [...sites, makeSite({ name: 'c', lat: 10, lng: 10, gpus: { total: 64, utilPct: 50 } })]
    rerender(<NodeLayer map={map} sites={bigger} hub={hub} selected={null} onSelect={() => {}} labels="unhealthy" />)
    expect(leaflet.groups).toHaveLength(2)
    expect(leaflet.groups[1].options.maxClusterRadius).toBe(clusterRadius([4, 4, 64]))
    // Existing markers are re-added to the rebuilt group.
    expect(leaflet.groups[1].addLayer).toHaveBeenCalledWith(leaflet.markers[0])
    expect(leaflet.groups[1].addLayer).toHaveBeenCalledWith(leaflet.markers[1])
  })

  it('refreshes cluster icons when a member health changes', () => {
    const { rerender } = renderLayer({ hub: null })
    expect(leaflet.groups[0].refreshClusters).not.toHaveBeenCalled()
    rerender(<NodeLayer map={map} sites={[makeSite({ health: 'red' })]} hub={null} selected={null} onSelect={() => {}} labels="unhealthy" />)
    expect(leaflet.groups[0].refreshClusters).toHaveBeenCalledTimes(1)
  })

  it('listens for the cluster group animation to end, to re-apply cluster accessibility', () => {
    renderLayer()
    expect(leaflet.groups[0].on).toHaveBeenCalledWith('animationend', expect.any(Function))
  })

  it('gives cluster icons a button role, tabindex and an aria-label naming the count and worst health', () => {
    const { rerender } = renderLayer({ sites: [makeSite(), makeSite({ name: 'b', lat: 51, lng: 0, health: 'yellow' })], hub: null })
    const group = leaflet.groups[0]
    const cluster = leaflet.makeCluster(2, [leaflet.markers[0], leaflet.markers[1]])
    group.clusters.push(cluster)
    // Re-run the sync effect so it re-applies cluster accessibility to the (now-present) fake cluster.
    rerender(
      <NodeLayer
        map={map}
        sites={[makeSite(), makeSite({ name: 'b', lat: 51, lng: 0, health: 'yellow' })]}
        hub={null}
        selected={null}
        onSelect={() => {}}
        labels="unhealthy"
      />,
    )
    expect(cluster.element.getAttribute('role')).toBe('button')
    expect(cluster.element.getAttribute('tabindex')).toBe('0')
    expect(cluster.element.getAttribute('aria-label')).toBe('2 sites, worst health degraded')
  })

  it('labels every site in all mode', () => {
    renderLayer({ labels: 'all' })
    expect(iconClass(leaflet.markers[0])).toBe('fleet-node fleet-node--green fleet-node--labelled')
  })

  it('gives each site marker a button role, tabindex and a descriptive name', () => {
    renderLayer({ sites: [makeSite({ health: 'yellow', reasons: ['GPU utilization 95% >= 90%'], gpus: { total: 12, utilPct: 95 } })] })
    const el = leaflet.markers[0].element
    expect(el.getAttribute('role')).toBe('button')
    expect(el.getAttribute('tabindex')).toBe('0')
    expect(el.getAttribute('aria-label')).toBe('Ohio, degraded, 12 GPUs, 95 percent utilization, GPU utilization 95% >= 90%')
  })

  it('calls onSelect with the site name and the map source on click', () => {
    const onSelect = vi.fn()
    renderLayer({ hub: null, onSelect })
    leaflet.markers[0].handlers.click()
    expect(onSelect).toHaveBeenCalledWith('aigrid-ds-spoke1', 'map')
  })

  it('swaps the icon to the selected glyph and raises it', () => {
    const { rerender } = renderLayer({ hub: null })
    rerender(<NodeLayer map={map} sites={[makeSite()]} hub={null} selected="aigrid-ds-spoke1" onSelect={() => {}} labels="unhealthy" />)
    expect(leaflet.markers).toHaveLength(1)
    expect(iconClass(leaflet.markers[0])).toBe('fleet-node fleet-node--green fleet-node--selected fleet-node--labelled')
    expect(leaflet.markers[0].setZIndexOffset).toHaveBeenLastCalledWith(1000)
  })

  it('re-adds a moved marker to the group and removes markers for sites that disappear', () => {
    const { rerender } = renderLayer({ sites: [makeSite(), makeSite({ name: 'b', lat: 51, lng: 0 })], hub: null })
    rerender(<NodeLayer map={map} sites={[makeSite({ lat: 41, lng: -83 })]} hub={null} selected={null} onSelect={() => {}} labels="unhealthy" />)
    expect(leaflet.markers).toHaveLength(2)
    expect(leaflet.markers[0].setLatLng).toHaveBeenCalledWith([41, -83])
    expect(leaflet.groups[0].removeLayer).toHaveBeenCalledWith(leaflet.markers[0])
    expect(leaflet.groups[0].addLayer).toHaveBeenCalledTimes(3)
    expect(leaflet.groups[0].removeLayer).toHaveBeenCalledWith(leaflet.markers[1])
    expect(leaflet.groups[0].layers).toEqual([leaflet.markers[0]])
  })

  it('removes the group, the markers and the hub on unmount', () => {
    const { unmount } = renderLayer()
    unmount()
    expect(leaflet.groups[0].remove).toHaveBeenCalled()
    expect(leaflet.markers[1].remove).toHaveBeenCalled()
  })

  it('skips setIcon and setLatLng on an unchanged snapshot', () => {
    const sites = [makeSite(), makeSite({ name: 'b', displayName: 'B', lat: 51, lng: 0, health: 'yellow' })]
    const { rerender } = renderLayer({ sites })
    // A fresh array with content-equal sites, as a new poll would produce.
    const samePayload = [makeSite(), makeSite({ name: 'b', displayName: 'B', lat: 51, lng: 0, health: 'yellow' })]
    rerender(<NodeLayer map={map} sites={samePayload} hub={{ ...hub }} selected={null} onSelect={() => {}} labels="unhealthy" />)
    for (const marker of leaflet.markers) {
      // Markers are created with their glyph icon already set (F9), so an
      // unchanged snapshot never needs setIcon at all.
      expect(marker.setIcon).not.toHaveBeenCalled()
      expect(marker.setLatLng).not.toHaveBeenCalled()
    }
  })

  it('creates markers with their glyph icon already set, instead of the default icon plus an immediate setIcon', () => {
    renderLayer({ sites: [makeSite({ health: 'yellow' })], hub: null })
    const [markerCall] = leaflet.api.marker.mock.calls
    const [, opts] = markerCall as [unknown, { icon?: { className?: string } }]
    expect(opts.icon).toBeDefined()
    expect(opts.icon?.className).toBe('fleet-node fleet-node--yellow fleet-node--labelled')
    expect(leaflet.markers[0].setIcon).not.toHaveBeenCalled()
  })

  it('calls setIcon and setLatLng again once the glyph or position actually changes', () => {
    const { rerender } = renderLayer({ hub: null })
    rerender(<NodeLayer map={map} sites={[makeSite({ health: 'yellow' })]} hub={null} selected={null} onSelect={() => {}} labels="unhealthy" />)
    expect(leaflet.markers[0].setIcon).toHaveBeenCalledTimes(1)
    rerender(<NodeLayer map={map} sites={[makeSite({ health: 'yellow', lat: 41, lng: -83 })]} hub={null} selected={null} onSelect={() => {}} labels="unhealthy" />)
    expect(leaflet.markers[0].setLatLng).toHaveBeenCalledTimes(1)
  })

  it('updates a down site label in place on a lastSeen tick, without restarting the halo via setIcon', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
    const site = makeSite({ health: 'red', lastSeen: '2026-09-06T11:59:30Z' })
    const { rerender } = renderLayer({ sites: [site], hub: null })
    const marker = leaflet.markers[0]
    expect(marker.setIcon).not.toHaveBeenCalled()
    expect(marker.element.querySelectorAll('.fleet-node__label span')[1]?.textContent).toBe('unreachable · last seen 30s')

    // Same site payload; only the poll clock (and so the label's age text) has moved on.
    vi.setSystemTime(new Date('2026-09-06T12:00:45Z'))
    rerender(<NodeLayer map={map} sites={[{ ...site }]} hub={null} selected={null} onSelect={() => {}} labels="unhealthy" />)

    expect(marker.setIcon).not.toHaveBeenCalled()
    expect(marker.element.querySelectorAll('.fleet-node__label span')[1]?.textContent).toBe('unreachable · last seen 1m')
    vi.useRealTimers()
  })
})
