import { render } from '@testing-library/react'
import { beforeEach, describe, expect, it, type Mock, vi } from 'vitest'
import type L from 'leaflet'
import { makeSite } from '../test/fixtures'

interface PolylineMock {
  addTo: Mock
  setLatLngs: Mock
  setStyle: Mock
  remove: Mock
  latlngs: [number, number][]
  options: { dashArray: string; opacity: number }
}

const leaflet = vi.hoisted(() => {
  const polylines: PolylineMock[] = []
  const makePolyline = (latlngs: [number, number][], options: PolylineMock['options']): PolylineMock => {
    const line: PolylineMock = {
      latlngs,
      options,
      addTo: vi.fn(() => line),
      setLatLngs: vi.fn((next: [number, number][]) => {
        line.latlngs = next
        return line
      }),
      setStyle: vi.fn((next: Partial<PolylineMock['options']>) => {
        line.options = { ...line.options, ...next }
        return line
      }),
      remove: vi.fn(),
    }
    polylines.push(line)
    return line
  }
  const api = { polyline: vi.fn(makePolyline) }
  return { polylines, api }
})

vi.mock('leaflet', () => ({ default: leaflet.api }))

import LinkLayer from './LinkLayer'

const map = {} as L.Map
const hub = { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 }
const a = makeSite({ lat: 41, lng: -83 })
const b = makeSite({ name: 'b', lat: 51, lng: 0 })

function renderLayer(props: Partial<React.ComponentProps<typeof LinkLayer>> = {}) {
  const all = { map, sites: [a, b], hub, routes: [], mode: 'all' as const, selected: null, ...props }
  return render(<LinkLayer {...all} />)
}

const endpoints = (line: PolylineMock) => [line.latlngs[0], line.latlngs[line.latlngs.length - 1]]

describe('LinkLayer', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    leaflet.polylines.length = 0
  })

  it('renders nothing until the map exists', () => {
    renderLayer({ map: null })
    expect(leaflet.api.polyline).not.toHaveBeenCalled()
  })

  it('draws one dashed curved hub->site line per placed site at full opacity in all mode with no selection', () => {
    renderLayer()
    expect(leaflet.polylines).toHaveLength(2)
    expect(endpoints(leaflet.polylines[0])).toEqual([[38.95, -77.45], [41, -83]])
    expect(leaflet.polylines[0].latlngs).toHaveLength(25)
    expect(leaflet.polylines[0].options.dashArray).toBe('2 6')
    expect(leaflet.polylines[0].options.opacity).toBe(0.9)
    for (const line of leaflet.polylines) expect(line.addTo).toHaveBeenCalledWith(map)
  })

  it('draws only the selected site link in selected mode at full opacity, and nothing in none mode', () => {
    const { rerender } = renderLayer({ mode: 'selected', selected: 'b' })
    expect(leaflet.polylines).toHaveLength(1)
    expect(endpoints(leaflet.polylines[0])[1]).toEqual([51, 0])
    expect(leaflet.polylines[0].options.opacity).toBe(0.9)
    rerender(<LinkLayer map={map} sites={[a, b]} hub={hub} routes={[]} mode="none" selected="b" />)
    expect(leaflet.polylines[0].remove).toHaveBeenCalledTimes(1)
  })

  it('fades the other links in all mode while a site is selected and restores them when cleared', () => {
    const { rerender } = renderLayer()
    rerender(<LinkLayer map={map} sites={[a, b]} hub={hub} routes={[]} mode="all" selected="b" />)
    expect(leaflet.polylines[0].setStyle).toHaveBeenCalledWith({ opacity: 0.35 })
    expect(leaflet.polylines[1].setStyle).not.toHaveBeenCalled()
    rerender(<LinkLayer map={map} sites={[a, b]} hub={hub} routes={[]} mode="all" selected={null} />)
    expect(leaflet.polylines[0].setStyle).toHaveBeenLastCalledWith({ opacity: 0.9 })
  })

  it('follows registered routes when the snapshot has them', () => {
    renderLayer({ routes: [{ from: 'aigrid-ds-spoke1', to: 'b' }] })
    expect(leaflet.polylines).toHaveLength(1)
    expect(endpoints(leaflet.polylines[0])).toEqual([[41, -83], [51, 0]])
  })

  it('draws no line when the hub has no coordinates and skips unplaced sites', () => {
    renderLayer({ hub: { ...hub, lat: null, lng: null } })
    expect(leaflet.api.polyline).not.toHaveBeenCalled()
    renderLayer({ sites: [makeSite({ lat: null, lng: null, placed: false })] })
    expect(leaflet.api.polyline).not.toHaveBeenCalled()
  })

  it('repositions in place and removes lines for sites that disappear', () => {
    const { rerender } = renderLayer()
    rerender(<LinkLayer map={map} sites={[makeSite({ lat: 45, lng: -122 })]} hub={hub} routes={[]} mode="all" selected={null} />)
    expect(leaflet.polylines).toHaveLength(2)
    expect(leaflet.polylines[0].setLatLngs).toHaveBeenCalledTimes(1)
    expect(endpoints(leaflet.polylines[0])[1]).toEqual([45, -122])
    expect(leaflet.polylines[1].remove).toHaveBeenCalledTimes(1)
  })

  it('skips setLatLngs and setStyle on an unchanged snapshot', () => {
    const { rerender } = renderLayer({ sites: [a] })
    rerender(<LinkLayer map={map} sites={[makeSite({ lat: 41, lng: -83 })]} hub={{ ...hub }} routes={[]} mode="all" selected={null} />)
    expect(leaflet.polylines[0].setLatLngs).not.toHaveBeenCalled()
    expect(leaflet.polylines[0].setStyle).not.toHaveBeenCalled()
  })

  it('removes every line once the hub loses its coordinates, and on unmount', () => {
    const { rerender, unmount } = renderLayer({ sites: [a] })
    rerender(<LinkLayer map={map} sites={[a]} hub={{ ...hub, lat: null, lng: null }} routes={[]} mode="all" selected={null} />)
    expect(leaflet.polylines[0].remove).toHaveBeenCalledTimes(1)
    rerender(<LinkLayer map={map} sites={[a]} hub={hub} routes={[]} mode="all" selected={null} />)
    unmount()
    expect(leaflet.polylines[1].remove).toHaveBeenCalledTimes(1)
  })
})
