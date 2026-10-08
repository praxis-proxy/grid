import type { Hub, Site } from '../api/types'

export type LatLng = [number, number]

export type FitPlan =
  | { kind: 'empty' }
  | { kind: 'single'; center: LatLng; zoom: number }
  | { kind: 'bounds'; bounds: [LatLng, LatLng] }

export const SINGLE_POINT_ZOOM = 4

export function placedPoints(sites: readonly Site[], hub: Hub | null): LatLng[] {
  const points: LatLng[] = []
  for (const site of sites) {
    if (site.placed && site.lat !== null && site.lng !== null) points.push([site.lat, site.lng])
  }
  if (hub && hub.lat !== null && hub.lng !== null) points.push([hub.lat, hub.lng])
  return points
}

export function planFit(points: readonly LatLng[]): FitPlan {
  if (points.length === 0) return { kind: 'empty' }
  let minLat = points[0][0]
  let maxLat = points[0][0]
  let minLng = points[0][1]
  let maxLng = points[0][1]
  for (const [lat, lng] of points) {
    minLat = Math.min(minLat, lat)
    maxLat = Math.max(maxLat, lat)
    minLng = Math.min(minLng, lng)
    maxLng = Math.max(maxLng, lng)
  }
  if (minLat === maxLat && minLng === maxLng) {
    return { kind: 'single', center: [minLat, minLng], zoom: SINGLE_POINT_ZOOM }
  }
  return {
    kind: 'bounds',
    bounds: [
      [minLat, minLng],
      [maxLat, maxLng],
    ],
  }
}
