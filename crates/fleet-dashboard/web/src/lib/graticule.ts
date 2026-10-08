import type { LatLng } from './bounds'

export const GRATICULE_STEP_DEG = 15
export const GRATICULE_COLOR = '#141b21'
/** Leaflet's Web Mercator projection is undefined at the poles; stop the meridians short of them. */
const LAT_LIMIT = 85

/**
 * Meridians every `step` degrees from -180 to 165 and parallels every `step`
 * degrees from -75 to 75, each as a two-point line. Meridians run from -85 to 85.
 */
export function graticuleLines(step = GRATICULE_STEP_DEG): LatLng[][] {
  const lines: LatLng[][] = []
  for (let lng = -180; lng < 180; lng += step) {
    lines.push([
      [-LAT_LIMIT, lng],
      [LAT_LIMIT, lng],
    ])
  }
  for (let lat = -90 + step; lat < 90; lat += step) {
    lines.push([
      [lat, -180],
      [lat, 180],
    ])
  }
  return lines
}
