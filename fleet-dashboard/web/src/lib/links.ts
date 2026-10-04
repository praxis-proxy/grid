import type { Hub, Route, Site } from '../api/types'
import type { LatLng } from './bounds'
import type { LinkMode } from './mapModes'

export interface ResolvedLink {
  /** "from>to" by node name; stable across polls so the layer can update lines in place. */
  key: string
  from: LatLng
  to: LatLng
  /** In "all" mode with a selected site, every link not touching it is drawn faint (0.35). */
  muted: boolean
}

export const LINK_OPACITY = 0.9
export const LINK_OPACITY_MUTED = 0.35

function positions(sites: readonly Site[], hub: Hub | null): Map<string, LatLng> {
  const out = new Map<string, LatLng>()
  for (const site of sites) {
    if (site.placed && site.lat !== null && site.lng !== null) out.set(site.name, [site.lat, site.lng])
  }
  if (hub && hub.lat !== null && hub.lng !== null) out.set(hub.name, [hub.lat, hub.lng])
  return out
}

/**
 * Which links to draw. With registered routes the pairs come from `routes`;
 * otherwise every placed site is assumed to route through the hub. Pairs whose
 * endpoints are not placed are dropped.
 */
export function resolveLinks(
  sites: readonly Site[],
  hub: Hub | null,
  routes: readonly Route[],
  mode: LinkMode,
  selected: string | null,
): ResolvedLink[] {
  if (mode === 'none') return []
  const pos = positions(sites, hub)
  const pairs: Array<[string, string]> =
    routes.length > 0
      ? routes.map((route) => [route.from, route.to])
      : hub
        ? sites.filter((site) => site.name !== hub.name).map((site) => [hub.name, site.name])
        : []
  const out: ResolvedLink[] = []
  for (const [fromName, toName] of pairs) {
    const from = pos.get(fromName)
    const to = pos.get(toName)
    if (!from || !to) continue
    const touchesSelected = selected !== null && (fromName === selected || toName === selected)
    if (mode === 'selected' && !touchesSelected) continue
    out.push({ key: `${fromName}>${toName}`, from, to, muted: mode === 'all' && selected !== null && !touchesSelected })
  }
  return out
}

/**
 * Points along a quadratic curve from `from` to `to`, bowing to the left of
 * the direction of travel by a fifth of the chord length. The endpoints are
 * returned exactly.
 */
export function curvePoints(from: LatLng, to: LatLng, segments = 24): LatLng[] {
  const [lat1, lng1] = from
  const [lat2, lng2] = to
  const midLat = (lat1 + lat2) / 2
  const midLng = (lng1 + lng2) / 2
  const dLat = lat2 - lat1
  const dLng = lng2 - lng1
  const ctrlLat = midLat - dLng * 0.2
  const ctrlLng = midLng + dLat * 0.2
  const points: LatLng[] = []
  for (let i = 0; i <= segments; i += 1) {
    const t = i / segments
    const a = (1 - t) * (1 - t)
    const b = 2 * (1 - t) * t
    const c = t * t
    points.push([a * lat1 + b * ctrlLat + c * lat2, a * lng1 + b * ctrlLng + c * lng2])
  }
  points[0] = from
  points[segments] = to
  return points
}
