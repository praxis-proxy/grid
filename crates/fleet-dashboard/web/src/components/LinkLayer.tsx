import { useEffect, useRef } from 'react'
import L from 'leaflet'
import type { Hub, Route, Site } from '../api/types'
import { HUB_COLOR } from '../lib/health'
import { curvePoints, LINK_OPACITY, LINK_OPACITY_MUTED, resolveLinks } from '../lib/links'
import type { LinkMode } from '../lib/mapModes'

export interface LinkLayerProps {
  map: L.Map | null
  sites: Site[]
  hub: Hub | null
  /** Registered routes from the snapshot; when non-empty they replace the hub-to-site assumption. */
  routes: Route[]
  mode: LinkMode
  selected: string | null
}

const LINK_STYLE: L.PolylineOptions = {
  color: HUB_COLOR,
  weight: 1.5,
  opacity: LINK_OPACITY,
  dashArray: '2 6',
  interactive: false,
}

/**
 * Draws dashed, curved registered-route lines. Vector overlays live in Leaflet's
 * overlayPane (below the markerPane), so the lines sit beneath the glyphs
 * without any explicit z-index handling. Renders no DOM itself.
 */
export default function LinkLayer({ map, sites, hub, routes, mode, selected }: LinkLayerProps) {
  const linesRef = useRef(new Map<string, L.Polyline>())
  // Last-applied endpoints and opacity per link key, so unchanged snapshots
  // skip setLatLngs/setStyle instead of redrawing every poll.
  const positionSigRef = useRef(new Map<string, string>())
  const mutedRef = useRef(new Map<string, boolean>())

  useEffect(() => {
    if (!map) return
    const lines = linesRef.current
    const positionSigs = positionSigRef.current
    const muted = mutedRef.current
    const seen = new Set<string>()
    for (const link of resolveLinks(sites, hub, routes, mode, selected)) {
      seen.add(link.key)
      const positionSig = `${link.from[0]},${link.from[1]};${link.to[0]},${link.to[1]}`
      const opacity = link.muted ? LINK_OPACITY_MUTED : LINK_OPACITY
      let line = lines.get(link.key)
      if (!line) {
        line = L.polyline(curvePoints(link.from, link.to), { ...LINK_STYLE, opacity })
        line.addTo(map)
        lines.set(link.key, line)
        positionSigs.set(link.key, positionSig)
        muted.set(link.key, link.muted)
        continue
      }
      if (positionSigs.get(link.key) !== positionSig) {
        line.setLatLngs(curvePoints(link.from, link.to))
        positionSigs.set(link.key, positionSig)
      }
      if (muted.get(link.key) !== link.muted) {
        line.setStyle({ opacity })
        muted.set(link.key, link.muted)
      }
    }
    for (const [key, line] of lines) {
      if (!seen.has(key)) {
        line.remove()
        lines.delete(key)
        positionSigs.delete(key)
        muted.delete(key)
      }
    }
  }, [map, sites, hub, routes, mode, selected])

  useEffect(() => {
    const lines = linesRef.current
    return () => {
      for (const line of lines.values()) line.remove()
      lines.clear()
    }
  }, [])

  return null
}
