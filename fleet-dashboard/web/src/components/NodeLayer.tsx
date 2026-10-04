import { useEffect, useMemo, useRef } from 'react'
import L from 'leaflet'
import 'leaflet.markercluster'
import type { Health, Hub, Site } from '../api/types'
import { REDUCED_MOTION_QUERY, useMediaQuery } from '../hooks/useMediaQuery'
import type { SelectionSource } from '../hooks/useSelectedSite'
import { clusterIcon, clusterRadius, hubGlyph, markerLabel, nodeGlyph, worstHealth, type Glyph } from '../lib/glyph'
import { healthWord } from '../lib/health'
import { shouldLabel, type LabelMode } from '../lib/mapModes'

export interface NodeLayerProps {
  map: L.Map | null
  sites: Site[]
  hub: Hub | null
  selected: string | null
  onSelect: (name: string, source: SelectionSource) => void
  labels: LabelMode
}

const SELECTED_Z_OFFSET = 1000

function glyphIcon(glyph: Glyph): L.DivIcon {
  return L.divIcon({
    html: glyph.html,
    className: glyph.className,
    iconSize: [glyph.size, glyph.size],
    iconAnchor: [glyph.size / 2, glyph.size / 2],
  })
}

/**
 * Identifies a glyph's drawn appearance so setIcon is only called when it
 * actually changes. Deliberately excludes the label lines (glyph.html
 * includes them) so a "last seen Ns" tick, which changes the label every
 * poll, does not rewrite the SVG and restart the halo animation.
 */
function glyphSignature(glyph: Glyph): string {
  return `${glyph.className} ${glyph.svg}`
}

/** Joined signature of a glyph's label lines, or '' when it has none. */
function labelSignature(glyph: Glyph): string {
  return glyph.label ? glyph.label.join('\0') : ''
}

/**
 * Updates the two label spans inside a marker's already-rendered element in
 * place, without touching the rest of the icon -- used when only the label
 * text changed (see glyphSignature) so the halo/appearance is left alone.
 */
function updateMarkerLabel(marker: L.Marker, label: [string, string] | undefined): void {
  if (!label) return
  const el = marker.getElement()
  if (!el) return
  const spans = el.querySelectorAll('.fleet-node__label span')
  if (spans[0]) spans[0].textContent = label[0]
  if (spans[1]) spans[1].textContent = label[1]
}

/**
 * Leaflet gives a keyboard marker tabindex and role=button; the accessible
 * name is ours. Re-applied after every setIcon because a DivIcon replaces the
 * element the first time it takes over from the default image icon, and on
 * every 'add' because a marker leaving a cluster gets a fresh element.
 */
function applyMarkerA11y(marker: L.Marker, label: string): void {
  const el = marker.getElement()
  if (!el) return
  el.setAttribute('role', 'button')
  el.setAttribute('tabindex', '0')
  el.setAttribute('aria-label', label)
}

/**
 * leaflet.markercluster overrides `eachLayer` to walk every individual child
 * marker (even the ones currently bundled inside a cluster icon), not the
 * cluster icons the plugin actually draws on the map in their place -- so
 * the public API cannot find cluster elements. `_featureGroup` is the
 * plugin's own internal FeatureGroup mirroring what is actually rendered
 * (loose markers plus cluster icons); it is the only place cluster layers
 * are reachable, hence the private-field reach-in is isolated to this one
 * helper.
 */
function eachRenderedCluster(group: L.MarkerClusterGroup, fn: (cluster: L.MarkerCluster) => void): void {
  const featureGroup = (group as unknown as { _featureGroup: L.LayerGroup })._featureGroup
  featureGroup.eachLayer((layer) => {
    const maybeCluster = layer as unknown as Partial<L.MarkerCluster>
    if (typeof maybeCluster.getChildCount === 'function') fn(layer as unknown as L.MarkerCluster)
  })
}

/**
 * Cluster icons have no accessible name of their own (their SVG is
 * aria-hidden): give every cluster currently rendered on the map a button
 * role, tabindex and an aria-label naming the member count and worst health.
 */
function applyClusterA11y(group: L.MarkerClusterGroup, healthByMarker: WeakMap<L.Marker, Health>): void {
  eachRenderedCluster(group, (cluster) => {
    const el = cluster.getElement()
    if (!el) return
    const count = cluster.getChildCount()
    const worst = worstHealth(cluster.getAllChildMarkers().map((m) => healthByMarker.get(m) ?? 'green'))
    el.setAttribute('role', 'button')
    el.setAttribute('tabindex', '0')
    el.setAttribute('aria-label', `${count} sites, worst health ${healthWord(worst)}`)
  })
}

/**
 * Keeps one L.Marker per placed site inside a marker cluster group (plus the
 * hub, directly on the map) in sync with props. Renders no DOM itself.
 */
export default function NodeLayer({ map, sites, hub, selected, onSelect, labels }: NodeLayerProps) {
  const reducedMotion = useMediaQuery(REDUCED_MOTION_QUERY)
  const groupRef = useRef<L.MarkerClusterGroup | null>(null)
  const markersRef = useRef(new Map<string, L.Marker>())
  // Last-applied glyph signature and lat/lng per site name, so unchanged
  // snapshots skip setIcon/setLatLng instead of rewriting the SVG (which
  // restarts the halo animation) and re-setting the position every poll.
  const glyphSigRef = useRef(new Map<string, string>())
  const labelSigRef = useRef(new Map<string, string>())
  const positionSigRef = useRef(new Map<string, string>())
  const a11yRef = useRef(new Map<string, string>())
  // Read by the cluster icon function, which only sees markers.
  const healthByMarkerRef = useRef(new WeakMap<L.Marker, Health>())
  const hubMarkerRef = useRef<L.Marker | null>(null)
  const hubGlyphSigRef = useRef<string | null>(null)
  const hubPositionSigRef = useRef<string | null>(null)
  const onSelectRef = useRef(onSelect)

  useEffect(() => {
    onSelectRef.current = onSelect
  }, [onSelect])

  // markercluster reads maxClusterRadius once, when it builds its grids at
  // addTo -- a function that reads a ref filled in later never sees an
  // updated fleet. Compute the number up front instead, and rebuild the
  // group (below) when it changes.
  const radius = useMemo(() => clusterRadius(sites.map((s) => s.gpus.total)), [sites])

  // One cluster group per map, rebuilt whenever the cluster radius changes;
  // existing markers are re-added to the fresh group below.
  useEffect(() => {
    if (!map) return
    const healthByMarker = healthByMarkerRef.current
    const group = L.markerClusterGroup({
      maxClusterRadius: radius,
      iconCreateFunction: (cluster) => glyphIcon(clusterIcon(cluster.getAllChildMarkers().map((m) => healthByMarker.get(m) ?? 'green'))),
      showCoverageOnHover: false,
      zoomToBoundsOnClick: true,
      spiderfyOnMaxZoom: true,
      animate: !reducedMotion,
    })
    // Clustering/spiderfying animations create and destroy cluster icon
    // elements outside of our own addLayer/removeLayer calls, so this is the
    // only reliable point to re-label whatever cluster icons are on screen
    // once an animation settles.
    group.on('animationend', () => applyClusterA11y(group, healthByMarkerRef.current))
    group.addTo(map)
    groupRef.current = group
    for (const marker of markersRef.current.values()) group.addLayer(marker)
    return () => {
      group.remove()
      groupRef.current = null
    }
  }, [map, reducedMotion, radius])

  useEffect(() => {
    const group = groupRef.current
    if (!map || !group) return
    const markers = markersRef.current
    const glyphSigs = glyphSigRef.current
    const labelSigs = labelSigRef.current
    const positionSigs = positionSigRef.current
    const a11y = a11yRef.current
    const healthByMarker = healthByMarkerRef.current
    const now = Date.now()
    const seen = new Set<string>()
    let healthChanged = false
    for (const site of sites) {
      if (site.lat === null || site.lng === null) continue
      seen.add(site.name)
      const isSelected = site.name === selected
      const glyph = nodeGlyph(site, { selected: isSelected, labelled: shouldLabel(site.health, isSelected, labels), now })
      const glyphSig = glyphSignature(glyph)
      const labelSig = labelSignature(glyph)
      const position: L.LatLngTuple = [site.lat, site.lng]
      const positionSig = `${site.lat},${site.lng}`
      const label = markerLabel(site)
      let marker = markers.get(site.name)
      if (!marker) {
        const name = site.name
        const created = L.marker(position, { keyboard: true, icon: glyphIcon(glyph) })
        created.on('click', () => onSelectRef.current(name, 'map'))
        created.on('add', () => applyMarkerA11y(created, a11y.get(name) ?? ''))
        healthByMarker.set(created, site.health)
        group.addLayer(created)
        markers.set(name, created)
        positionSigs.set(name, positionSig)
        glyphSigs.set(name, glyphSig)
        labelSigs.set(name, labelSig)
        marker = created
      } else if (positionSigs.get(site.name) !== positionSig) {
        // The cluster index does not follow setLatLng; re-add so the marker lands in the right cluster.
        group.removeLayer(marker)
        marker.setLatLng(position)
        group.addLayer(marker)
        positionSigs.set(site.name, positionSig)
      }
      if (healthByMarker.get(marker) !== site.health) {
        healthByMarker.set(marker, site.health)
        healthChanged = true
      }
      const labelChanged = a11y.get(site.name) !== label
      a11y.set(site.name, label)
      if (glyphSigs.get(site.name) !== glyphSig) {
        marker.setIcon(glyphIcon(glyph))
        glyphSigs.set(site.name, glyphSig)
        labelSigs.set(site.name, labelSig)
        applyMarkerA11y(marker, label)
      } else {
        // The glyph's drawn appearance is unchanged; a "last seen Ns" tick
        // still needs its label text updated, but in place -- setIcon would
        // rewrite the SVG and restart the down-site halo animation.
        if (labelSigs.get(site.name) !== labelSig) {
          updateMarkerLabel(marker, glyph.label)
          labelSigs.set(site.name, labelSig)
        }
        if (labelChanged) applyMarkerA11y(marker, label)
      }
      marker.setZIndexOffset(isSelected ? SELECTED_Z_OFFSET : 0)
    }
    for (const [name, marker] of markers) {
      if (!seen.has(name)) {
        group.removeLayer(marker)
        markers.delete(name)
        glyphSigs.delete(name)
        labelSigs.delete(name)
        positionSigs.delete(name)
        a11y.delete(name)
      }
    }
    if (healthChanged) group.refreshClusters()
    // Cover clusters formed or changed by this pass's addLayer/removeLayer/refreshClusters calls.
    applyClusterA11y(group, healthByMarker)
  }, [map, sites, selected, labels])

  useEffect(() => {
    if (!map) return
    if (!hub || hub.lat === null || hub.lng === null) {
      hubMarkerRef.current?.remove()
      hubMarkerRef.current = null
      hubGlyphSigRef.current = null
      hubPositionSigRef.current = null
      return
    }
    const position: L.LatLngTuple = [hub.lat, hub.lng]
    const positionSig = `${hub.lat},${hub.lng}`
    const glyph = hubGlyph(hub.name)
    const glyphSig = glyphSignature(glyph)
    if (!hubMarkerRef.current) {
      hubMarkerRef.current = L.marker(position, { title: hub.name, interactive: false, icon: glyphIcon(glyph) })
      hubMarkerRef.current.addTo(map)
      hubPositionSigRef.current = positionSig
      hubGlyphSigRef.current = glyphSig
    } else if (hubPositionSigRef.current !== positionSig) {
      hubMarkerRef.current.setLatLng(position)
      hubPositionSigRef.current = positionSig
    }
    if (hubGlyphSigRef.current !== glyphSig) {
      hubMarkerRef.current.setIcon(glyphIcon(glyph))
      hubGlyphSigRef.current = glyphSig
    }
  }, [map, hub])

  useEffect(() => {
    const markers = markersRef.current
    return () => {
      for (const marker of markers.values()) groupRef.current?.removeLayer(marker)
      markers.clear()
      hubMarkerRef.current?.remove()
      hubMarkerRef.current = null
    }
  }, [])

  return null
}
