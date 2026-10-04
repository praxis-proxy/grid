import type { Health, Site } from '../api/types'
import { ageSeconds, formatAgeShort, formatPct } from './format'
import { GAUGE_TRACK_COLOR, HUB_COLOR, healthColor, healthShapeMarkup, healthWord } from './health'

export const RING_MIN = 12
export const RING_MAX = 34
export const RING_STROKE = 3
export const GAUGE_RATIO = 0.62
export const GAUGE_STROKE = 3
/** GPU count is drawn inside the ring from this radius up; smaller rings rely on the label. */
export const COUNT_MIN_RADIUS = 24
export const SELECTED_GAP = 7
export const ACCENT = '#2fc4d1'
const INK = '#e6ecf0'
const FILL = '#0b0f12'
const FILL_OPACITY = '0.78'
/** Room around the ring for the selected ring (7 px gap plus its 2 px stroke). */
const GLYPH_PAD = 10
/** Degraded mark size as a fraction of the ring radius, matching the cluster icon's proportion (8 / 22). */
const DEGRADED_MARK_RATIO = 8 / 22
/** Gap from the top of the ring to the degraded mark, when it sits above the count. */
const DEGRADED_MARK_TOP_PAD = 3
const HUB_RADIUS = 10
const HUB_PAD = 4

export interface Glyph {
  /** Width and height in CSS px of the icon box; the label overflows below it. */
  size: number
  html: string
  className: string
  /**
   * The svg markup alone, excluding the label lines below it. A "last seen
   * Ns" tick changes `label`/`html` every poll but not the glyph's drawn
   * appearance; callers that only want to know when the mark itself changed
   * (NodeLayer's setIcon signature) should compare this instead of `html`.
   */
  svg: string
  /** The two label lines rendered under the glyph, when it has one. */
  label?: [string, string]
}

export type GlyphSite = Pick<Site, 'name' | 'displayName' | 'health' | 'gpus' | 'lastSeen'>

export interface GlyphOptions {
  selected: boolean
  /** Whether the label under the glyph is visible without hover (see lib/mapModes shouldLabel). */
  labelled: boolean
  /** Epoch ms used for the "last seen" age of down sites. */
  now: number
}

/** Outer ring radius: clamp(12 + 5 * sqrt(gpus), 12, 34). */
export function ringRadius(gpuTotal: number): number {
  const gpus = Number.isFinite(gpuTotal) && gpuTotal > 0 ? gpuTotal : 0
  return Math.min(RING_MAX, Math.max(RING_MIN, 12 + 5 * Math.sqrt(gpus)))
}

export function gaugeRadius(ringR: number): number {
  return Math.round(ringR * GAUGE_RATIO)
}

/**
 * SVG path for an arc starting at 12 o'clock and sweeping clockwise through
 * `fraction` of the circle. Empty for fraction <= 0; a full circle at >= 1
 * (drawn as two half arcs, since a single arc cannot close on itself).
 */
export function arcPath(cx: number, cy: number, r: number, fraction: number): string {
  if (!Number.isFinite(fraction) || fraction <= 0) return ''
  if (fraction >= 1) {
    return `M ${cx} ${cy - r} A ${r} ${r} 0 1 1 ${cx} ${cy + r} A ${r} ${r} 0 1 1 ${cx} ${cy - r}`
  }
  const angle = -Math.PI / 2 + 2 * Math.PI * fraction
  const endX = (cx + r * Math.cos(angle)).toFixed(2)
  const endY = (cy + r * Math.sin(angle)).toFixed(2)
  const large = fraction > 0.5 ? 1 : 0
  return `M ${cx} ${cy - r} A ${r} ${r} 0 ${large} 1 ${endX} ${endY}`
}

/** Flat-topped hexagon points centred on (cx, cy). */
export function hexagonPoints(cx: number, cy: number, r: number): string {
  return Array.from({ length: 6 }, (_, i) => {
    const angle = (Math.PI / 180) * (60 * i - 30)
    return `${(cx + r * Math.cos(angle)).toFixed(1)},${(cy + r * Math.sin(angle)).toFixed(1)}`
  }).join(' ')
}

export function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

/** The two label lines under a glyph: name, then "N GPU · U%" or, when down, "unreachable · last seen 45s". */
export function glyphLabel(site: GlyphSite, now: number): [string, string] {
  const name = site.displayName || site.name
  if (site.health === 'red') {
    const age = ageSeconds(site.lastSeen, now)
    return [name, age === null ? 'unreachable · never seen' : `unreachable · last seen ${formatAgeShort(age)}`]
  }
  return [name, `${site.gpus.total} GPU · ${formatPct(site.gpus.utilPct)}`]
}

function labelMarkup(lines: [string, string]): string {
  return `<div class="fleet-node__label"><span>${escapeHtml(lines[0])}</span><span>${escapeHtml(lines[1])}</span></div>`
}

/** Screen-reader name for a marker: "Frankfurt, degraded, 12 GPUs, 95 percent utilization, GPU utilization 95% >= 90%". */
export function markerLabel(site: Pick<Site, 'name' | 'displayName' | 'health' | 'gpus' | 'reasons'>): string {
  const util =
    site.gpus.utilPct === null || !Number.isFinite(site.gpus.utilPct)
      ? 'utilization unknown'
      : `${Math.round(site.gpus.utilPct)} percent utilization`
  const parts = [site.displayName || site.name, healthWord(site.health), `${site.gpus.total} GPUs`, util, ...site.reasons]
  return parts.join(', ')
}

export function nodeGlyph(site: GlyphSite, opts: GlyphOptions): Glyph {
  const r = ringRadius(site.gpus.total)
  const size = 2 * (r + GLYPH_PAD)
  const c = size / 2
  const color = healthColor(site.health)
  const down = site.health === 'red'
  const parts: string[] = []
  if (down) {
    parts.push(`<circle class="fleet-node__halo" cx="${c}" cy="${c}" r="${r}" fill="none" stroke="${color}" stroke-width="2"/>`)
  }
  if (opts.selected) {
    parts.push(`<circle cx="${c}" cy="${c}" r="${r + SELECTED_GAP}" fill="none" stroke="${ACCENT}" stroke-width="2"/>`)
  }
  parts.push(
    `<circle cx="${c}" cy="${c}" r="${r}" fill="${FILL}" fill-opacity="${FILL_OPACITY}" stroke="${color}" stroke-width="${RING_STROKE}"/>`,
  )
  if (down) {
    const mark = Math.round(r * 0.55)
    parts.push(healthShapeMarkup('red', mark, c - mark / 2, c - mark / 2))
  } else {
    const g = gaugeRadius(r)
    parts.push(`<circle cx="${c}" cy="${c}" r="${g}" fill="none" stroke="${GAUGE_TRACK_COLOR}" stroke-width="${GAUGE_STROKE}"/>`)
    const util = site.gpus.utilPct
    const fraction = util === null || !Number.isFinite(util) ? 0 : Math.min(100, Math.max(0, util)) / 100
    const arc = arcPath(c, c, g, fraction)
    if (arc) {
      parts.push(`<path d="${arc}" fill="none" stroke="${color}" stroke-width="${GAUGE_STROKE}" stroke-linecap="round"/>`)
    }
    if (site.health === 'yellow') {
      // Health always carries a shape as well as a color (spec section 2); draw the
      // triangle mark used by the cluster icon so it coexists with the count text
      // below -- at the top of the ring normally, centered when there is no count.
      const mark = Math.round(r * DEGRADED_MARK_RATIO)
      const markX = c - mark / 2
      const markY = r >= COUNT_MIN_RADIUS ? c - r + DEGRADED_MARK_TOP_PAD : c - mark / 2
      parts.push(healthShapeMarkup('yellow', mark, markX, markY))
    }
    if (r >= COUNT_MIN_RADIUS) {
      parts.push(
        `<text x="${c}" y="${c}" text-anchor="middle" dominant-baseline="central" font-size="11" font-weight="600" fill="${INK}">${site.gpus.total}</text>`,
      )
    }
  }
  const svg = `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" aria-hidden="true">${parts.join('')}</svg>`
  const className = [
    'fleet-node',
    `fleet-node--${site.health}`,
    opts.selected ? 'fleet-node--selected' : '',
    opts.labelled ? 'fleet-node--labelled' : '',
  ]
    .filter(Boolean)
    .join(' ')
  const label = glyphLabel(site, opts.now)
  return { size, html: svg + labelMarkup(label), className, svg, label }
}

export function hubGlyph(name: string): Glyph {
  const size = 2 * (HUB_RADIUS + HUB_PAD)
  const c = size / 2
  const svg =
    `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" aria-hidden="true">` +
    `<polygon points="${hexagonPoints(c, c, HUB_RADIUS)}" fill="${HUB_COLOR}" fill-opacity="0.35" stroke="${HUB_COLOR}" stroke-width="2"/>` +
    `</svg>`
  const label: [string, string] = [name, 'hub']
  return { size, html: svg + labelMarkup(label), className: 'fleet-node fleet-node--hub fleet-node--labelled', svg, label }
}

export const CLUSTER_RADIUS = 22
const CLUSTER_PAD = 4
const CLUSTER_FILL = '#12181d'
/** Worst-health shape mark drawn above the count; omitted entirely when the worst health is green. */
const CLUSTER_MARK_SIZE = 8
const CLUSTER_MARK_Y = 5
const HEALTH_RANK: Record<Health, number> = { red: 0, yellow: 1, green: 2 }

export function worstHealth(healths: readonly Health[]): Health {
  let worst: Health = 'green'
  for (const health of healths) {
    if (HEALTH_RANK[health] < HEALTH_RANK[worst]) worst = health
  }
  return worst
}

/**
 * Distance in px under which two glyphs are merged into a cluster: the sum of
 * the two largest possible ring radii for this fleet, so rings never overlap.
 */
export function clusterRadius(gpuTotals: readonly number[]): number {
  const largest = gpuTotals.reduce((max, gpus) => Math.max(max, ringRadius(gpus)), RING_MIN)
  return 2 * largest
}

/**
 * Cluster glyph: 22 px ring in the worst member health, member count in the
 * middle, "sites" below. Health always carries a shape as well as a color
 * (spec section 2), so the worst health's shape mark is drawn above the
 * count too -- a triangle for degraded, a square for down. Skipped when the
 * worst health is green, since the healthy shape is a plain circle and would
 * just duplicate the ring.
 */
export function clusterIcon(healths: readonly Health[]): Glyph {
  const worst = worstHealth(healths)
  const size = 2 * (CLUSTER_RADIUS + CLUSTER_PAD)
  const c = size / 2
  const mark = worst === 'green' ? '' : healthShapeMarkup(worst, CLUSTER_MARK_SIZE, c - CLUSTER_MARK_SIZE / 2, CLUSTER_MARK_Y)
  const svg =
    `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" aria-hidden="true">` +
    `<circle cx="${c}" cy="${c}" r="${CLUSTER_RADIUS}" fill="${CLUSTER_FILL}" fill-opacity="0.95" stroke="${healthColor(worst)}" stroke-width="${RING_STROKE}"/>` +
    mark +
    `<text x="${c}" y="${c - 3}" text-anchor="middle" dominant-baseline="central" font-size="14" font-weight="600" fill="${INK}">${healths.length}</text>` +
    `<text x="${c}" y="${c + 10}" text-anchor="middle" dominant-baseline="central" font-size="11" fill="#8a99a6">sites</text>` +
    `</svg>`
  return { size, html: svg, className: `fleet-cluster fleet-cluster--${worst}`, svg }
}
