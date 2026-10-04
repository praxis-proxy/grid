import type { Health } from '../api/types'

export const HEALTH_COLORS: Record<Health, string> = {
  green: '#38b26a',
  yellow: '#e0a21c',
  red: '#de4b3f',
}

export const HUB_COLOR = '#2fc4d1'
export const LAND_COLOR = '#1a2228'
export const COAST_COLOR = '#2c3944'
export const GAUGE_TRACK_COLOR = '#2c3944'

const HEALTH_LABELS: Record<Health, string> = {
  green: 'Healthy',
  yellow: 'Degraded',
  red: 'Down',
}

/** Health always carries a shape as well as a color (spec section 2). */
export type HealthShapeKind = 'circle' | 'triangle' | 'square'

const HEALTH_SHAPES: Record<Health, HealthShapeKind> = {
  green: 'circle',
  yellow: 'triangle',
  red: 'square',
}

export const HEALTH_ORDER: Health[] = ['red', 'yellow', 'green']

export function healthColor(health: Health): string {
  return HEALTH_COLORS[health]
}

/** Capitalised, for badges and accessible names: "Healthy", "Degraded", "Down". */
export function healthLabel(health: Health): string {
  return HEALTH_LABELS[health]
}

/** Lower-case, for running text: "healthy", "degraded", "down". */
export function healthWord(health: Health): string {
  return HEALTH_LABELS[health].toLowerCase()
}

export function healthShape(health: Health): HealthShapeKind {
  return HEALTH_SHAPES[health]
}

export type ShapeGeometry =
  | { kind: 'circle'; cx: number; cy: number; r: number }
  | { kind: 'polygon'; points: string }

/**
 * Geometry of the health shape inside a `size` by `size` box whose top-left
 * corner is at (x, y). Shared by the React component (JSX) and the map glyphs
 * (markup strings) so both draw exactly the same shape.
 */
export function healthShapeGeometry(health: Health, size: number, x = 0, y = 0): ShapeGeometry {
  const half = size / 2
  switch (healthShape(health)) {
    case 'circle':
      return { kind: 'circle', cx: x + half, cy: y + half, r: half }
    case 'triangle':
      return { kind: 'polygon', points: `${x + half},${y} ${x + size},${y + size} ${x},${y + size}` }
    case 'square':
      return { kind: 'polygon', points: `${x},${y} ${x + size},${y} ${x + size},${y + size} ${x},${y + size}` }
  }
}

/** SVG markup for the health shape, for HTML strings handed to L.divIcon. */
export function healthShapeMarkup(health: Health, size: number, x = 0, y = 0): string {
  const g = healthShapeGeometry(health, size, x, y)
  const fill = healthColor(health)
  return g.kind === 'circle'
    ? `<circle cx="${g.cx}" cy="${g.cy}" r="${g.r}" fill="${fill}"/>`
    : `<polygon points="${g.points}" fill="${fill}"/>`
}
