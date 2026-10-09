import type { Health } from '../api/types'
import { healthColor, healthLabel, healthShapeGeometry } from '../lib/health'

export interface HealthShapeProps {
  health: Health
  /** Box size in CSS px; the shape fills it. */
  size?: number
  /** Accessible name; defaults to "Healthy", "Degraded" or "Down". Pass "" to hide from assistive tech. */
  label?: string
  className?: string
}

/** Circle, triangle or square in the health color: the shape carries the meaning when color cannot. */
export default function HealthShape({ health, size = 10, label, className }: HealthShapeProps) {
  const geometry = healthShapeGeometry(health, size)
  const color = healthColor(health)
  const name = label ?? healthLabel(health)
  return (
    <svg
      role={name ? 'img' : undefined}
      aria-label={name || undefined}
      aria-hidden={name ? undefined : true}
      width={size}
      height={size}
      viewBox={`0 0 ${size} ${size}`}
      className={className ? `shrink-0 ${className}` : 'shrink-0'}
    >
      {geometry.kind === 'circle' ? (
        <circle cx={geometry.cx} cy={geometry.cy} r={geometry.r} fill={color} />
      ) : (
        <polygon points={geometry.points} fill={color} />
      )}
    </svg>
  )
}
