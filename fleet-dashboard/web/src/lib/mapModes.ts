import type { Health } from '../api/types'

/** Which hub-to-site links the map draws (spec section 3.3, "Map tools"). */
export type LinkMode = 'selected' | 'all' | 'none'

/** Which node labels are shown: unhealthy plus selected (default), or every node. */
export type LabelMode = 'unhealthy' | 'all'

export const LINK_MODES: ReadonlyArray<{ value: LinkMode; label: string }> = [
  { value: 'selected', label: 'selected' },
  { value: 'all', label: 'all' },
  { value: 'none', label: 'none' },
]

export const LABEL_MODES: ReadonlyArray<{ value: LabelMode; label: string }> = [
  { value: 'unhealthy', label: 'unhealthy' },
  { value: 'all', label: 'all' },
]

export function shouldLabel(health: Health, selected: boolean, mode: LabelMode): boolean {
  return mode === 'all' || selected || health !== 'green'
}
