import { describe, expect, it } from 'vitest'
import { LABEL_MODES, LINK_MODES, shouldLabel } from './mapModes'

describe('shouldLabel', () => {
  it('labels unhealthy and selected sites by default', () => {
    expect(shouldLabel('green', false, 'unhealthy')).toBe(false)
    expect(shouldLabel('yellow', false, 'unhealthy')).toBe(true)
    expect(shouldLabel('red', false, 'unhealthy')).toBe(true)
    expect(shouldLabel('green', true, 'unhealthy')).toBe(true)
  })
  it('labels everything in all mode', () => {
    expect(shouldLabel('green', false, 'all')).toBe(true)
  })
  it('lists the modes in display order', () => {
    expect(LINK_MODES.map((m) => m.value)).toEqual(['selected', 'all', 'none'])
    expect(LABEL_MODES.map((m) => m.value)).toEqual(['unhealthy', 'all'])
  })
})
