import { describe, expect, it } from 'vitest'
import { GRATICULE_COLOR, GRATICULE_STEP_DEG, graticuleLines } from './graticule'

describe('graticuleLines', () => {
  it('draws 24 meridians and 11 parallels at 15 degrees', () => {
    const lines = graticuleLines()
    expect(GRATICULE_STEP_DEG).toBe(15)
    expect(lines).toHaveLength(35)
    expect(lines[0]).toEqual([
      [-85, -180],
      [85, -180],
    ])
    expect(lines[23]).toEqual([
      [-85, 165],
      [85, 165],
    ])
    expect(lines[24]).toEqual([
      [-75, -180],
      [-75, 180],
    ])
    expect(lines[34][0][0]).toBe(75)
  })
  it('honours another step and pins the color', () => {
    expect(graticuleLines(30)).toHaveLength(12 + 5)
    expect(GRATICULE_COLOR).toBe('#141b21')
  })
})
