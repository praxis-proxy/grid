import { describe, expect, it } from 'vitest'
import {
  HEALTH_COLORS,
  HEALTH_ORDER,
  HUB_COLOR,
  healthColor,
  healthLabel,
  healthShape,
  healthShapeGeometry,
  healthShapeMarkup,
  healthWord,
} from './health'

describe('health colors', () => {
  it('maps each health to its exact color', () => {
    expect(healthColor('green')).toBe('#38b26a')
    expect(healthColor('yellow')).toBe('#e0a21c')
    expect(healthColor('red')).toBe('#de4b3f')
    expect(HEALTH_COLORS).toEqual({ green: '#38b26a', yellow: '#e0a21c', red: '#de4b3f' })
  })
  it('uses the accent for the hub', () => {
    expect(HUB_COLOR).toBe('#2fc4d1')
  })
  it('labels health for screen readers and running text', () => {
    expect(healthLabel('green')).toBe('Healthy')
    expect(healthLabel('yellow')).toBe('Degraded')
    expect(healthLabel('red')).toBe('Down')
    expect(healthWord('yellow')).toBe('degraded')
  })
  it('orders worst first', () => {
    expect(HEALTH_ORDER).toEqual(['red', 'yellow', 'green'])
  })
})

describe('health shapes', () => {
  it('pairs every health with a distinct shape', () => {
    expect(healthShape('green')).toBe('circle')
    expect(healthShape('yellow')).toBe('triangle')
    expect(healthShape('red')).toBe('square')
  })
  it('computes geometry inside the box, honoring the offset', () => {
    expect(healthShapeGeometry('green', 10)).toEqual({ kind: 'circle', cx: 5, cy: 5, r: 5 })
    expect(healthShapeGeometry('yellow', 10)).toEqual({ kind: 'polygon', points: '5,0 10,10 0,10' })
    expect(healthShapeGeometry('red', 8, 2, 4)).toEqual({ kind: 'polygon', points: '2,4 10,4 10,12 2,12' })
  })
  it('renders markup with the health fill', () => {
    expect(healthShapeMarkup('green', 10)).toBe('<circle cx="5" cy="5" r="5" fill="#38b26a"/>')
    expect(healthShapeMarkup('red', 6, 1, 1)).toBe('<polygon points="1,1 7,1 7,7 1,7" fill="#de4b3f"/>')
  })
})
