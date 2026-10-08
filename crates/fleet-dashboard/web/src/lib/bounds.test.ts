import { describe, expect, it } from 'vitest'
import { makeSite } from '../test/fixtures'
import { placedPoints, planFit, SINGLE_POINT_ZOOM } from './bounds'

describe('placedPoints', () => {
  it('includes placed sites and the hub, skips unplaced and hubs without coordinates', () => {
    const sites = [
      makeSite({ name: 'a', lat: 10, lng: 20 }),
      makeSite({ name: 'b', lat: null, lng: null, placed: false }),
      makeSite({ name: 'c', lat: 30, lng: 40, placed: false }),
    ]
    expect(placedPoints(sites, { name: 'hub', region: 'x', lat: 1, lng: 2 })).toEqual([
      [10, 20],
      [1, 2],
    ])
    expect(placedPoints(sites, { name: 'hub', region: 'x', lat: null, lng: null })).toEqual([[10, 20]])
    expect(placedPoints(sites, null)).toEqual([[10, 20]])
  })
})

describe('planFit', () => {
  it('returns empty for no points', () => {
    expect(planFit([])).toEqual({ kind: 'empty' })
  })
  it('centers on a single point at zoom 4', () => {
    expect(planFit([[10, 20]])).toEqual({ kind: 'single', center: [10, 20], zoom: SINGLE_POINT_ZOOM })
    expect(SINGLE_POINT_ZOOM).toBe(4)
  })
  it('treats identical points as a single point', () => {
    expect(planFit([[10, 20], [10, 20]])).toEqual({ kind: 'single', center: [10, 20], zoom: 4 })
  })
  it('computes the south-west and north-east corners for several points', () => {
    expect(planFit([[40, -82], [51, 0], [-23, -46]])).toEqual({
      kind: 'bounds',
      bounds: [
        [-23, -82],
        [51, 0],
      ],
    })
  })
})
