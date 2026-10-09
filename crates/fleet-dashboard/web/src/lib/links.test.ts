import { describe, expect, it } from 'vitest'
import { makeSite } from '../test/fixtures'
import { curvePoints, resolveLinks } from './links'

const hub = { name: 'hub', region: 'us-east-1', lat: 38.95, lng: -77.45 }
const a = makeSite({ name: 'a', lat: 41, lng: -83 })
const b = makeSite({ name: 'b', lat: 51, lng: 0 })
const unplaced = makeSite({ name: 'u', lat: null, lng: null, placed: false })
const sites = [a, b, unplaced]

describe('resolveLinks', () => {
  it('draws nothing in none mode', () => {
    expect(resolveLinks(sites, hub, [], 'none', 'a')).toEqual([])
  })
  it('assumes hub-to-site for every placed site when there are no routes, at full opacity with no selection', () => {
    const links = resolveLinks(sites, hub, [], 'all', null)
    expect(links.map((l) => l.key)).toEqual(['hub>a', 'hub>b'])
    expect(links[0].from).toEqual([38.95, -77.45])
    expect(links[0].to).toEqual([41, -83])
    expect(links.every((l) => l.muted)).toBe(false)
  })
  it('keeps only links touching the selected site in selected mode, at full opacity', () => {
    const links = resolveLinks(sites, hub, [], 'selected', 'b')
    expect(links.map((l) => [l.key, l.muted])).toEqual([['hub>b', false]])
    expect(resolveLinks(sites, hub, [], 'selected', null)).toEqual([])
  })
  it('lifts only the selected link out of the faint set in all mode', () => {
    const links = resolveLinks(sites, hub, [], 'all', 'b')
    expect(links.map((l) => [l.key, l.muted])).toEqual([
      ['hub>a', true],
      ['hub>b', false],
    ])
  })
  it('follows registered routes instead of the hub assumption when present', () => {
    const links = resolveLinks(sites, hub, [{ from: 'a', to: 'b' }, { from: 'b', to: 'u' }, { from: 'x', to: 'a' }], 'all', null)
    expect(links.map((l) => l.key)).toEqual(['a>b'])
    expect(links[0].from).toEqual([41, -83])
  })
  it('draws nothing without a placed hub and no routes', () => {
    expect(resolveLinks(sites, { ...hub, lat: null, lng: null }, [], 'all', null)).toEqual([])
    expect(resolveLinks(sites, null, [], 'all', null)).toEqual([])
  })
})

describe('curvePoints', () => {
  it('starts and ends exactly at the endpoints and bows away from the chord', () => {
    const points = curvePoints([0, 0], [0, 10], 4)
    expect(points).toHaveLength(5)
    expect(points[0]).toEqual([0, 0])
    expect(points[4]).toEqual([0, 10])
    expect(points[2][1]).toBeCloseTo(5)
    expect(points[2][0]).toBeCloseTo(-1)
  })
})
