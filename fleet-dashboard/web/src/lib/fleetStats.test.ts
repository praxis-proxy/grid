import { describe, expect, it } from 'vitest'
import { makeSeries, makeSite } from '../test/fixtures'
import {
  fleetP50,
  fleetQueueDepth,
  formatPctDelta,
  formatPointsDelta,
  placedCounts,
  seriesDelta,
  tenantLeader,
  topQueue,
  totalRunning,
} from './fleetStats'

const ohio = makeSite({ name: 'ohio', gpus: { total: 64, utilPct: 67 }, p50LatencyMs: 800, queueDepth: 12, tenants: [{ name: 'research', sharePct: 60 }, { name: 'platform', sharePct: 40 }] })
const london = makeSite({ name: 'london', gpus: { total: 16, utilPct: 91 }, p50LatencyMs: 1200, queueDepth: 60, models: [{ name: 'mistral-7b', running: 2 }], tenants: [{ name: 'platform', sharePct: 100 }] })
const lima = makeSite({ name: 'lima', health: 'red', placed: false, gpus: { total: 4, utilPct: null }, p50LatencyMs: null, queueDepth: null, models: [], tenants: [] })
const sites = [ohio, london, lima]

describe('seriesDelta', () => {
  it('compares the first and last known points, skipping nulls', () => {
    expect(seriesDelta(makeSeries().points, 'gpuUtil')).toEqual({ first: 55, last: 61, delta: 6, pct: (100 * 6) / 55 })
  })
  it('is unknown with fewer than two known points or a zero baseline', () => {
    expect(seriesDelta([], 'gpuUtil')).toEqual({ first: null, last: null, delta: null, pct: null })
    expect(seriesDelta([{ t: 'a', gpuUtil: 5, queueDepth: null, tokensPerSec: null }], 'gpuUtil')).toEqual({ first: 5, last: 5, delta: null, pct: null })
    const fromZero = [
      { t: 'a', gpuUtil: null, queueDepth: null, tokensPerSec: 0 },
      { t: 'b', gpuUtil: null, queueDepth: null, tokensPerSec: 10 },
    ]
    expect(seriesDelta(fromZero, 'tokensPerSec')).toEqual({ first: 0, last: 10, delta: 10, pct: null })
  })
})

describe('delta formatting', () => {
  it('signs points and percents and rounds', () => {
    expect(formatPointsDelta(3.4)).toBe('+3 pts')
    expect(formatPointsDelta(-11.6)).toBe('-12 pts')
    expect(formatPointsDelta(0)).toBe('0 pts')
    expect(formatPointsDelta(null)).toBe('--')
    expect(formatPctDelta(12.3)).toBe('+12%')
    expect(formatPctDelta(-0.4)).toBe('0%')
    expect(formatPctDelta(null)).toBe('--')
  })
})

describe('fleet aggregates', () => {
  it('weights p50 by GPUs and skips sites without one', () => {
    expect(fleetP50(sites)).toBe((800 * 64 + 1200 * 16) / 80)
    expect(fleetP50([lima])).toBeNull()
  })
  it('sums queue depth and finds the top contributor', () => {
    expect(fleetQueueDepth(sites)).toBe(72)
    expect(fleetQueueDepth([lima])).toBeNull()
    expect(topQueue(sites)?.name).toBe('london')
    expect(topQueue([lima])).toBeNull()
  })
  it('counts running models', () => {
    expect(totalRunning(sites)).toBe(8)
  })
  it('names the tenant with the largest GPU-weighted share', () => {
    expect(tenantLeader(sites)).toEqual({ name: 'platform', gpus: 0.4 * 64 + 16 })
    expect(tenantLeader([lima])).toBeNull()
  })
  it('counts placed and unplaced sites', () => {
    expect(placedCounts(sites)).toEqual({ placed: 2, unplaced: 1 })
  })
})
