import { describe, expect, it } from 'vitest'
import { makeSeries } from '../test/fixtures'
import { crossesThreshold, seriesMinMax } from './thresholds'

describe('crossesThreshold', () => {
  it('warns at or above the threshold and never for unknown values', () => {
    expect(crossesThreshold(90, 90)).toBe(true)
    expect(crossesThreshold(95.5, 90)).toBe(true)
    expect(crossesThreshold(89.9, 90)).toBe(false)
    expect(crossesThreshold(null, 90)).toBe(false)
    expect(crossesThreshold(undefined, 90)).toBe(false)
    expect(crossesThreshold(Number.NaN, 90)).toBe(false)
  })
})

describe('seriesMinMax', () => {
  it('ignores nulls and reports the extremes', () => {
    expect(seriesMinMax(makeSeries().points, 'gpuUtil')).toEqual({ min: 55, max: 61 })
    expect(seriesMinMax(makeSeries().points, 'tokensPerSec')).toEqual({ min: 5800, max: 6100 })
  })
  it('is null for an all-null or empty series', () => {
    expect(seriesMinMax([], 'gpuUtil')).toBeNull()
    expect(seriesMinMax([{ t: '2026-09-06T12:00:00Z', gpuUtil: null, queueDepth: null, tokensPerSec: null }], 'queueDepth')).toBeNull()
  })
})
