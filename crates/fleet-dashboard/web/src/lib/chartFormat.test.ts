import { describe, expect, it } from 'vitest'
import { formatChartValue, isSparklineField, timeTicks } from './chartFormat'

describe('formatChartValue', () => {
  it('adds the unit for each series', () => {
    expect(formatChartValue('gpuUtil', 71.4)).toBe('71 %')
    expect(formatChartValue('tokensPerSec', 3140)).toBe('3.1k tok/s')
    expect(formatChartValue('queueDepth', 42)).toBe('42 queued')
    expect(formatChartValue('queueDepth', null)).toBe('--')
  })
  it('recognises the series keys', () => {
    expect(isSparklineField('gpuUtil')).toBe(true)
    expect(isSparklineField('t')).toBe(false)
  })
})

describe('timeTicks', () => {
  it('returns start, middle and end', () => {
    const rows = [
      { t: 1000, gpuUtil: 1, tokensPerSec: 1, queueDepth: 1 },
      { t: 2000, gpuUtil: 1, tokensPerSec: 1, queueDepth: 1 },
      { t: 5000, gpuUtil: 1, tokensPerSec: 1, queueDepth: 1 },
    ]
    expect(timeTicks(rows)).toEqual([1000, 3000, 5000])
    expect(timeTicks([rows[0]])).toEqual([1000])
    expect(timeTicks([])).toEqual([])
  })
})
