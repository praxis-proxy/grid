import { describe, expect, it } from 'vitest'
import { makeSeries } from '../test/fixtures'
import { seriesToChartData } from './chartData'

describe('seriesToChartData', () => {
  it('converts timestamps to epoch ms and keeps nulls as gaps', () => {
    const rows = seriesToChartData(makeSeries().points)
    expect(rows).toHaveLength(3)
    expect(rows[0]).toEqual({ t: Date.parse('2026-09-06T11:58:00Z'), gpuUtil: 55, tokensPerSec: 5800, queueDepth: 20 })
    expect(rows[1]).toEqual({ t: Date.parse('2026-09-06T11:58:30Z'), gpuUtil: null, tokensPerSec: null, queueDepth: null })
  })
  it('drops points with unparseable timestamps', () => {
    expect(seriesToChartData([{ t: 'garbage', gpuUtil: 1, queueDepth: 1, tokensPerSec: 1 }])).toEqual([])
  })
})
