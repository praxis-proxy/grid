import { describe, expect, it } from 'vitest'
import { formatSparkValue } from './sparkline'

describe('formatSparkValue', () => {
  it('formats utilization as a percentage and the rest compactly', () => {
    expect(formatSparkValue('gpuUtil', 67.4)).toBe('67%')
    expect(formatSparkValue('tokensPerSec', 3140)).toBe('3.1k')
    expect(formatSparkValue('queueDepth', null)).toBe('--')
  })
})
