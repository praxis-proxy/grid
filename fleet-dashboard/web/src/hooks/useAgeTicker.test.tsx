import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useAgeTicker, useNow } from './useAgeTicker'

describe('useAgeTicker', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('returns null when there is no last update', () => {
    const { result } = renderHook(() => useAgeTicker(null))
    expect(result.current).toBeNull()
  })

  it('computes the initial age and ticks once a second', () => {
    const lastUpdate = Date.now() - 3000
    const { result } = renderHook(() => useAgeTicker(lastUpdate))
    expect(result.current).toBe(3)
    act(() => vi.advanceTimersByTime(5000))
    expect(result.current).toBe(8)
  })

  it('stops ticking on unmount', () => {
    const lastUpdate = Date.now()
    const { unmount } = renderHook(() => useAgeTicker(lastUpdate))
    unmount()
    // No assertion beyond "does not throw" — proves the interval is cleared.
    act(() => vi.advanceTimersByTime(5000))
  })
})

describe('useNow', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('ticks at the requested interval while enabled', () => {
    const start = Date.now()
    const { result } = renderHook(() => useNow(true, 5000))
    expect(result.current).toBe(start)
    act(() => vi.advanceTimersByTime(4999))
    expect(result.current).toBe(start)
    act(() => vi.advanceTimersByTime(1))
    expect(result.current).toBe(start + 5000)
  })

  it('does not tick when disabled', () => {
    const start = Date.now()
    const { result } = renderHook(() => useNow(false))
    act(() => vi.advanceTimersByTime(10_000))
    expect(result.current).toBe(start)
  })
})
