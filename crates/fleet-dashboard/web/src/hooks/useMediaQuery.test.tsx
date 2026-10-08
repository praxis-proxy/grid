import { act, renderHook } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { NARROW_SCREEN_QUERY, useMediaQuery } from './useMediaQuery'

describe('useMediaQuery', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('is false when matchMedia is unavailable', () => {
    const { result } = renderHook(() => useMediaQuery(NARROW_SCREEN_QUERY))
    expect(result.current).toBe(false)
  })

  it('tracks the query and its change events', () => {
    let listener: (() => void) | null = null
    const list = {
      matches: true,
      addEventListener: vi.fn((_: string, cb: () => void) => {
        listener = cb
      }),
      removeEventListener: vi.fn(),
    }
    vi.stubGlobal('matchMedia', vi.fn(() => list))
    const { result, unmount } = renderHook(() => useMediaQuery(NARROW_SCREEN_QUERY))
    expect(result.current).toBe(true)
    act(() => {
      list.matches = false
      listener?.()
    })
    expect(result.current).toBe(false)
    unmount()
    expect(list.removeEventListener).toHaveBeenCalled()
  })
})
