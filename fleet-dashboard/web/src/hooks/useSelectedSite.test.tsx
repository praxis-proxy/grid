import { act, renderHook } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { useSelectedSite } from './useSelectedSite'

describe('useSelectedSite', () => {
  afterEach(() => window.history.replaceState(null, '', '/'))

  it('reads the initial selection from ?site= and attributes it to the url', () => {
    window.history.replaceState(null, '', '/?site=aigrid-ds-spoke2')
    const { result } = renderHook(() => useSelectedSite())
    expect(result.current.selected).toBe('aigrid-ds-spoke2')
    expect(result.current.source).toBe('url')
  })

  it('writes the selection to the URL and clears it', () => {
    const { result } = renderHook(() => useSelectedSite())
    expect(result.current.selected).toBeNull()
    expect(result.current.source).toBeNull()
    act(() => result.current.select('a b'))
    expect(result.current.selected).toBe('a b')
    expect(result.current.source).toBe('roster')
    expect(new URLSearchParams(window.location.search).get('site')).toBe('a b')
    act(() => result.current.clear())
    expect(result.current.selected).toBeNull()
    expect(result.current.source).toBeNull()
    expect(window.location.search).toBe('')
  })

  it('records the source of a selection', () => {
    const { result } = renderHook(() => useSelectedSite())
    act(() => result.current.select('x', 'map'))
    expect(result.current.source).toBe('map')
  })

  it('follows browser navigation', () => {
    const { result } = renderHook(() => useSelectedSite())
    window.history.replaceState(null, '', '/?site=aigrid-ds-spoke3')
    act(() => {
      window.dispatchEvent(new PopStateEvent('popstate'))
    })
    expect(result.current.selected).toBe('aigrid-ds-spoke3')
    expect(result.current.source).toBe('url')
  })
})
