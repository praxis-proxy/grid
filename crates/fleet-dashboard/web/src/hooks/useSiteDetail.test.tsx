import { renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { makeSiteDetail } from '../test/fixtures'
import { stubFetchRoutes } from '../test/http'
import { useSiteDetail } from './useSiteDetail'

describe('useSiteDetail', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('does nothing for a null name', () => {
    const fetchMock = stubFetchRoutes({})
    const { result } = renderHook(() => useSiteDetail(null))
    expect(result.current).toEqual({ detail: null, loading: false, error: null })
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('loads the detail for a site and reports loading while in flight', async () => {
    stubFetchRoutes({ '/api/v1/sites/': makeSiteDetail() })
    const { result } = renderHook(() => useSiteDetail('aigrid-ds-spoke1'))
    expect(result.current.loading).toBe(true)
    await waitFor(() => expect(result.current.detail?.series.points).toHaveLength(2))
    expect(result.current.loading).toBe(false)
  })

  it('drops a stale detail when the name changes', async () => {
    stubFetchRoutes({ '/api/v1/sites/': makeSiteDetail() })
    const { result, rerender } = renderHook(({ name }: { name: string | null }) => useSiteDetail(name), {
      initialProps: { name: 'aigrid-ds-spoke1' as string | null },
    })
    await waitFor(() => expect(result.current.detail).not.toBeNull())
    rerender({ name: 'other' })
    expect(result.current.detail).toBeNull()
    expect(result.current.loading).toBe(true)
  })

  it('reports the backend error message', async () => {
    stubFetchRoutes({})
    const { result } = renderHook(() => useSiteDetail('nope'))
    await waitFor(() => expect(result.current.error).toBe('not found'))
    expect(result.current.loading).toBe(false)
  })

  it('refetches the same site when refreshKey changes', async () => {
    const fetchMock = stubFetchRoutes({ '/api/v1/sites/': makeSiteDetail() })
    const { result, rerender } = renderHook(
      ({ refreshKey }: { refreshKey: string }) => useSiteDetail('aigrid-ds-spoke1', refreshKey),
      { initialProps: { refreshKey: '2026-09-06T12:00:00Z' } },
    )
    await waitFor(() => expect(result.current.detail).not.toBeNull())
    expect(fetchMock).toHaveBeenCalledTimes(1)
    rerender({ refreshKey: '2026-09-06T12:00:15Z' })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    expect(result.current.detail).not.toBeNull()
  })
})
