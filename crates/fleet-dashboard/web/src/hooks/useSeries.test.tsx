import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { SeriesRange } from '../api/types'
import { makeSeries } from '../test/fixtures'
import { stubFetchRoutes } from '../test/http'
import { SERIES_REFRESH_MS, useSeries } from './useSeries'

function calledUrls(fetchMock: ReturnType<typeof stubFetchRoutes>): string[] {
  return fetchMock.mock.calls.map((call) => String(call[0]))
}

describe('useSeries', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
  })

  it('fetches the requested range and refetches when it changes', async () => {
    const fetchMock = stubFetchRoutes({ '/api/v1/series': makeSeries('1h') })
    const { result, rerender } = renderHook(({ range }: { range: SeriesRange }) => useSeries(range), {
      initialProps: { range: '1h' as SeriesRange },
    })
    await waitFor(() => expect(result.current.series?.range).toBe('1h'))
    expect(calledUrls(fetchMock)).toEqual(['/api/v1/series?range=1h'])
    rerender({ range: '6h' })
    await waitFor(() => expect(calledUrls(fetchMock)).toEqual(['/api/v1/series?range=1h', '/api/v1/series?range=6h']))
  })

  it('refreshes every 60 seconds', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const fetchMock = stubFetchRoutes({ '/api/v1/series': makeSeries('1h') })
    const { result } = renderHook(() => useSeries('1h'))
    await waitFor(() => expect(result.current.series).not.toBeNull())
    expect(fetchMock).toHaveBeenCalledTimes(1)
    act(() => {
      vi.advanceTimersByTime(SERIES_REFRESH_MS)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('exposes fetch errors', async () => {
    stubFetchRoutes({})
    const { result } = renderHook(() => useSeries('24h'))
    await waitFor(() => expect(result.current.error).toBe('not found'))
    expect(result.current.series).toBeNull()
  })

  it('clears the previous range series immediately when range changes', async () => {
    stubFetchRoutes({ '/api/v1/series': makeSeries('1h') })
    const { result, rerender } = renderHook(({ range }: { range: SeriesRange }) => useSeries(range), {
      initialProps: { range: '1h' as SeriesRange },
    })
    await waitFor(() => expect(result.current.series?.range).toBe('1h'))

    // Swap in a pending fetch that never resolves so the '6h' request stalls,
    // letting us observe the state right after the range switch.
    vi.stubGlobal('fetch', vi.fn(() => new Promise<Response>(() => {})))
    rerender({ range: '6h' })

    expect(result.current.series).toBeNull()
    expect(result.current.error).toBeNull()
  })
})
