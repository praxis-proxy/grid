import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../test/fakeEventSource'
import { makeSnapshot } from '../test/fixtures'
import { jsonResponse } from '../test/http'
import { useFleet } from './useFleet'

const pendingFetch = () => vi.fn(() => new Promise<Response>(() => {}))

describe('useFleet', () => {
  beforeEach(() => {
    FakeEventSource.reset()
    vi.stubGlobal('EventSource', FakeEventSource)
  })
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
  })

  it('loads the initial snapshot over fetch and opens the stream', async () => {
    const snap = makeSnapshot()
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse(snap)))
    const { result } = renderHook(() => useFleet(5))
    expect(result.current.status).toBe('reconnecting')
    expect(result.current.snapshot).toBeNull()
    await waitFor(() => expect(result.current.snapshot?.generatedAt).toBe(snap.generatedAt))
    expect(FakeEventSource.instances).toHaveLength(1)
    expect(FakeEventSource.instances[0].url).toBe('/api/v1/stream')
    act(() => FakeEventSource.instances[0].emitOpen())
    expect(result.current.status).toBe('live')
    expect(result.current.lastUpdate).not.toBeNull()
  })

  it('applies fleet events from the stream', () => {
    vi.stubGlobal('fetch', pendingFetch())
    const { result } = renderHook(() => useFleet(5))
    const es = FakeEventSource.instances[0]
    act(() => {
      es.emitOpen()
      es.emitFleet(makeSnapshot({ generatedAt: '2026-09-06T12:00:05Z' }))
    })
    expect(result.current.snapshot?.generatedAt).toBe('2026-09-06T12:00:05Z')
    expect(result.current.status).toBe('live')
  })

  it('surfaces the initial fetch error and its status while keeping the stream', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse({ error: 'collector not ready' }, 503)))
    const { result } = renderHook(() => useFleet(5))
    await waitFor(() => expect(result.current.error).toBe('collector not ready'))
    expect(result.current.errorStatus).toBe(503)
    expect(FakeEventSource.instances).toHaveLength(1)
  })

  it('clears the error status once a snapshot arrives', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse({ error: 'collector not ready' }, 503)))
    const { result } = renderHook(() => useFleet(5))
    await waitFor(() => expect(result.current.errorStatus).toBe(503))
    const es = FakeEventSource.instances[0]
    act(() => {
      es.emitOpen()
      es.emitFleet(makeSnapshot())
    })
    expect(result.current.errorStatus).toBeNull()
    expect(result.current.error).toBeNull()
  })

  it('retry re-fetches the snapshot and opens a fresh stream', async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ error: 'upstream down' }, 502))
    vi.stubGlobal('fetch', fetchMock)
    const { result } = renderHook(() => useFleet(5))
    await waitFor(() => expect(result.current.error).toBe('upstream down'))
    expect(FakeEventSource.instances).toHaveLength(1)
    fetchMock.mockImplementation(async () => jsonResponse(makeSnapshot()))
    act(() => result.current.retry())
    expect(result.current.error).toBeNull()
    expect(result.current.errorStatus).toBeNull()
    await waitFor(() => expect(result.current.snapshot).not.toBeNull())
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(FakeEventSource.instances).toHaveLength(2)
    expect(FakeEventSource.instances[0].closed).toBe(true)
  })

  it('reconnects with exponential backoff after stream errors', () => {
    vi.useFakeTimers()
    vi.stubGlobal('fetch', pendingFetch())
    const { result } = renderHook(() => useFleet(5))
    const first = FakeEventSource.instances[0]
    act(() => first.emitOpen())
    expect(result.current.status).toBe('live')

    act(() => first.emitError())
    expect(first.closed).toBe(true)
    expect(result.current.status).toBe('reconnecting')

    act(() => {
      vi.advanceTimersByTime(999)
    })
    expect(FakeEventSource.instances).toHaveLength(1)
    act(() => {
      vi.advanceTimersByTime(1)
    })
    expect(FakeEventSource.instances).toHaveLength(2)

    act(() => FakeEventSource.instances[1].emitError())
    act(() => {
      vi.advanceTimersByTime(1999)
    })
    expect(FakeEventSource.instances).toHaveLength(2)
    act(() => {
      vi.advanceTimersByTime(1)
    })
    expect(FakeEventSource.instances).toHaveLength(3)

    act(() => FakeEventSource.instances[2].emitOpen())
    expect(result.current.status).toBe('live')
  })

  it('reports stale when no snapshot arrives for three poll intervals', () => {
    vi.useFakeTimers()
    vi.stubGlobal('fetch', pendingFetch())
    const { result } = renderHook(() => useFleet(5))
    const es = FakeEventSource.instances[0]
    act(() => {
      es.emitOpen()
      es.emitFleet(makeSnapshot())
    })
    expect(result.current.status).toBe('live')
    act(() => {
      vi.advanceTimersByTime(15_000)
    })
    expect(result.current.status).toBe('live')
    act(() => {
      vi.advanceTimersByTime(1_000)
    })
    expect(result.current.status).toBe('stale')
  })

  it('re-arms the stale timer when a fresh snapshot arrives', () => {
    vi.useFakeTimers()
    vi.stubGlobal('fetch', pendingFetch())
    const { result } = renderHook(() => useFleet(5))
    const es = FakeEventSource.instances[0]
    act(() => {
      es.emitOpen()
      es.emitFleet(makeSnapshot())
    })
    act(() => {
      vi.advanceTimersByTime(10_000)
      es.emitFleet(makeSnapshot({ generatedAt: '2026-09-06T12:00:10Z' }))
    })
    act(() => {
      vi.advanceTimersByTime(15_000)
    })
    expect(result.current.status).toBe('live')
    act(() => {
      vi.advanceTimersByTime(1_000)
    })
    expect(result.current.status).toBe('stale')
  })

  it('closes the stream and stops timers on unmount', () => {
    vi.useFakeTimers()
    vi.stubGlobal('fetch', pendingFetch())
    const { unmount } = renderHook(() => useFleet(5))
    const es = FakeEventSource.instances[0]
    act(() => es.emitError())
    unmount()
    expect(es.closed).toBe(true)
    act(() => {
      vi.advanceTimersByTime(60_000)
    })
    expect(FakeEventSource.instances).toHaveLength(1)
  })
})
