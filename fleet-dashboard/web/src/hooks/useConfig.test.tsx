import { renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { testConfig } from '../test/fixtures'
import { jsonResponse } from '../test/http'
import { DEFAULT_CONFIG, useConfig } from './useConfig'

describe('useConfig', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('starts with the defaults, not yet loaded', () => {
    vi.stubGlobal('fetch', vi.fn(() => new Promise<Response>(() => {})))
    const { result } = renderHook(() => useConfig())
    expect(result.current.loaded).toBe(false)
    expect(result.current.pollIntervalSeconds).toBe(DEFAULT_CONFIG.pollIntervalSeconds)
    expect(result.current.version).toBe(DEFAULT_CONFIG.version)
  })

  it('keeps the defaults but marks loaded when the fetch fails', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse({ error: 'not found' }, 404)))
    const { result } = renderHook(() => useConfig())
    await waitFor(() => expect(result.current.loaded).toBe(true))
    expect(result.current.pollIntervalSeconds).toBe(DEFAULT_CONFIG.pollIntervalSeconds)
    expect(result.current.version).toBe(DEFAULT_CONFIG.version)
  })

  it('propagates pollIntervalSeconds and version and flips loaded on success', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse(testConfig)))
    const { result } = renderHook(() => useConfig())
    expect(result.current.loaded).toBe(false)
    await waitFor(() => expect(result.current.loaded).toBe(true))
    expect(result.current.pollIntervalSeconds).toBe(testConfig.pollIntervalSeconds)
    expect(result.current.version).toBe(testConfig.version)
  })
})

describe('useConfig thresholds and user', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('defaults to the collector thresholds and no user', () => {
    expect(DEFAULT_CONFIG.thresholds).toEqual({ gpuUtilWarn: 90, queueWarn: 50, latencyWarnMs: 5000 })
    expect(DEFAULT_CONFIG.user).toBeNull()
  })

  it('exposes the user and thresholds from the response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => jsonResponse({ ...testConfig, user: 'alice', thresholds: { gpuUtilWarn: 80, queueWarn: 10, latencyWarnMs: 900 } })),
    )
    const { result } = renderHook(() => useConfig())
    await waitFor(() => expect(result.current.loaded).toBe(true))
    expect(result.current.user).toBe('alice')
    expect(result.current.thresholds.gpuUtilWarn).toBe(80)
  })
})
