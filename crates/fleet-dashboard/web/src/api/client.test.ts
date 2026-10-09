import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, errorMessage, fetchSeries, fetchSite } from './client'
import { jsonResponse, stubFetchRoutes } from '../test/http'

describe('api client', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('returns parsed JSON and encodes the site name', async () => {
    const fetchMock = stubFetchRoutes({ '/api/v1/sites/': { name: 'a/b' } })
    const site = await fetchSite('a/b')
    expect(site.name).toBe('a/b')
    expect(fetchMock.mock.calls[0][0]).toBe('/api/v1/sites/a%2Fb')
  })

  it('throws ApiError with the backend error message on 404', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse({ error: 'site not found' }, 404)))
    await expect(fetchSite('nope')).rejects.toMatchObject({ status: 404, message: 'site not found' })
    await expect(fetchSite('nope')).rejects.toBeInstanceOf(ApiError)
  })

  it('falls back to the status text when the error body is not JSON', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('boom', { status: 502, statusText: 'Bad Gateway' })))
    await expect(fetchSeries('1h')).rejects.toMatchObject({ message: '502 Bad Gateway' })
  })

  it('passes the range as a query parameter', async () => {
    const fetchMock = stubFetchRoutes({ '/api/v1/series': { range: '6h', step: 120, points: [] } })
    await fetchSeries('6h')
    expect(fetchMock.mock.calls[0][0]).toBe('/api/v1/series?range=6h')
  })

  it('errorMessage handles Error and non-Error values', () => {
    expect(errorMessage(new Error('x'))).toBe('x')
    expect(errorMessage('plain')).toBe('plain')
  })
})
