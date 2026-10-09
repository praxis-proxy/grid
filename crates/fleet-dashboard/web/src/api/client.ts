import type { AppConfig, FleetSnapshot, SeriesRange, SeriesResponse, SiteDetail } from './types'

export const API_BASE = '/api/v1'
export const STREAM_URL = `${API_BASE}/stream`

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' }, signal })
  if (!res.ok) {
    let message = `${res.status} ${res.statusText}`
    try {
      const body = (await res.json()) as { error?: string }
      if (typeof body.error === 'string' && body.error) message = body.error
    } catch {
      // Body was not JSON; keep the status line as the message.
    }
    throw new ApiError(res.status, message)
  }
  return (await res.json()) as T
}

export function fetchConfig(signal?: AbortSignal): Promise<AppConfig> {
  return getJson<AppConfig>(`${API_BASE}/config`, signal)
}

export function fetchFleet(signal?: AbortSignal): Promise<FleetSnapshot> {
  return getJson<FleetSnapshot>(`${API_BASE}/fleet`, signal)
}

export function fetchSite(name: string, signal?: AbortSignal): Promise<SiteDetail> {
  return getJson<SiteDetail>(`${API_BASE}/sites/${encodeURIComponent(name)}`, signal)
}

export function fetchSeries(range: SeriesRange, signal?: AbortSignal): Promise<SeriesResponse> {
  return getJson<SeriesResponse>(`${API_BASE}/series?range=${range}`, signal)
}
