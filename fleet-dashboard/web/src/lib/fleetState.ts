import type { FleetSnapshot } from '../api/types'

export type StreamStatus = 'live' | 'reconnecting' | 'stale'

export interface FleetState {
  snapshot: FleetSnapshot | null
  /** Client-clock epoch ms when the current snapshot arrived. */
  lastUpdate: number | null
  connected: boolean
  /** Set once the current snapshot is older than STALE_POLLS poll intervals. */
  stale: boolean
  error: string | null
  /** HTTP status of the last fetch failure, when known (e.g. 503 before the collector's first poll). */
  errorStatus: number | null
}

export type FleetAction =
  | { type: 'snapshot'; snapshot: FleetSnapshot; at: number }
  | { type: 'open' }
  | { type: 'closed' }
  | { type: 'stale' }
  | { type: 'fetchFailed'; message: string; status: number | null }
  /** The user pressed Retry: forget the failure while the new fetch is in flight. */
  | { type: 'retry' }

export const initialFleetState: FleetState = {
  snapshot: null,
  lastUpdate: null,
  connected: false,
  stale: false,
  error: null,
  errorStatus: null,
}

export function fleetReducer(state: FleetState, action: FleetAction): FleetState {
  switch (action.type) {
    case 'snapshot': {
      // Ignore an incoming snapshot that is not newer than the one we have.
      // RFC 3339 timestamps are not always safe to string-compare (differing
      // zone offsets, fractional-second precision), so compare Date.parse.
      const current = state.snapshot ? Date.parse(state.snapshot.generatedAt) : Number.NEGATIVE_INFINITY
      const incoming = Date.parse(action.snapshot.generatedAt)
      if (Number.isFinite(incoming) && incoming <= current) return state
      return { ...state, snapshot: action.snapshot, lastUpdate: action.at, stale: false, error: null, errorStatus: null }
    }
    case 'open':
      return { ...state, connected: true }
    case 'closed':
      return { ...state, connected: false }
    case 'stale':
      return { ...state, stale: true }
    case 'fetchFailed':
      return { ...state, error: action.message, errorStatus: action.status }
    case 'retry':
      return { ...state, error: null, errorStatus: null }
  }
}

export const BACKOFF_MIN_MS = 1000
export const BACKOFF_MAX_MS = 30000

/** attempt 1 -> 1s, 2 -> 2s, 3 -> 4s, ... capped at 30s. */
export function backoffMs(attempt: number): number {
  const n = Math.max(1, Math.floor(attempt))
  return Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** (n - 1))
}

export const STALE_POLLS = 3

export function deriveStatus(state: FleetState, now: number, pollIntervalSeconds: number): StreamStatus {
  if (state.lastUpdate !== null && now - state.lastUpdate > STALE_POLLS * pollIntervalSeconds * 1000) {
    return 'stale'
  }
  if (!state.connected) return 'reconnecting'
  return 'live'
}

export function parseSnapshot(data: unknown): FleetSnapshot | null {
  if (typeof data !== 'string') return null
  try {
    const obj = JSON.parse(data) as Partial<FleetSnapshot> | null
    if (obj && typeof obj.generatedAt === 'string' && Array.isArray(obj.sites) && obj.summary) {
      return obj as FleetSnapshot
    }
    return null
  } catch {
    return null
  }
}
