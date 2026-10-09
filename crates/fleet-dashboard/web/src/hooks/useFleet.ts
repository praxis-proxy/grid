import { useCallback, useEffect, useReducer, useState } from 'react'
import { ApiError, errorMessage, fetchFleet, STREAM_URL } from '../api/client'
import type { FleetSnapshot } from '../api/types'
import {
  backoffMs,
  fleetReducer,
  initialFleetState,
  parseSnapshot,
  STALE_POLLS,
  type StreamStatus,
} from '../lib/fleetState'

export interface UseFleetResult {
  snapshot: FleetSnapshot | null
  status: StreamStatus
  lastUpdate: number | null
  error: string | null
  /** HTTP status of the last fetch failure, when known (e.g. 503 before the collector's first poll). */
  errorStatus: number | null
  /** Re-fetch /api/v1/fleet and reconnect the stream from scratch. */
  retry: () => void
}

/**
 * Fetches /api/v1/fleet once, then follows /api/v1/stream. The stream is
 * reconnected manually with exponential backoff (1s doubling to 30s) so the
 * browser's fixed retry interval does not apply.
 */
export function useFleet(pollIntervalSeconds: number): UseFleetResult {
  const [state, dispatch] = useReducer(fleetReducer, initialFleetState)
  // Bumped by retry(); both effects below depend on it so they tear down and start again.
  const [generation, setGeneration] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    fetchFleet(controller.signal)
      .then((snapshot) => dispatch({ type: 'snapshot', snapshot, at: Date.now() }))
      .catch((err: unknown) => {
        if (!controller.signal.aborted) {
          dispatch({ type: 'fetchFailed', message: errorMessage(err), status: err instanceof ApiError ? err.status : null })
        }
      })
    return () => controller.abort()
  }, [generation])

  useEffect(() => {
    let source: EventSource | null = null
    let timer: ReturnType<typeof setTimeout> | null = null
    let attempt = 0
    let disposed = false

    const connect = () => {
      if (disposed) return
      const es = new EventSource(STREAM_URL)
      source = es
      es.addEventListener('open', () => {
        attempt = 0
        dispatch({ type: 'open' })
      })
      es.addEventListener('fleet', (ev) => {
        const snapshot = parseSnapshot((ev as MessageEvent<string>).data)
        if (snapshot) dispatch({ type: 'snapshot', snapshot, at: Date.now() })
      })
      es.onerror = () => {
        es.close()
        if (source === es) source = null
        attempt += 1
        dispatch({ type: 'closed' })
        timer = setTimeout(connect, backoffMs(attempt))
      }
    }

    connect()
    return () => {
      disposed = true
      source?.close()
      if (timer !== null) clearTimeout(timer)
    }
  }, [generation])

  // A single timer arms per snapshot arrival and dispatches 'stale' once the
  // snapshot is older than three poll intervals, instead of ticking a clock
  // into state every second and re-rendering the whole tree on each tick.
  useEffect(() => {
    if (state.lastUpdate === null) return
    const threshold = STALE_POLLS * pollIntervalSeconds * 1000
    const elapsed = Date.now() - state.lastUpdate
    const timer = setTimeout(() => dispatch({ type: 'stale' }), Math.max(0, threshold - elapsed + 1))
    return () => clearTimeout(timer)
  }, [state.lastUpdate, pollIntervalSeconds])

  const status: StreamStatus = state.stale ? 'stale' : state.connected ? 'live' : 'reconnecting'

  const retry = useCallback(() => {
    dispatch({ type: 'retry' })
    setGeneration((g) => g + 1)
  }, [])

  return {
    snapshot: state.snapshot,
    status,
    lastUpdate: state.lastUpdate,
    error: state.error,
    errorStatus: state.errorStatus,
    retry,
  }
}
