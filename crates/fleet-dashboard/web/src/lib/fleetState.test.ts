import { describe, expect, it } from 'vitest'
import { makeSnapshot } from '../test/fixtures'
import {
  backoffMs,
  deriveStatus,
  fleetReducer,
  initialFleetState,
  parseSnapshot,
  type FleetState,
} from './fleetState'

describe('fleetReducer', () => {
  it('stores a snapshot with its arrival time and clears errors', () => {
    const errored: FleetState = { ...initialFleetState, error: 'boom' }
    const next = fleetReducer(errored, { type: 'snapshot', snapshot: makeSnapshot(), at: 1000 })
    expect(next.snapshot?.generatedAt).toBe('2026-09-06T12:00:00Z')
    expect(next.lastUpdate).toBe(1000)
    expect(next.error).toBeNull()
  })
  it('tracks connection open and close', () => {
    const open = fleetReducer(initialFleetState, { type: 'open' })
    expect(open.connected).toBe(true)
    expect(fleetReducer(open, { type: 'closed' }).connected).toBe(false)
  })
  it('records fetch failures without dropping the last snapshot', () => {
    const withSnap = fleetReducer(initialFleetState, { type: 'snapshot', snapshot: makeSnapshot(), at: 1 })
    const failed = fleetReducer(withSnap, { type: 'fetchFailed', message: '502 Bad Gateway', status: 502 })
    expect(failed.error).toBe('502 Bad Gateway')
    expect(failed.errorStatus).toBe(502)
    expect(failed.snapshot).not.toBeNull()
  })

  it('records the failure status so the caller can special-case 503', () => {
    const failed = fleetReducer(initialFleetState, {
      type: 'fetchFailed',
      message: 'collector not ready',
      status: 503,
    })
    expect(failed.errorStatus).toBe(503)
  })

  it('clears the error and its status once a snapshot arrives', () => {
    const failed = fleetReducer(initialFleetState, { type: 'fetchFailed', message: 'boom', status: 503 })
    const recovered = fleetReducer(failed, { type: 'snapshot', snapshot: makeSnapshot(), at: 1 })
    expect(recovered.error).toBeNull()
    expect(recovered.errorStatus).toBeNull()
  })

  it('clears the failure on retry but keeps any snapshot', () => {
    const withSnap = fleetReducer(initialFleetState, { type: 'snapshot', snapshot: makeSnapshot(), at: 1 })
    const failed = fleetReducer(withSnap, { type: 'fetchFailed', message: 'boom', status: 502 })
    const retried = fleetReducer(failed, { type: 'retry' })
    expect(retried.error).toBeNull()
    expect(retried.errorStatus).toBeNull()
    expect(retried.snapshot).not.toBeNull()
  })

  it('ignores an incoming snapshot that is not newer than the current one', () => {
    const first = fleetReducer(initialFleetState, {
      type: 'snapshot',
      snapshot: makeSnapshot({ generatedAt: '2026-09-06T12:00:05Z' }),
      at: 1,
    })
    const olderOrEqual = fleetReducer(first, {
      type: 'snapshot',
      snapshot: makeSnapshot({ generatedAt: '2026-09-06T12:00:05Z' }),
      at: 2,
    })
    expect(olderOrEqual).toBe(first)
    const older = fleetReducer(first, {
      type: 'snapshot',
      snapshot: makeSnapshot({ generatedAt: '2026-09-06T12:00:00Z' }),
      at: 3,
    })
    expect(older).toBe(first)
  })

  it('accepts a newer snapshot even with a differing timezone offset', () => {
    const first = fleetReducer(initialFleetState, {
      type: 'snapshot',
      snapshot: makeSnapshot({ generatedAt: '2026-09-06T12:00:05Z' }),
      at: 1,
    })
    // Same instant expressed with an explicit offset should not be treated as newer.
    const sameInstant = fleetReducer(first, {
      type: 'snapshot',
      snapshot: makeSnapshot({ generatedAt: '2026-09-06T13:00:05+01:00' }),
      at: 2,
    })
    expect(sameInstant).toBe(first)
    const newer = fleetReducer(first, {
      type: 'snapshot',
      snapshot: makeSnapshot({ generatedAt: '2026-09-06T14:00:06+01:00' }),
      at: 3,
    })
    expect(newer.snapshot?.generatedAt).toBe('2026-09-06T14:00:06+01:00')
  })
})

describe('backoffMs', () => {
  it('doubles from 1s and caps at 30s', () => {
    expect(backoffMs(1)).toBe(1000)
    expect(backoffMs(2)).toBe(2000)
    expect(backoffMs(3)).toBe(4000)
    expect(backoffMs(5)).toBe(16000)
    expect(backoffMs(6)).toBe(30000)
    expect(backoffMs(50)).toBe(30000)
    expect(backoffMs(0)).toBe(1000)
  })
})

describe('deriveStatus', () => {
  const poll = 5
  it('is reconnecting before the stream is open', () => {
    expect(deriveStatus(initialFleetState, 0, poll)).toBe('reconnecting')
  })
  it('is live when connected and the snapshot is fresh', () => {
    const state: FleetState = { ...initialFleetState, connected: true, lastUpdate: 10_000 }
    expect(deriveStatus(state, 24_000, poll)).toBe('live')
    expect(deriveStatus(state, 25_000, poll)).toBe('live')
  })
  it('is stale once the snapshot is older than three poll intervals, even if connected', () => {
    const state: FleetState = { ...initialFleetState, connected: true, lastUpdate: 10_000 }
    expect(deriveStatus(state, 25_001, poll)).toBe('stale')
  })
  it('prefers stale over reconnecting when disconnected with old data', () => {
    const state: FleetState = { ...initialFleetState, connected: false, lastUpdate: 0 }
    expect(deriveStatus(state, 60_000, poll)).toBe('stale')
  })
})

describe('parseSnapshot', () => {
  it('parses a valid fleet event payload', () => {
    const snap = makeSnapshot()
    expect(parseSnapshot(JSON.stringify(snap))).toEqual(snap)
  })
  it('rejects garbage, non-strings and objects without sites', () => {
    expect(parseSnapshot('not json')).toBeNull()
    expect(parseSnapshot(42)).toBeNull()
    expect(parseSnapshot(JSON.stringify({ generatedAt: 'x' }))).toBeNull()
  })
})
