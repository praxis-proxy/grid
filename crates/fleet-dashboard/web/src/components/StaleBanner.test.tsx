import { act, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import StaleBanner from './StaleBanner'

describe('StaleBanner', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('keeps the ticking age out of the alert, in a sibling next to it', () => {
    render(<StaleBanner lastUpdate={Date.now() - 50_000} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/^Data is stale$/)
    expect(screen.getByText(/last update 50s ago/)).toBeInTheDocument()
    act(() => vi.advanceTimersByTime(10_000))
    expect(screen.getByRole('alert')).toHaveTextContent(/^Data is stale$/)
    expect(screen.getByText(/last update 1m ago/)).toBeInTheDocument()
  })
  it('copes without a last update', () => {
    render(<StaleBanner lastUpdate={null} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/^Data is stale$/)
    expect(screen.queryByText(/last update/)).not.toBeInTheDocument()
  })
})
