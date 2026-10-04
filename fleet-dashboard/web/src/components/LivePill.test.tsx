import { act, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import LivePill from './LivePill'

describe('LivePill', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('keeps only the state word in the live region and shows the age beside it', () => {
    const { container } = render(<LivePill status="live" lastUpdate={Date.now() - 3000} />)
    expect(screen.getByRole('status')).toHaveTextContent(/^LIVE$/)
    expect(screen.getByRole('status')).toHaveAttribute('aria-live', 'polite')
    expect(container).toHaveTextContent('LIVE · updated 3s ago')
  })
  it('shows RECONNECTING without an age', () => {
    const { container } = render(<LivePill status="reconnecting" lastUpdate={Date.now() - 12_000} />)
    expect(container).toHaveTextContent(/^RECONNECTING$/)
  })
  it('shows STALE with the last update age', () => {
    const { container } = render(<LivePill status="stale" lastUpdate={Date.now() - 130_000} />)
    expect(container).toHaveTextContent('STALE · last update 2m ago')
  })
  it('omits the age when there is no snapshot yet', () => {
    const { container } = render(<LivePill status="live" lastUpdate={null} />)
    expect(container).toHaveTextContent(/^LIVE$/)
  })
  it('ticks the age once a second on its own', () => {
    const { container } = render(<LivePill status="live" lastUpdate={Date.now()} />)
    expect(container).toHaveTextContent('LIVE · updated 0s ago')
    act(() => vi.advanceTimersByTime(5000))
    expect(container).toHaveTextContent('LIVE · updated 5s ago')
  })
})
