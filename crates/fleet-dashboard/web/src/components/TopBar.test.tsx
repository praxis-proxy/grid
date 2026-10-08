import { render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeSnapshot } from '../test/fixtures'
import TopBar from './TopBar'

const hub = { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 }
const summary = makeSnapshot().summary

function renderBar(props: Partial<React.ComponentProps<typeof TopBar>> = {}) {
  return render(
    <TopBar
      hub={hub}
      version="v0.1.0"
      status="live"
      lastUpdate={Date.now() - 5000}
      summary={summary}
      healthFilter={null}
      onHealthFilterChange={() => {}}
      user={null}
      {...props}
    />,
  )
}

describe('TopBar', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('renders product name, hub chip with the version in its tooltip, status pill and live pill', () => {
    const { container } = renderBar()
    expect(screen.getByText('AI GRID FLEET')).toBeInTheDocument()
    expect(screen.getByText('aigrid-ds-hub')).toBeInTheDocument()
    expect(screen.getByText('us-east-1')).toBeInTheDocument()
    expect(screen.getByTitle('hub aigrid-ds-hub · version v0.1.0')).toBeInTheDocument()
    expect(screen.queryByText('v0.1.0')).not.toBeInTheDocument()
    expect(screen.getByRole('group', { name: 'Fleet status' })).toBeInTheDocument()
    expect(screen.getByRole('status')).toHaveTextContent('LIVE')
    expect(container).toHaveTextContent('LIVE · updated 5s ago')
  })
  it('says when no hub is configured', () => {
    renderBar({ hub: null, status: 'reconnecting', lastUpdate: null, version: '' })
    expect(screen.getByText('hub not configured')).toBeInTheDocument()
  })
  it('shows the user and a sign-out link only when a user is known', () => {
    const { rerender } = renderBar({ user: 'alice' })
    expect(screen.getByText('alice')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Sign out' })).toHaveAttribute('href', '/oauth/sign_out')
    rerender(
      <TopBar hub={hub} version="" status="live" lastUpdate={null} summary={summary} healthFilter={null} onHealthFilterChange={() => {}} user={null} />,
    )
    expect(screen.queryByRole('link', { name: 'Sign out' })).not.toBeInTheDocument()
  })
})
