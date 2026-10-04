import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import FleetOverlay, { REGISTER_COMMAND } from './FleetOverlay'

describe('FleetOverlay', () => {
  it('shows the connecting and waiting states with a spinner', () => {
    const { rerender } = render(<FleetOverlay kind="loading" />)
    expect(screen.getByRole('status')).toHaveTextContent('Connecting to hub')
    rerender(<FleetOverlay kind="waiting" />)
    expect(screen.getByRole('status')).toHaveTextContent('Waiting for the first poll')
  })
  it('shows the error in plain words with a Retry button', () => {
    const onRetry = vi.fn()
    render(<FleetOverlay kind="error" message="502 Bad Gateway" onRetry={onRetry} />)
    expect(screen.getByRole('status')).toHaveTextContent('Fleet data unavailable')
    expect(screen.getByText('502 Bad Gateway')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(onRetry).toHaveBeenCalledTimes(1)
  })
  it('shows the register command for an empty fleet', () => {
    render(<FleetOverlay kind="empty" />)
    expect(screen.getByText('No sites registered yet')).toBeInTheDocument()
    expect(screen.getByText(REGISTER_COMMAND)).toHaveClass('font-mono')
    expect(REGISTER_COMMAND).toContain('hack/register-site.sh')
  })
})
