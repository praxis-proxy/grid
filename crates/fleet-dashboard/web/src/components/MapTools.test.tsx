import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import MapTools from './MapTools'

describe('MapTools', () => {
  it('shows the current modes and forwards changes', () => {
    const onLinksChange = vi.fn()
    const onLabelsChange = vi.fn()
    const onFit = vi.fn()
    render(<MapTools links="selected" onLinksChange={onLinksChange} labels="unhealthy" onLabelsChange={onLabelsChange} onFit={onFit} />)
    expect(screen.getByRole('toolbar', { name: 'Map tools' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'selected' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'unhealthy' })).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByRole('button', { name: 'none' }))
    expect(onLinksChange).toHaveBeenCalledWith('none')
    fireEvent.click(screen.getByRole('group', { name: 'Labels' }).querySelector('button[aria-pressed="false"]') as HTMLElement)
    expect(onLabelsChange).toHaveBeenCalledWith('all')
    fireEvent.click(screen.getByRole('button', { name: 'Fit' }))
    expect(onFit).toHaveBeenCalledTimes(1)
  })
})
