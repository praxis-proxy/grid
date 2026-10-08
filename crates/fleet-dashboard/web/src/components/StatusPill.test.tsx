import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { makeSnapshot } from '../test/fixtures'
import StatusPill from './StatusPill'

const summary = { ...makeSnapshot().summary, sitesGreen: 5, sitesYellow: 2, sitesRed: 1 }

describe('StatusPill', () => {
  it('shows the three counts with their words', () => {
    render(<StatusPill summary={summary} filter={null} onFilterChange={() => {}} />)
    expect(screen.getByRole('button', { name: '5 healthy' })).toHaveAttribute('aria-pressed', 'false')
    expect(screen.getByRole('button', { name: '2 degraded' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '1 down' })).toBeInTheDocument()
  })
  it('shows dashes before the first snapshot', () => {
    render(<StatusPill summary={null} filter={null} onFilterChange={() => {}} />)
    expect(screen.getByRole('button', { name: '-- down' })).toBeInTheDocument()
  })
  it('filters on click and clears when the pressed count is clicked again', () => {
    const onFilterChange = vi.fn()
    const { rerender } = render(<StatusPill summary={summary} filter={null} onFilterChange={onFilterChange} />)
    fireEvent.click(screen.getByRole('button', { name: '1 down' }))
    expect(onFilterChange).toHaveBeenCalledWith('red')
    rerender(<StatusPill summary={summary} filter="red" onFilterChange={onFilterChange} />)
    expect(screen.getByRole('button', { name: '1 down' })).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByRole('button', { name: '1 down' }))
    expect(onFilterChange).toHaveBeenLastCalledWith(null)
  })
})
