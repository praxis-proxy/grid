import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeSite } from '../test/fixtures'
import AttentionStrip from './AttentionStrip'

const down = makeSite({ name: 'lima', displayName: 'Lima', health: 'red', reasons: ['metrics unreachable (2 consecutive failures)'], lastSeen: '2026-09-06T11:59:15Z' })
const degraded = makeSite({ name: 'fra', displayName: 'Frankfurt', health: 'yellow', reasons: ['GPU utilization 95% >= 90%'] })

describe('AttentionStrip', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('renders nothing when every site is healthy', () => {
    const { container } = render(<AttentionStrip sites={[makeSite()]} selected={null} onSelect={() => {}} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows down sites first with reason and age, degraded sites with reason only', () => {
    render(<AttentionStrip sites={[degraded, down, makeSite()]} selected={null} onSelect={() => {}} />)
    const chips = screen.getAllByRole('button')
    expect(chips).toHaveLength(2)
    expect(chips[0]).toHaveTextContent('Lima')
    expect(chips[0]).toHaveTextContent('metrics unreachable (2 consecutive failures)')
    expect(chips[0]).toHaveTextContent('45s ago')
    expect(chips[1]).toHaveTextContent('Frankfurt')
    expect(chips[1]).toHaveTextContent('GPU utilization 95% >= 90%')
    expect(chips[1]).not.toHaveTextContent('ago')
    expect(screen.getByRole('img', { name: 'Down' })).toBeInTheDocument()
    act(() => vi.advanceTimersByTime(3000))
    expect(chips[0]).toHaveTextContent('48s ago')
  })

  it('selects a site on click and marks the selected chip pressed', () => {
    const onSelect = vi.fn()
    render(<AttentionStrip sites={[down]} selected="lima" onSelect={onSelect} />)
    const chip = screen.getByRole('button', { name: /Lima/ })
    expect(chip).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(chip)
    expect(onSelect).toHaveBeenCalledWith('lima')
  })
})
