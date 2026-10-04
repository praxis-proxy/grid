import { fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeSite } from '../test/fixtures'
import RosterRail from './RosterRail'

const ohio = makeSite({ name: 'ohio', displayName: 'Ohio', region: 'us-east-2', health: 'green', gpus: { total: 64, utilPct: 67 } })
const london = makeSite({ name: 'london', displayName: 'London', region: 'eu-west-2', health: 'yellow', reasons: ['GPU utilization 91% >= 90%'], gpus: { total: 16, utilPct: 91 } })
const lima = makeSite({ name: 'lima', displayName: 'Lima', region: 'sa-east-1', health: 'red', reasons: ['metrics unreachable'], gpus: { total: 4, utilPct: null }, lastSeen: '2026-09-06T11:59:15Z' })
const lab = makeSite({ name: 'aigrid-lab', displayName: 'Lab rack', region: 'unknown-lab', dc: 'onprem', placed: false, lat: null, lng: null, gpus: { total: 2, utilPct: 20 }, models: [{ name: 'mistral-7b', running: 1 }] })
const sites = [ohio, london, lima, lab]

function renderRail(props: Partial<React.ComponentProps<typeof RosterRail>> = {}) {
  const onSelect = vi.fn()
  const onClear = vi.fn()
  const onCollapsedChange = vi.fn()
  const view = render(
    <RosterRail
      sites={sites}
      selected={null}
      onSelect={onSelect}
      onClear={onClear}
      healthFilter={null}
      pollIntervalSeconds={15}
      collapsed={false}
      onCollapsedChange={onCollapsedChange}
      {...props}
    />,
  )
  const rerenderWith = (next: Partial<React.ComponentProps<typeof RosterRail>>) =>
    view.rerender(
      <RosterRail
        sites={sites}
        selected={null}
        onSelect={onSelect}
        onClear={onClear}
        healthFilter={null}
        pollIntervalSeconds={15}
        collapsed={false}
        onCollapsedChange={onCollapsedChange}
        {...props}
        {...next}
      />,
    )
  return { onSelect, onClear, onCollapsedChange, rerenderWith }
}

const optionNames = () => screen.getAllByRole('option').map((option) => within(option).getByText(/Ohio|London|Lima|Lab rack/).textContent)

describe('RosterRail', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    vi.setSystemTime(new Date('2026-09-06T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('lists every site worst first with GPU count, utilization, reason lines and the no-position tag', () => {
    renderRail()
    expect(optionNames()).toEqual(['Lima', 'London', 'Ohio', 'Lab rack'])
    const limaRow = screen.getByRole('option', { name: /Lima/ })
    expect(limaRow).toHaveTextContent('4 GPU')
    expect(limaRow).toHaveTextContent('unreachable · last seen 45s ago')
    expect(within(limaRow).getByRole('img', { name: 'Down' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: /London/ })).toHaveTextContent('GPU utilization 91% >= 90%')
    expect(screen.getByRole('option', { name: /Ohio/ })).toHaveTextContent('67%')
    expect(screen.getByRole('option', { name: /Lab rack/ })).toHaveTextContent('no position')
    expect(screen.getByRole('option', { name: /Ohio/ })).not.toHaveTextContent('no position')
    expect(screen.getByText('4 sites · 86 GPUs · polled every 15 s')).toBeInTheDocument()
  })

  it('filters by search text across names, regions, dc and models, and says when nothing matches', () => {
    renderRail()
    const search = screen.getByRole('searchbox', { name: 'Search sites' })
    fireEvent.change(search, { target: { value: 'mistral' } })
    expect(optionNames()).toEqual(['Lab rack'])
    fireEvent.change(search, { target: { value: 'eu-west' } })
    expect(optionNames()).toEqual(['London'])
    fireEvent.change(search, { target: { value: 'zzz' } })
    expect(screen.getByText('No sites match')).toBeInTheDocument()
    expect(screen.queryAllByRole('option')).toHaveLength(0)
  })

  it('applies the health filter from the status pill and re-sorts on demand', () => {
    renderRail({ healthFilter: 'green' })
    expect(optionNames()).toEqual(['Ohio', 'Lab rack'])
    fireEvent.click(screen.getByRole('button', { name: 'Name' }))
    expect(optionNames()).toEqual(['Lab rack', 'Ohio'])
  })

  it('selects on click and marks the selected row', () => {
    const { onSelect } = renderRail({ selected: 'london' })
    expect(screen.getByRole('option', { name: /London/ })).toHaveAttribute('aria-selected', 'true')
    fireEvent.click(screen.getByRole('option', { name: /Ohio/ }))
    expect(onSelect).toHaveBeenCalledWith('ohio')
  })

  it('moves with arrow keys, selects with Enter or Space, clears with Escape', () => {
    const { onSelect, onClear } = renderRail()
    const list = screen.getByRole('listbox', { name: 'Site list' })
    list.focus()
    fireEvent.keyDown(list, { key: 'ArrowDown' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-lima')
    fireEvent.keyDown(list, { key: 'ArrowDown' })
    fireEvent.keyDown(list, { key: 'ArrowDown' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-ohio')
    fireEvent.keyDown(list, { key: 'Enter' })
    expect(onSelect).toHaveBeenLastCalledWith('ohio')
    fireEvent.keyDown(list, { key: 'ArrowUp' })
    fireEvent.keyDown(list, { key: ' ' })
    expect(onSelect).toHaveBeenLastCalledWith('london')
    fireEvent.keyDown(list, { key: 'End' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-aigrid-lab')
    fireEvent.keyDown(list, { key: 'Home' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-lima')
    fireEvent.keyDown(list, { key: 'Escape' })
    expect(onClear).toHaveBeenCalledTimes(1)
    expect(list).not.toHaveAttribute('aria-activedescendant')
  })

  it('collapses to health shapes with names as tooltips', () => {
    const { onSelect, onCollapsedChange } = renderRail({ collapsed: true, selected: 'lima' })
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
    const lima = screen.getByRole('button', { name: 'Lima' })
    expect(lima).toHaveAttribute('title', 'Lima')
    expect(lima).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByRole('button', { name: 'Ohio' }))
    expect(onSelect).toHaveBeenCalledWith('ohio')
    fireEvent.click(screen.getByRole('button', { name: 'Expand roster' }))
    expect(onCollapsedChange).toHaveBeenCalledWith(false)
  })

  it('disables the toggle while forced collapsed by a narrow viewport', () => {
    const { onCollapsedChange } = renderRail({ collapsed: true, forced: true })
    const toggle = screen.getByRole('button', { name: 'Expand roster' })
    expect(toggle).toBeDisabled()
    expect(toggle).toHaveAttribute('title', 'Roster collapses automatically below 1280 px')
    expect(toggle).toHaveAttribute('aria-disabled', 'true')
    fireEvent.click(toggle)
    expect(onCollapsedChange).not.toHaveBeenCalled()
  })

  it('renders skeleton rows while loading', () => {
    renderRail({ sites: [], loading: true })
    expect(screen.getByRole('list', { name: 'Loading sites' })).toHaveAttribute('aria-busy', 'true')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })

  it('shows No sites match only when a filter emptied a non-empty roster', () => {
    renderRail({ sites: [] })
    expect(screen.queryByText('No sites match')).not.toBeInTheDocument()
  })

  it('shows dashes in the footer counts before the first snapshot arrives', () => {
    renderRail({ sites: [], loading: true })
    expect(screen.getByText('-- sites · -- GPUs · polled every 15 s')).toBeInTheDocument()
  })

  it('moves the keyboard cursor to a selection made elsewhere (map, URL)', () => {
    const { rerenderWith } = renderRail({ selected: 'lima' })
    const list = screen.getByRole('listbox', { name: 'Site list' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-lima')
    list.focus()
    fireEvent.keyDown(list, { key: 'ArrowDown' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-london')
    rerenderWith({ selected: 'ohio' })
    expect(list).toHaveAttribute('aria-activedescendant', 'roster-option-ohio')
  })
})
