import { fireEvent, render, screen } from '@testing-library/react'
import { useState } from 'react'
import { describe, expect, it } from 'vitest'
import { tabPanelId } from '../../lib/tabs'
import Tabs from './Tabs'

const tabs = [
  { id: 'overview', label: 'Overview' },
  { id: 'models', label: 'Models (2)' },
  { id: 'history', label: 'History' },
] as const

function Harness() {
  const [active, setActive] = useState<(typeof tabs)[number]['id']>('overview')
  return <Tabs label="Site sections" tabs={tabs} active={active} onChange={setActive} />
}

describe('Tabs', () => {
  it('exposes a tablist where only the active tab is tabbable and arrows move the selection', () => {
    render(<Harness />)
    expect(screen.getByRole('tablist', { name: 'Site sections' })).toBeInTheDocument()
    const overview = screen.getByRole('tab', { name: 'Overview' })
    expect(overview).toHaveAttribute('aria-selected', 'true')
    expect(overview).toHaveAttribute('tabindex', '0')
    expect(screen.getByRole('tab', { name: 'Models (2)' })).toHaveAttribute('tabindex', '-1')
    fireEvent.keyDown(screen.getByRole('tablist'), { key: 'ArrowRight' })
    expect(screen.getByRole('tab', { name: 'Models (2)' })).toHaveAttribute('aria-selected', 'true')
    fireEvent.keyDown(screen.getByRole('tablist'), { key: 'ArrowLeft' })
    fireEvent.keyDown(screen.getByRole('tablist'), { key: 'ArrowLeft' })
    expect(screen.getByRole('tab', { name: 'History' })).toHaveAttribute('aria-selected', 'true')
    fireEvent.click(overview)
    expect(overview).toHaveAttribute('aria-selected', 'true')
  })

  it('sets aria-controls only on the selected tab', () => {
    render(<Harness />)
    expect(screen.getByRole('tab', { name: 'Overview' })).toHaveAttribute('aria-controls', tabPanelId('overview'))
    expect(screen.getByRole('tab', { name: 'Models (2)' })).not.toHaveAttribute('aria-controls')
    expect(screen.getByRole('tab', { name: 'History' })).not.toHaveAttribute('aria-controls')
  })
})
