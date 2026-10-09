import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeSite, makeSiteDetail, testConfig } from '../test/fixtures'
import { stubFetchRoutes } from '../test/http'
import SitePanel from './SitePanel'

const site = makeSite({ health: 'yellow', reasons: ['queue depth 60 >= 50'], queueDepth: 60 })
const thresholds = testConfig.thresholds

function renderPanel(props: Partial<React.ComponentProps<typeof SitePanel>> = {}) {
  const onClose = vi.fn()
  render(<SitePanel site={site} onClose={onClose} thresholds={thresholds} {...props} />)
  return { onClose }
}

describe('SitePanel', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    vi.setSystemTime(new Date('2026-09-06T12:00:45Z'))
    // Block body on purpose: a function returned from beforeEach is treated as a cleanup hook by vitest.
    stubFetchRoutes({ '/api/v1/sites/': makeSiteDetail() })
  })
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
  })

  it('is a dialog named by the site, with badge, meta line, reasons and threshold-labelled tiles', async () => {
    renderPanel()
    const dialog = screen.getByRole('dialog', { name: 'Ohio' })
    expect(dialog).toBeInTheDocument()
    expect(within(dialog).getByText('Degraded')).toBeInTheDocument()
    expect(screen.getByText('us-east-2 · aws-us-east-2 · updated 45s ago')).toHaveAttribute('title')
    expect(screen.getByText('queue depth 60 >= 50').closest('ul')).toHaveClass('text-degraded')

    const util = screen.getByRole('group', { name: 'GPU util' })
    expect(util).toHaveTextContent('67%')
    expect(util).toHaveTextContent('warn at 90%')
    expect(util).not.toHaveAttribute('data-warn')
    const queue = screen.getByRole('group', { name: 'Queue' })
    expect(queue).toHaveTextContent('60')
    expect(queue).toHaveTextContent('warn at 50')
    expect(queue).toHaveAttribute('data-warn', 'true')
    expect(screen.getByRole('group', { name: 'Requests/s' })).toHaveTextContent('p50 812 ms')
    expect(screen.getByRole('group', { name: 'Tokens/s' })).toHaveTextContent('64 GPUs')

    expect(screen.getByText('Loading 20 minute history')).toBeInTheDocument()
    expect(await screen.findByRole('tab', { name: 'Models (1)' })).toBeInTheDocument()
    expect(screen.queryByText('Loading 20 minute history')).not.toBeInTheDocument()
  })

  it('shows dashes on every tile for a down site', () => {
    renderPanel({ site: makeSite({ health: 'red', reasons: ['metrics unreachable'], gpus: { total: 64, utilPct: 95 }, queueDepth: 99 }) })
    expect(screen.getByRole('group', { name: 'GPU util' })).toHaveTextContent('—')
    expect(screen.getByRole('group', { name: 'Queue' })).not.toHaveAttribute('data-warn')
    expect(screen.getByRole('group', { name: 'Requests/s' })).toHaveTextContent('p50 —')
    expect(screen.getByText('metrics unreachable').closest('ul')).toHaveClass('text-down')
  })

  it('switches between the models, tenants and history tabs', async () => {
    renderPanel()
    fireEvent.click(screen.getByRole('tab', { name: 'Models (1)' }))
    expect(screen.getByRole('cell', { name: 'llama-3.1-70b' })).toBeInTheDocument()
    expect(screen.getByRole('img', { name: '6 of 6' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('tab', { name: 'Tenants (2)' }))
    expect(screen.getByRole('img', { name: 'research 60%, platform 40%' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('tab', { name: 'History' }))
    expect(await screen.findByText('Last 20 minutes · step 30 s')).toBeInTheDocument()
    expect(screen.getByText('min 60% · max 67%')).toBeInTheDocument()
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', 'tab-history')
  })

  it('keeps the last error collapsed in a monospace block and copies the address', async () => {
    const writeText = vi.fn(() => Promise.resolve())
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
    renderPanel({ site: makeSite({ lastError: 'dial tcp: i/o timeout' }) })
    const details = screen.getByText('Last error').closest('details')
    expect(details).not.toHaveAttribute('open')
    expect(screen.getByText('dial tcp: i/o timeout')).toHaveClass('font-mono')
    fireEvent.click(screen.getByRole('button', { name: /Copy address/ }))
    expect(writeText).toHaveBeenCalledWith('gateway.apps.aigrid-ds-spoke1.example.internal')
    await waitFor(() => expect(screen.getByRole('button', { name: /Copy address/ })).toHaveTextContent('Copied'))
  })

  it('closes on Escape and on the close button', () => {
    const { onClose } = renderPanel()
    fireEvent.keyDown(window, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Close panel' }))
    expect(onClose).toHaveBeenCalledTimes(2)
  })

  it('shows the detail fetch error', async () => {
    stubFetchRoutes({})
    renderPanel()
    expect(await screen.findByText('not found')).toBeInTheDocument()
  })

  it('handles sites with no models or tenants', () => {
    renderPanel({ site: makeSite({ models: [], tenants: [] }) })
    fireEvent.click(screen.getByRole('tab', { name: 'Models (0)' }))
    expect(screen.getByText('No models reporting')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('tab', { name: 'Tenants (0)' }))
    expect(screen.getByText('No tenant breakdown configured')).toBeInTheDocument()
  })

  it('includes the last-error summary in the focus trap when there is no address to copy', () => {
    renderPanel({ site: makeSite({ lastError: 'boom', address: '' }) })
    const summary = screen.getByText('Last error')
    summary.focus()
    expect(document.activeElement).toBe(summary)
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab' })
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Close panel' }))
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab', shiftKey: true })
    expect(document.activeElement).toBe(summary)
  })

  it('focuses the close button on mount, traps Tab inside, and restores focus to the trigger on unmount', () => {
    // Copy address is only focusable (and in the trap) when the page can copy at all.
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText: vi.fn() } })
    const trigger = document.createElement('button')
    document.body.appendChild(trigger)
    trigger.focus()

    const { unmount } = render(<SitePanel site={site} onClose={() => {}} thresholds={thresholds} />)
    const close = screen.getByRole('button', { name: 'Close panel' })
    expect(document.activeElement).toBe(close)
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab', shiftKey: true })
    expect(document.activeElement).toBe(screen.getByRole('button', { name: /Copy address/ }))
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab' })
    expect(document.activeElement).toBe(close)

    unmount()
    expect(document.activeElement).toBe(trigger)
    trigger.remove()
  })
})
