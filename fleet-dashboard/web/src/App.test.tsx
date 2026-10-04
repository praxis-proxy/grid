import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Site } from './api/types'
import { NARROW_SCREEN_QUERY } from './hooks/useMediaQuery'
import { FakeEventSource } from './test/fakeEventSource'
import { makeSeries, makeSite, makeSiteDetail, makeSnapshot, testConfig } from './test/fixtures'
import { jsonResponse, stubFetchRoutes } from './test/http'

function stubMatchMedia(matchesNarrow: boolean): void {
  vi.stubGlobal(
    'matchMedia',
    vi.fn((query: string) => ({
      matches: query === NARROW_SCREEN_QUERY ? matchesNarrow : false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  )
}

// Leaflet cannot lay out in jsdom; FleetMap has its own tests with a mocked Leaflet.
// The map tools toolbar is reproduced here (matching MapTools' own roles/names) so
// document-order assertions against the real "Links" toolbar stay meaningful (F12).
vi.mock('./components/FleetMap', () => ({
  default: ({ sites, selected, onSelect }: { sites: Site[]; selected: string | null; onSelect: (name: string, source: 'map') => void }) => (
    <div data-testid="fleet-map" data-sites={sites.length} data-selected={selected ?? ''}>
      {sites.map((site) => (
        <button key={site.name} type="button" onClick={() => onSelect(site.name, 'map')}>
          {`marker ${site.name}`}
        </button>
      ))}
      <div role="toolbar" aria-label="Map tools">
        <div role="group" aria-label="Links">
          <button type="button">selected</button>
        </div>
      </div>
    </div>
  ),
}))

import App from './App'

const unplaced = makeSite({
  name: 'aigrid-lab',
  displayName: 'Lab rack',
  lat: null,
  lng: null,
  placed: false,
  gpus: { total: 2, utilPct: 20 },
})

describe('App', () => {
  beforeEach(() => {
    FakeEventSource.reset()
    vi.stubGlobal('EventSource', FakeEventSource)
    stubFetchRoutes({
      '/api/v1/config': testConfig,
      '/api/v1/fleet': makeSnapshot({ sites: [makeSite(), unplaced] }),
      '/api/v1/sites/': makeSiteDetail(),
      '/api/v1/series': makeSeries('1h'),
    })
  })
  afterEach(() => {
    vi.unstubAllGlobals()
    window.history.replaceState(null, '', '/')
  })

  it('renders the product name, skeletons and a connecting overlay before data arrives', () => {
    render(<App />)
    expect(screen.getByText('AI GRID FLEET')).toBeInTheDocument()
    expect(screen.getByText('Connecting to hub')).toBeInTheDocument()
    expect(screen.getByRole('list', { name: 'Loading sites' })).toBeInTheDocument()
    expect(screen.getByRole('list', { name: 'Loading fleet summary' })).toBeInTheDocument()
  })

  it('shows the hub, passes placed sites to the map and lists every site in the roster', async () => {
    render(<App />)
    expect(await screen.findByText('aigrid-ds-hub')).toBeInTheDocument()
    const lab = await screen.findByRole('option', { name: /Lab rack/ })
    expect(lab).toHaveTextContent('no position')
    expect(screen.getByRole('option', { name: /Ohio/ })).not.toHaveTextContent('no position')
    expect(screen.getByTestId('fleet-map')).toHaveAttribute('data-sites', '1')
    expect(screen.queryByText('Connecting to hub')).not.toBeInTheDocument()
    expect(screen.getByText('2 sites · 66 GPUs · polled every 5 s')).toBeInTheDocument()
  })

  it('filters the roster from the status pill and selects from the roster', async () => {
    render(<App />)
    await screen.findByRole('option', { name: /Lab rack/ })
    fireEvent.click(screen.getByRole('button', { name: '1 healthy' }))
    expect(screen.getAllByRole('option')).toHaveLength(2)
    fireEvent.click(screen.getByRole('option', { name: /Ohio/ }))
    expect(await screen.findByRole('heading', { name: 'Ohio' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: /Ohio/ })).toHaveAttribute('aria-selected', 'true')
  })

  it('reports a failed initial fetch in plain words and recovers on Retry', async () => {
    stubFetchRoutes({ '/api/v1/config': testConfig })
    render(<App />)
    expect(await screen.findByText('Fleet data unavailable')).toBeInTheDocument()
    expect(screen.getByText('not found')).toBeInTheDocument()
    stubFetchRoutes({ '/api/v1/config': testConfig, '/api/v1/fleet': makeSnapshot(), '/api/v1/series': makeSeries('1h') })
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('option', { name: /Ohio/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
  })

  it('shows the register command when the fleet is empty', async () => {
    stubFetchRoutes({ '/api/v1/config': testConfig, '/api/v1/fleet': makeSnapshot({ sites: [] }), '/api/v1/series': makeSeries('1h') })
    render(<App />)
    expect(await screen.findByText('No sites registered yet')).toBeInTheDocument()
    expect(screen.getByText(/hack\/register-site.sh/)).toBeInTheDocument()
  })

  it('shows the stale banner and desaturates the strip once the stream goes quiet', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<App />)
      await screen.findByRole('option', { name: /Ohio/ })
      act(() => FakeEventSource.instances[0].emitOpen())
      expect(screen.queryByRole('alert')).not.toBeInTheDocument()
      act(() => {
        vi.advanceTimersByTime(3 * testConfig.pollIntervalSeconds * 1000 + 1000)
      })
      expect(screen.getByRole('alert')).toHaveTextContent(/^Data is stale$/)
      expect(screen.getByText(/, last update \d+s ago/)).toBeInTheDocument()
      expect(screen.getByRole('contentinfo', { name: 'Fleet summary' })).toHaveAttribute('data-stale', 'true')
    } finally {
      vi.useRealTimers()
    }
  })

  it('shows a waiting message, not an error, for a 503 before the first poll', async () => {
    stubFetchRoutes({
      '/api/v1/config': testConfig,
      '/api/v1/fleet': jsonResponse({ error: 'collector not ready' }, 503),
    })
    render(<App />)
    expect(await screen.findByText('Waiting for the first poll')).toBeInTheDocument()
    expect(screen.queryByText(/Fleet data unavailable/)).not.toBeInTheDocument()
    expect(screen.getByRole('list', { name: 'Loading sites' })).toBeInTheDocument()
  })

  it('opens the site panel from a ?site= deep link and closes it with Escape', async () => {
    window.history.replaceState(null, '', '/?site=aigrid-ds-spoke1')
    render(<App />)
    expect(await screen.findByRole('dialog', { name: 'Ohio' })).toBeInTheDocument()
    // The panel's Escape listener and its focus move are passive effects of the
    // same commit; waiting for focus proves both have run before Escape fires.
    await waitFor(() => expect(screen.getByRole('button', { name: 'Close panel' })).toHaveFocus())
    fireEvent.keyDown(window, { key: 'Escape' })
    expect(screen.queryByRole('heading', { name: 'Ohio' })).not.toBeInTheDocument()
    expect(window.location.search).toBe('')
  })

  it('shows an attention chip for an unhealthy site that selects it', async () => {
    stubFetchRoutes({
      '/api/v1/config': testConfig,
      '/api/v1/fleet': makeSnapshot({ sites: [makeSite({ health: 'yellow', reasons: ['queue depth 60 >= 50'] })] }),
      '/api/v1/sites/': makeSiteDetail(),
      '/api/v1/series': makeSeries('1h'),
    })
    render(<App />)
    const chip = await screen.findByRole('button', { name: /Ohio/ })
    expect(chip).toHaveTextContent('queue depth 60 >= 50')
    fireEvent.click(chip)
    expect(await screen.findByRole('heading', { name: 'Ohio' })).toBeInTheDocument()
    expect(window.location.search).toBe('?site=aigrid-ds-spoke1')
  })

  it('places the attention strip before the map tools in document order, so Tab reaches it first', async () => {
    stubFetchRoutes({
      '/api/v1/config': testConfig,
      '/api/v1/fleet': makeSnapshot({ sites: [makeSite({ health: 'yellow', reasons: ['queue depth 60 >= 50'] })] }),
      '/api/v1/sites/': makeSiteDetail(),
      '/api/v1/series': makeSeries('1h'),
    })
    render(<App />)
    const chip = await screen.findByRole('button', { name: /Ohio/ })
    const links = screen.getByRole('group', { name: 'Links' })
    expect(chip.compareDocumentPosition(links) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })

  it('keeps the roster and the map in sync in both directions', async () => {
    render(<App />)
    await screen.findByRole('option', { name: /Ohio/ })
    fireEvent.click(screen.getByRole('button', { name: 'marker aigrid-ds-spoke1' }))
    expect(screen.getByRole('option', { name: /Ohio/ })).toHaveAttribute('aria-selected', 'true')
    expect(await screen.findByRole('heading', { name: 'Ohio' })).toBeInTheDocument()
    expect(screen.getByRole('main').style.gridTemplateColumns).toBe('280px minmax(0,1fr) 340px')
    fireEvent.click(screen.getByRole('button', { name: 'Close panel' }))
    expect(screen.getByRole('option', { name: /Ohio/ })).toHaveAttribute('aria-selected', 'false')
    expect(screen.getByRole('main').style.gridTemplateColumns).toBe('280px minmax(0,1fr)')
    fireEvent.click(screen.getByRole('option', { name: /Ohio/ }))
    expect(screen.getByTestId('fleet-map')).toHaveAttribute('data-selected', 'aigrid-ds-spoke1')
  })

  it('collapses the roster from its toggle', async () => {
    render(<App />)
    await screen.findByRole('option', { name: /Ohio/ })
    fireEvent.click(screen.getByRole('button', { name: 'Collapse roster' }))
    expect(screen.getByRole('main').style.gridTemplateColumns).toBe('56px minmax(0,1fr)')
    expect(screen.getByRole('button', { name: 'Ohio' })).toBeInTheDocument()
  })

  it('force-collapses the roster and disables its toggle under a narrow viewport', async () => {
    stubMatchMedia(true)
    render(<App />)
    // Wait for the snapshot so no overlay covers the roster.
    await screen.findByRole('button', { name: 'Ohio' })
    expect(screen.getByRole('main').style.gridTemplateColumns).toBe('56px minmax(0,1fr)')
    expect(screen.getByRole('button', { name: 'Expand roster' })).toBeDisabled()
  })

  it('leaves the roster toggle enabled and working on a wide viewport', async () => {
    stubMatchMedia(false)
    render(<App />)
    await screen.findByRole('option', { name: /Ohio/ })
    const toggle = screen.getByRole('button', { name: 'Collapse roster' })
    expect(toggle).not.toBeDisabled()
    fireEvent.click(toggle)
    expect(screen.getByRole('main').style.gridTemplateColumns).toBe('56px minmax(0,1fr)')
  })

  it('fills the summary tiles from the snapshot', async () => {
    render(<App />)
    await screen.findByText('aigrid-ds-hub')
    expect(within(screen.getByRole('group', { name: 'GPUs' })).getByText('64')).toBeInTheDocument()
    expect(within(screen.getByRole('group', { name: 'Utilization' })).getByText('67%')).toBeInTheDocument()
    expect(within(screen.getByRole('group', { name: 'Sites' })).getByText('1 placed · 1 unplaced')).toBeInTheDocument()
  })
})
