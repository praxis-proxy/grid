import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { FleetSummary } from '../api/types'
import { makeSeries, makeSite } from '../test/fixtures'
import SummaryTiles from './SummaryTiles'

const summary: FleetSummary = {
  gpuTotal: 1900,
  gpuUtilPct: 67.4,
  tokensPerSec: 3140,
  rps: 120.4,
  activeModels: 7,
  activeTenants: 12,
  sitesGreen: 5,
  sitesYellow: 1,
  sitesRed: 1,
}
const sites = [
  makeSite({ name: 'ohio', displayName: 'Ohio', queueDepth: 12, p50LatencyMs: 800, gpus: { total: 64, utilPct: 67 } }),
  makeSite({ name: 'london', displayName: 'London', queueDepth: 60, p50LatencyMs: 1200, gpus: { total: 16, utilPct: 91 }, placed: false, tenants: [{ name: 'platform', sharePct: 100 }] }),
]

const tile = (name: string) => within(screen.getByRole('group', { name }))

describe('SummaryTiles', () => {
  it('fills the eight tiles with values, deltas from the hourly series and context lines', () => {
    render(<SummaryTiles summary={summary} sites={sites} hourly={makeSeries('1h')} />)
    expect(tile('GPUs').getByText('1.9k')).toBeInTheDocument()
    expect(screen.getByRole('group', { name: 'GPUs' })).toHaveTextContent('5')
    expect(screen.getByRole('group', { name: 'GPUs' })).toHaveTextContent('1')
    expect(tile('Utilization').getByText('67%')).toBeInTheDocument()
    expect(tile('Utilization').getByText('+6 pts')).toBeInTheDocument()
    expect(tile('Utilization').getByText('vs 1h ago')).toBeInTheDocument()
    expect(tile('Tokens/s').getByText('3.1k')).toBeInTheDocument()
    expect(tile('Tokens/s').getByText('+5%')).toBeInTheDocument()
    expect(tile('Requests/s').getByText('120')).toBeInTheDocument()
    expect(tile('Requests/s').getByText('fleet p50 880 ms')).toBeInTheDocument()
    expect(tile('Queue depth').getByText('72')).toBeInTheDocument()
    expect(tile('Queue depth').getByText('top London · 60')).toBeInTheDocument()
    expect(tile('Models').getByText('7')).toBeInTheDocument()
    expect(tile('Models').getByText('12 running')).toBeInTheDocument()
    expect(tile('Tenants').getByText('12')).toBeInTheDocument()
    expect(tile('Tenants').getByText('leader platform')).toBeInTheDocument()
    expect(tile('Sites').getByText('2')).toBeInTheDocument()
    expect(tile('Sites').getByText('1 placed · 1 unplaced')).toBeInTheDocument()
    expect(screen.getAllByRole('group')).toHaveLength(8)
  })
  it('shows dashes before the first snapshot and no deltas without the hourly series', () => {
    render(<SummaryTiles summary={null} sites={[]} hourly={null} />)
    expect(tile('GPUs').getByText('--')).toBeInTheDocument()
    expect(tile('Utilization').getByText('--')).toBeInTheDocument()
    expect(tile('Utilization').getByText('GPU-weighted mean')).toBeInTheDocument()
    expect(tile('Sites').getByText('--')).toBeInTheDocument()
    expect(screen.queryByText('vs 1h ago')).not.toBeInTheDocument()
  })
  it('renders eight skeleton tiles while loading', () => {
    render(<SummaryTiles summary={null} sites={[]} hourly={null} loading />)
    expect(screen.getByRole('list', { name: 'Loading fleet summary' })).toHaveAttribute('aria-busy', 'true')
    expect(screen.getAllByRole('listitem')).toHaveLength(8)
    expect(screen.queryAllByRole('group')).toHaveLength(0)
  })
})
