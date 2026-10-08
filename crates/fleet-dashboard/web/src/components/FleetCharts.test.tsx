import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { makeSeries } from '../test/fixtures'
import FleetCharts from './FleetCharts'

// jsdom gives ResponsiveContainer a 0x0 box, so the SVG itself is not asserted here; the data
// mapping is covered by lib/chartData.test.ts and the formatting by lib/chartFormat.test.ts.
describe('FleetCharts', () => {
  it('renders the header with the step, both chart slots and a 40 px range control', () => {
    const onRangeChange = vi.fn()
    render(<FleetCharts series={makeSeries()} range="1h" error={null} onRangeChange={onRangeChange} gpuUtilWarn={90} />)
    expect(screen.getByRole('region', { name: 'Fleet over time' })).toHaveTextContent('Fleet over time · step 30 s')
    expect(screen.getByRole('img', { name: 'GPU utilization %' })).toBeInTheDocument()
    expect(screen.getByRole('img', { name: 'Tokens / s and queue depth' })).toBeInTheDocument()
    const oneHour = screen.getByRole('button', { name: '1h' })
    expect(oneHour).toHaveAttribute('aria-pressed', 'true')
    expect(oneHour.className).toContain('h-10')
    fireEvent.click(screen.getByRole('button', { name: '6h' }))
    expect(onRangeChange).toHaveBeenCalledWith('6h')
  })
  it('renders without data', () => {
    render(<FleetCharts series={null} range="24h" error={null} onRangeChange={() => {}} gpuUtilWarn={90} />)
    expect(screen.getByText('No series data yet')).toBeInTheDocument()
    expect(screen.getByRole('region', { name: 'Fleet over time' })).toHaveTextContent(/^Fleet over time/)
  })
  it('shows the fetch error instead of the empty-state message when present', () => {
    render(<FleetCharts series={null} range="24h" error="not found" onRangeChange={() => {}} gpuUtilWarn={90} />)
    expect(screen.getByText('Series unavailable: not found')).toBeInTheDocument()
    expect(screen.queryByText('No series data yet')).not.toBeInTheDocument()
  })
  it('is memoized so an unrelated parent re-render does not force it to re-render', () => {
    expect(FleetCharts.$$typeof).toBe(Symbol.for('react.memo'))
  })
})
