import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { makeSeries, makeSite, makeSnapshot } from '../test/fixtures'
import SummaryStrip from './SummaryStrip'

describe('SummaryStrip', () => {
  it('lays out the eight tiles beside the chart', () => {
    render(
      <SummaryStrip
        summary={makeSnapshot().summary}
        sites={[makeSite()]}
        hourly={makeSeries('1h')}
        series={makeSeries('6h')}
        seriesError={null}
        range="6h"
        onRangeChange={() => {}}
        gpuUtilWarn={90}
      />,
    )
    expect(screen.getAllByRole('group').filter((el) => el.closest('footer')).length).toBeGreaterThanOrEqual(8)
    expect(within(screen.getByRole('group', { name: 'GPUs' })).getByText('64')).toBeInTheDocument()
    expect(screen.getByRole('region', { name: 'Fleet over time' })).toHaveTextContent('step 30 s')
  })
  it('marks the strip stale so it desaturates', () => {
    render(<SummaryStrip summary={null} sites={[]} hourly={null} series={null} seriesError={null} range="1h" onRangeChange={() => {}} gpuUtilWarn={90} stale />)
    expect(screen.getByRole('contentinfo', { name: 'Fleet summary' })).toHaveAttribute('data-stale', 'true')
  })
  it('shows skeleton tiles while loading', () => {
    render(<SummaryStrip summary={null} sites={[]} hourly={null} series={null} seriesError={null} range="1h" onRangeChange={() => {}} gpuUtilWarn={90} loading />)
    expect(screen.getByRole('list', { name: 'Loading fleet summary' })).toBeInTheDocument()
  })
})
