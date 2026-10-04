import { render } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { makeSeries } from '../../test/fixtures'
import HistoryTab from './HistoryTab'

describe('HistoryTab', () => {
  it('draws each sparkline in its palette color, matching FleetCharts: accent GPU util, amber tokens, ink-2 queue', () => {
    const { container } = render(<HistoryTab series={makeSeries()} loading={false} error={null} />)
    const lines = container.querySelectorAll('.recharts-line-curve')
    expect(lines).toHaveLength(3)
    expect(lines[0]).toHaveAttribute('stroke', '#2fc4d1')
    expect(lines[1]).toHaveAttribute('stroke', '#8a99a6')
    expect(lines[2]).toHaveAttribute('stroke', '#e0a21c')
  })
})
