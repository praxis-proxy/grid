import { render, screen } from '@testing-library/react'
import { cloneElement, type ReactElement } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { makeSeries } from '../test/fixtures'

// The full recharts render gives ResponsiveContainer a 0x0 box in jsdom (see FleetCharts.test.tsx),
// so the threshold ReferenceLine's props are otherwise unobservable: with no measured size, recharts
// never lays out its children, real ReferenceLine included. This file mocks ResponsiveContainer to
// clone its chart child with an explicit width/height (mirroring what it does via ResizeObserver in a
// real browser) and mocks ReferenceLine as a probe, keeping every other recharts export real, to cover
// the "warning threshold drawn as a dashed amber line" requirement without disturbing the other
// FleetCharts tests, which render the genuine chart tree.
vi.mock('recharts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('recharts')>()
  return {
    ...actual,
    ResponsiveContainer: ({ children }: { children: ReactElement<{ width?: number; height?: number }> }) =>
      cloneElement(children, { width: 800, height: 200 }),
    ReferenceLine: (props: { y?: number; stroke?: string; strokeDasharray?: string }) => (
      <div data-testid="reference-line" data-y={props.y} data-stroke={props.stroke} data-dash={props.strokeDasharray} />
    ),
  }
})

const { default: FleetCharts } = await import('./FleetCharts')

describe('FleetCharts threshold line', () => {
  it('draws the GPU utilization warning threshold as a dashed amber reference line', () => {
    render(<FleetCharts series={makeSeries()} range="1h" error={null} onRangeChange={() => {}} gpuUtilWarn={90} />)
    const line = screen.getByTestId('reference-line')
    expect(line).toHaveAttribute('data-y', '90')
    expect(line.getAttribute('data-dash')).toBeTruthy()
    expect(line).toHaveAttribute('data-stroke', '#e0a21c')
  })
})
