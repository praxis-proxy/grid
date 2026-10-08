import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import MapLegend from './MapLegend'

describe('MapLegend', () => {
  it('explains ring size, the arc, the three health shapes, the hub and the route line', () => {
    render(<MapLegend />)
    const legend = screen.getByRole('complementary', { name: 'Map legend' })
    expect(legend).toHaveTextContent('ring size = GPUs (4 · 16 · 64)')
    expect(legend).toHaveTextContent('inner arc = GPU utilization')
    expect(screen.getByRole('img', { name: 'healthy' })).toBeInTheDocument()
    expect(screen.getByRole('img', { name: 'degraded' })).toBeInTheDocument()
    expect(screen.getByRole('img', { name: 'down' })).toBeInTheDocument()
    expect(legend).toHaveTextContent('registered route hub → site')
    expect(legend).not.toHaveTextContent(/traffic/i)
    expect(legend.querySelectorAll('circle').length).toBeGreaterThanOrEqual(3)
    expect(legend.querySelector('polygon')).not.toBeNull()
  })
})
