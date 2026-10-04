import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import HealthShape from './HealthShape'

describe('HealthShape', () => {
  it('draws a green circle with the health label as its accessible name', () => {
    const { container } = render(<HealthShape health="green" />)
    expect(screen.getByRole('img', { name: 'Healthy' })).toBeInTheDocument()
    expect(container.querySelector('circle')).toHaveAttribute('fill', '#38b26a')
  })
  it('draws a triangle for degraded and a square for down', () => {
    const { container } = render(
      <>
        <HealthShape health="yellow" size={12} />
        <HealthShape health="red" size={12} />
      </>,
    )
    const polygons = container.querySelectorAll('polygon')
    expect(polygons).toHaveLength(2)
    expect(polygons[0]).toHaveAttribute('points', '6,0 12,12 0,12')
    expect(polygons[1]).toHaveAttribute('fill', '#de4b3f')
    expect(screen.getByRole('img', { name: 'Degraded' })).toHaveAttribute('width', '12')
  })
  it('accepts a custom label and can be decorative', () => {
    render(<HealthShape health="red" label="Ohio down" />)
    expect(screen.getByRole('img', { name: 'Ohio down' })).toBeInTheDocument()
    const { container } = render(<HealthShape health="red" label="" />)
    expect(container.querySelector('svg')).toHaveAttribute('aria-hidden', 'true')
    expect(container.querySelector('svg')).not.toHaveAttribute('role')
  })
})
