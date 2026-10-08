import { render } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import TenantBar from './TenantBar'

function makeTenants(n: number) {
  return Array.from({ length: n }, (_, i) => ({ name: `tenant-${i}`, sharePct: 100 / n }))
}

describe('TenantBar', () => {
  it('cycles through the three accent shades, then repeats them at reduced opacity', () => {
    const { container } = render(<TenantBar tenants={makeTenants(5)} />)
    const swatches = Array.from(container.querySelectorAll('li span[aria-hidden]')) as HTMLElement[]
    expect(swatches).toHaveLength(5)
    expect(swatches[0]).toHaveStyle({ background: '#2fc4d1' })
    expect(swatches[1]).toHaveStyle({ background: '#1d8a94' })
    expect(swatches[2]).toHaveStyle({ background: '#155f66' })
    // The fourth and fifth tenants repeat the first two shades, at reduced opacity.
    expect(swatches[3]).toHaveStyle({ background: '#2fc4d1' })
    expect(swatches[4]).toHaveStyle({ background: '#1d8a94' })
    const fullOpacity = Number(swatches[0].style.opacity || '1')
    const repeatOpacity = Number(swatches[3].style.opacity || '1')
    expect(repeatOpacity).toBeLessThan(fullOpacity)
    expect(Number(swatches[4].style.opacity || '1')).toBe(repeatOpacity)
  })

  it('shows the same share bar segment colors as the legend swatches', () => {
    const { container } = render(<TenantBar tenants={makeTenants(2)} />)
    const bar = container.querySelector('[role="img"]') as HTMLElement
    const segments = Array.from(bar.children) as HTMLElement[]
    const swatches = Array.from(container.querySelectorAll('li span[aria-hidden]')) as HTMLElement[]
    expect(segments[0].style.background).toBe(swatches[0].style.background)
    expect(segments[1].style.background).toBe(swatches[1].style.background)
  })
})
