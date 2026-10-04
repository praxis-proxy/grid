import { describe, expect, it } from 'vitest'
import {
  arcPath,
  clusterIcon,
  clusterRadius,
  COUNT_MIN_RADIUS,
  escapeHtml,
  gaugeRadius,
  glyphLabel,
  hexagonPoints,
  hubGlyph,
  markerLabel,
  nodeGlyph,
  ringRadius,
  worstHealth,
} from './glyph'

const now = Date.parse('2026-09-06T12:00:45Z')
const opts = { selected: false, labelled: false, now }

describe('ringRadius', () => {
  it('scales with sqrt of GPU count and clamps to [12, 34]', () => {
    expect(ringRadius(0)).toBe(12)
    expect(ringRadius(1)).toBe(17)
    expect(ringRadius(4)).toBe(22)
    expect(ringRadius(16)).toBe(32)
    expect(ringRadius(64)).toBe(34)
    expect(ringRadius(-3)).toBe(12)
    expect(ringRadius(Number.NaN)).toBe(12)
  })
  it('places the gauge at 62 percent of the ring', () => {
    expect(gaugeRadius(32)).toBe(20)
    expect(gaugeRadius(12)).toBe(7)
    expect(COUNT_MIN_RADIUS).toBe(24)
  })
})

describe('arcPath', () => {
  it('is empty at zero and a closed pair of arcs at one', () => {
    expect(arcPath(10, 10, 5, 0)).toBe('')
    expect(arcPath(10, 10, 5, -1)).toBe('')
    expect(arcPath(10, 10, 5, 1)).toBe('M 10 5 A 5 5 0 1 1 10 15 A 5 5 0 1 1 10 5')
    expect(arcPath(10, 10, 5, 1.7)).toBe(arcPath(10, 10, 5, 1))
  })
  it('sweeps clockwise from 12 o clock with the large-arc flag past half', () => {
    expect(arcPath(10, 10, 5, 0.25)).toBe('M 10 5 A 5 5 0 0 1 15.00 10.00')
    expect(arcPath(10, 10, 5, 0.75)).toBe('M 10 5 A 5 5 0 1 1 5.00 10.00')
  })
})

describe('hexagonPoints', () => {
  it('returns six vertices', () => {
    expect(hexagonPoints(14, 14, 10).split(' ')).toHaveLength(6)
  })
})

describe('escapeHtml', () => {
  it('escapes markup characters', () => {
    expect(escapeHtml('<b>&"\'')).toBe('&lt;b&gt;&amp;&quot;&#39;')
  })
})

describe('glyphLabel', () => {
  const site = { name: 'spoke1', displayName: 'Ohio', health: 'green' as const, gpus: { total: 16, utilPct: 50 }, lastSeen: '2026-09-06T12:00:00Z' }
  it('shows GPU count and utilization for reachable sites', () => {
    expect(glyphLabel(site, now)).toEqual(['Ohio', '16 GPU · 50%'])
  })
  it('shows the outage and age for down sites', () => {
    expect(glyphLabel({ ...site, health: 'red' }, now)).toEqual(['Ohio', 'unreachable · last seen 45s'])
    expect(glyphLabel({ ...site, health: 'red', lastSeen: null }, now)).toEqual(['Ohio', 'unreachable · never seen'])
  })
  it('falls back to the name when displayName is empty', () => {
    expect(glyphLabel({ ...site, displayName: '' }, now)[0]).toBe('spoke1')
  })
})

describe('nodeGlyph', () => {
  const site = { name: 'aigrid-ds-spoke1', displayName: 'Ohio', health: 'yellow' as const, gpus: { total: 16, utilPct: 50 }, lastSeen: '2026-09-06T12:00:00Z' }

  it('draws a 3 px health ring, a gauge track, a half arc and the count for a large ring', () => {
    const g = nodeGlyph(site, opts)
    expect(g.size).toBe(84)
    expect(g.html).toContain('r="32" fill="#0b0f12" fill-opacity="0.78" stroke="#e0a21c" stroke-width="3"')
    expect(g.html).toContain('r="20" fill="none" stroke="#2c3944" stroke-width="3"')
    expect(g.html).toContain('<path d="M 42 22 A 20 20 0 0 1 42.00 62.00"')
    expect(g.html).toContain('>16</text>')
    expect(g.html).not.toContain('fleet-node__halo')
    expect(g.className).toBe('fleet-node fleet-node--yellow')
  })

  it('omits the count below 24 px and the arc when utilization is unknown', () => {
    const g = nodeGlyph({ ...site, gpus: { total: 4, utilPct: null } }, opts)
    expect(g.html).not.toContain('<text')
    expect(g.html).not.toContain('<path')
    expect(g.html).toContain('r="14" fill="none" stroke="#2c3944"')
  })

  it('draws a triangle mark at the top of the ring for degraded sites, but not for healthy ones', () => {
    const g = nodeGlyph(site, opts)
    expect(g.html).toContain('<polygon')
    expect(nodeGlyph({ ...site, health: 'green' }, opts).html).not.toContain('<polygon')
  })

  it('draws down sites with a halo, a square mark and no gauge', () => {
    const g = nodeGlyph({ ...site, health: 'red' }, opts)
    expect(g.html).toContain('class="fleet-node__halo"')
    expect(g.html).toContain('<polygon points="33,33 51,33 51,51 33,51" fill="#de4b3f"/>')
    expect(g.html).not.toContain('<path')
    expect(g.html).not.toContain('stroke="#2c3944"')
    expect(g.html).toContain('<span>unreachable · last seen 45s</span>')
  })

  it('adds an accent ring 7 px outside when selected, and the labelled class when labelled', () => {
    const g = nodeGlyph(site, { ...opts, selected: true, labelled: true })
    expect(g.html).toContain('r="39" fill="none" stroke="#2fc4d1" stroke-width="2"')
    expect(g.className).toBe('fleet-node fleet-node--yellow fleet-node--selected fleet-node--labelled')
  })

  it('escapes names in the label', () => {
    const g = nodeGlyph({ ...site, displayName: '', name: '<x>' }, opts)
    expect(g.html).toContain('<span>&lt;x&gt;</span>')
  })
})

describe('markerLabel', () => {
  it('reads name, health, GPUs, utilization and reasons', () => {
    expect(
      markerLabel({ name: 'fra', displayName: 'Frankfurt', health: 'yellow', gpus: { total: 12, utilPct: 95.2 }, reasons: ['GPU utilization 95% >= 90%'] }),
    ).toBe('Frankfurt, degraded, 12 GPUs, 95 percent utilization, GPU utilization 95% >= 90%')
  })
  it('says utilization is unknown when null', () => {
    expect(markerLabel({ name: 'x', displayName: '', health: 'red', gpus: { total: 4, utilPct: null }, reasons: [] })).toBe(
      'x, down, 4 GPUs, utilization unknown',
    )
  })
})

describe('hubGlyph', () => {
  it('draws an accent hexagon with a permanent hub label', () => {
    const g = hubGlyph('aigrid-ds-hub')
    expect(g.html).toContain('<polygon')
    expect(g.html).toContain('stroke="#2fc4d1"')
    expect(g.html).toContain('<span>aigrid-ds-hub</span><span>hub</span>')
    expect(g.className).toBe('fleet-node fleet-node--hub fleet-node--labelled')
    expect(g.size).toBe(28)
  })
})

describe('clusters', () => {
  it('picks the worst member health', () => {
    expect(worstHealth(['green', 'yellow', 'green'])).toBe('yellow')
    expect(worstHealth(['yellow', 'red'])).toBe('red')
    expect(worstHealth([])).toBe('green')
  })
  it('clusters within twice the largest ring radius of the fleet', () => {
    expect(clusterRadius([])).toBe(24)
    expect(clusterRadius([4, 64])).toBe(68)
    expect(clusterRadius([1])).toBe(34)
  })
  it('draws a 22 px ring in the worst health with the count and the word sites', () => {
    const g = clusterIcon(['green', 'red', 'yellow'])
    expect(g.size).toBe(52)
    expect(g.html).toContain('r="22" fill="#12181d" fill-opacity="0.95" stroke="#de4b3f" stroke-width="3"')
    expect(g.html).toContain('>3</text>')
    expect(g.html).toContain('>sites</text>')
    expect(g.className).toBe('fleet-cluster fleet-cluster--red')
    expect(g.html).not.toContain('fleet-node__label')
  })
  it('draws the worst-health shape mark, but none when every member is healthy', () => {
    expect(clusterIcon(['green', 'green']).html).not.toContain('<polygon')
    expect(clusterIcon(['green', 'yellow']).html).toContain('<polygon')
    expect(clusterIcon(['yellow', 'red']).html).toContain('<polygon')
  })
})
