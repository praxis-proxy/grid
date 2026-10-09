import { describe, expect, it } from 'vitest'
import { makeSite } from '../test/fixtures'
import { filterSites, fleetGpuTotal, rosterOptionId, SORT_OPTIONS, sortSites } from './roster'

const ohio = makeSite({ name: 'ohio', displayName: 'Ohio', region: 'us-east-2', health: 'green', gpus: { total: 64, utilPct: 67 } })
const london = makeSite({ name: 'london', displayName: 'London', region: 'eu-west-2', health: 'yellow', gpus: { total: 16, utilPct: 91 } })
const tokyo = makeSite({ name: 'tokyo', displayName: 'Tokyo', region: 'ap-northeast-1', health: 'green', gpus: { total: 8, utilPct: 95 } })
const lima = makeSite({ name: 'lima', displayName: 'Lima', region: 'sa-east-1', health: 'red', gpus: { total: 4, utilPct: null }, models: [] })
const lab = makeSite({ name: 'aigrid-lab', displayName: 'Lab rack', region: 'unknown-lab', dc: 'onprem', health: 'green', gpus: { total: 2, utilPct: null }, models: [{ name: 'mistral-7b', running: 1 }] })
const all = [ohio, london, tokyo, lima, lab]

describe('sortSites', () => {
  it('lists the sort options in display order with worst first as the default', () => {
    expect(SORT_OPTIONS.map((o) => o.value)).toEqual(['worst', 'utilization', 'name', 'region'])
  })
  it('worst first: down, degraded, healthy; ties by utilization descending, unknown last', () => {
    expect(sortSites(all, 'worst').map((s) => s.name)).toEqual(['lima', 'london', 'tokyo', 'ohio', 'aigrid-lab'])
  })
  it('utilization: highest first, unknown last, ties by name', () => {
    expect(sortSites(all, 'utilization').map((s) => s.name)).toEqual(['tokyo', 'london', 'ohio', 'aigrid-lab', 'lima'])
  })
  it('name and region sort alphabetically', () => {
    expect(sortSites(all, 'name').map((s) => s.name)).toEqual(['aigrid-lab', 'lima', 'london', 'ohio', 'tokyo'])
    expect(sortSites(all, 'region').map((s) => s.region)).toEqual(['ap-northeast-1', 'eu-west-2', 'sa-east-1', 'unknown-lab', 'us-east-2'])
  })
  it('does not mutate the input', () => {
    const input = [ohio, lima]
    sortSites(input, 'worst')
    expect(input.map((s) => s.name)).toEqual(['ohio', 'lima'])
  })
})

describe('filterSites', () => {
  it('matches name, display name, region, dc and model names case-insensitively', () => {
    expect(filterSites(all, 'LONDON', null).map((s) => s.name)).toEqual(['london'])
    expect(filterSites(all, 'eu-west', null).map((s) => s.name)).toEqual(['london'])
    expect(filterSites(all, 'onprem', null).map((s) => s.name)).toEqual(['aigrid-lab'])
    expect(filterSites(all, 'mistral', null).map((s) => s.name)).toEqual(['aigrid-lab'])
    expect(filterSites(all, 'llama', null)).toHaveLength(3)
  })
  it('applies the health filter and ignores surrounding whitespace', () => {
    expect(filterSites(all, '  ', 'red').map((s) => s.name)).toEqual(['lima'])
    expect(filterSites(all, 'o', 'green').map((s) => s.name)).toEqual(['ohio', 'tokyo', 'aigrid-lab'])
  })
  it('returns everything for an empty query and no filter', () => {
    expect(filterSites(all, '', null)).toHaveLength(5)
  })
})

describe('fleetGpuTotal', () => {
  it('sums GPUs across sites', () => {
    expect(fleetGpuTotal(all)).toBe(94)
    expect(fleetGpuTotal([])).toBe(0)
  })
})

describe('rosterOptionId', () => {
  it('prefixes the site name', () => {
    expect(rosterOptionId('ohio')).toBe('roster-option-ohio')
  })
})
