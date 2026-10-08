import { describe, expect, it } from 'vitest'
import { makeSite } from '../test/fixtures'
import { attentionSites } from './attention'

describe('attentionSites', () => {
  it('lists down sites before degraded ones and drops healthy sites', () => {
    const sites = [
      makeSite({ name: 'ok', displayName: 'Ohio', health: 'green' }),
      makeSite({ name: 'y2', displayName: 'Tokyo', health: 'yellow' }),
      makeSite({ name: 'r1', displayName: 'Lima', health: 'red' }),
      makeSite({ name: 'y1', displayName: 'Berlin', health: 'yellow' }),
    ]
    expect(attentionSites(sites).map((s) => s.name)).toEqual(['r1', 'y1', 'y2'])
  })
  it('is empty when every site is healthy', () => {
    expect(attentionSites([makeSite()])).toEqual([])
  })
})
