import { describe, expect, it } from 'vitest'
import { ageSeconds, formatAge, formatAgeShort, formatCompact, formatMs, formatPct } from './format'

describe('formatCompact', () => {
  it('abbreviates thousands and millions with one decimal', () => {
    expect(formatCompact(1900)).toBe('1.9k')
    expect(formatCompact(3140)).toBe('3.1k')
    expect(formatCompact(1000)).toBe('1k')
    expect(formatCompact(2_400_000)).toBe('2.4M')
  })
  it('keeps small numbers readable', () => {
    expect(formatCompact(12)).toBe('12')
    expect(formatCompact(42.5)).toBe('42.5')
    expect(formatCompact(999)).toBe('999')
    expect(formatCompact(0)).toBe('0')
  })
  it('renders missing values as a dash', () => {
    expect(formatCompact(null)).toBe('--')
    expect(formatCompact(undefined)).toBe('--')
    expect(formatCompact(Number.NaN)).toBe('--')
  })
})

describe('formatPct', () => {
  it('rounds to a whole percent', () => {
    expect(formatPct(67.4)).toBe('67%')
    expect(formatPct(0)).toBe('0%')
    expect(formatPct(null)).toBe('--')
  })
})

describe('formatMs', () => {
  it('rounds milliseconds', () => {
    expect(formatMs(812.4)).toBe('812 ms')
    expect(formatMs(null)).toBe('--')
  })
})

describe('formatAge', () => {
  it('picks seconds, minutes or hours', () => {
    expect(formatAge(3)).toBe('3s ago')
    expect(formatAge(59)).toBe('59s ago')
    expect(formatAge(130)).toBe('2m ago')
    expect(formatAge(7200)).toBe('2h ago')
    expect(formatAge(-4)).toBe('0s ago')
  })
})

describe('formatAgeShort', () => {
  it('drops the "ago" suffix', () => {
    expect(formatAgeShort(45)).toBe('45s')
    expect(formatAgeShort(130)).toBe('2m')
    expect(formatAgeShort(7200)).toBe('3h'.replace('3', '2'))
  })
})

describe('ageSeconds', () => {
  const now = Date.parse('2026-09-06T12:00:00Z')
  it('measures whole seconds since a timestamp', () => {
    expect(ageSeconds('2026-09-06T11:59:15Z', now)).toBe(45)
    expect(ageSeconds('2026-09-06T12:00:03Z', now)).toBe(0)
  })
  it('is null for missing or unparseable input', () => {
    expect(ageSeconds(null, now)).toBeNull()
    expect(ageSeconds('', now)).toBeNull()
    expect(ageSeconds('garbage', now)).toBeNull()
  })
})
