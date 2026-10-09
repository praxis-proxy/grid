function trim(v: number): string {
  return v.toFixed(1).replace(/\.0$/, '')
}

/** 1900 -> "1.9k", 2400000 -> "2.4M", 12 -> "12", null -> "--". */
export function formatCompact(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return '--'
  const abs = Math.abs(n)
  if (abs >= 1_000_000) return `${trim(n / 1_000_000)}M`
  if (abs >= 1_000) return `${trim(n / 1_000)}k`
  if (abs >= 100) return Math.round(n).toString()
  return trim(n)
}

export function formatPct(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return '--'
  return `${Math.round(n)}%`
}

export function formatMs(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return '--'
  return `${Math.round(n)} ms`
}

/** "45s", "2m", "3h": the compact form used inside map labels and chips. */
export function formatAgeShort(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m`
  return `${Math.floor(s / 3600)}h`
}

/** "45s ago", "2m ago", "3h ago". */
export function formatAge(seconds: number): string {
  return `${formatAgeShort(seconds)} ago`
}

/** Whole seconds from an RFC 3339 timestamp to `now` (epoch ms); null when absent or unparseable. */
export function ageSeconds(iso: string | null | undefined, now: number): number | null {
  if (!iso) return null
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return null
  return Math.max(0, Math.floor((now - t) / 1000))
}
