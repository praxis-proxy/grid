import type { Health, Site } from '../api/types'

export type SortKey = 'worst' | 'utilization' | 'name' | 'region'

export const SORT_OPTIONS: ReadonlyArray<{ value: SortKey; label: string }> = [
  { value: 'worst', label: 'Worst first' },
  { value: 'utilization', label: 'Utilization' },
  { value: 'name', label: 'Name' },
  { value: 'region', label: 'Region' },
]

const HEALTH_RANK: Record<Health, number> = { red: 0, yellow: 1, green: 2 }

function displayName(site: Site): string {
  return site.displayName || site.name
}

function byName(a: Site, b: Site): number {
  return displayName(a).localeCompare(displayName(b))
}

/** Higher utilization first; unknown utilization sorts last. */
function byUtilizationDesc(a: Site, b: Site): number {
  const ua = a.gpus.utilPct
  const ub = b.gpus.utilPct
  if (ua === ub) return 0
  if (ua === null) return 1
  if (ub === null) return -1
  return ub - ua
}

/** Returns a new array; never mutates the input. */
export function sortSites(sites: readonly Site[], key: SortKey): Site[] {
  const copy = [...sites]
  switch (key) {
    case 'worst':
      return copy.sort((a, b) => HEALTH_RANK[a.health] - HEALTH_RANK[b.health] || byUtilizationDesc(a, b) || byName(a, b))
    case 'utilization':
      return copy.sort((a, b) => byUtilizationDesc(a, b) || byName(a, b))
    case 'name':
      return copy.sort(byName)
    case 'region':
      return copy.sort((a, b) => a.region.localeCompare(b.region) || byName(a, b))
  }
}

/** Case-insensitive substring match on name, display name, region, dc and model names, then the health filter. */
export function filterSites(sites: readonly Site[], query: string, health: Health | null): Site[] {
  const q = query.trim().toLowerCase()
  return sites.filter((site) => {
    if (health !== null && site.health !== health) return false
    if (q === '') return true
    const haystack = [site.name, site.displayName, site.region, site.dc, ...site.models.map((m) => m.name)]
    return haystack.some((field) => field.toLowerCase().includes(q))
  })
}

export function fleetGpuTotal(sites: readonly Site[]): number {
  return sites.reduce((sum, site) => sum + site.gpus.total, 0)
}

/** DOM id of a roster option, the target of the listbox's aria-activedescendant. */
export function rosterOptionId(name: string): string {
  return `roster-option-${name}`
}
