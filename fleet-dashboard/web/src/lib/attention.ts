import type { Site } from '../api/types'

const RANK: Record<Site['health'], number> = { red: 0, yellow: 1, green: 2 }

/** Unhealthy sites, down first, then degraded; ties by display name so chips never reorder between polls. */
export function attentionSites(sites: readonly Site[]): Site[] {
  return sites
    .filter((site) => site.health !== 'green')
    .sort((a, b) => RANK[a.health] - RANK[b.health] || (a.displayName || a.name).localeCompare(b.displayName || b.name))
}
