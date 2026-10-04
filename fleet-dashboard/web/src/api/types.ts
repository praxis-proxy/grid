export type Health = 'green' | 'yellow' | 'red'

export interface Hub {
  name: string
  region: string
  lat: number | null
  lng: number | null
}

export interface SiteModel {
  name: string
  running: number
}

export interface SiteTenant {
  name: string
  sharePct: number
}

export interface Site {
  name: string
  displayName: string
  region: string
  dc: string
  lat: number | null
  lng: number | null
  placed: boolean
  address: string
  health: Health
  reasons: string[]
  gpus: { total: number; utilPct: number | null }
  models: SiteModel[]
  rps: number | null
  p50LatencyMs: number | null
  tokensPerSec: number | null
  queueDepth: number | null
  tenants: SiteTenant[]
  lastSeen: string | null
  lastError: string
}

export interface FleetSummary {
  gpuTotal: number
  gpuUtilPct: number
  tokensPerSec: number
  rps: number
  activeModels: number
  activeTenants: number
  sitesGreen: number
  sitesYellow: number
  sitesRed: number
}

/** A registered route between two named nodes (hub or site). Empty in v1 snapshots. */
export interface Route {
  from: string
  to: string
}

export interface FleetSnapshot {
  generatedAt: string
  hub: Hub | null
  sites: Site[]
  summary: FleetSummary
  routes: Route[]
}

export interface SeriesPoint {
  t: string
  gpuUtil: number | null
  queueDepth: number | null
  tokensPerSec: number | null
}

export interface Series {
  step: number
  points: SeriesPoint[]
}

export type SeriesRange = '1h' | '6h' | '24h'

export interface SeriesResponse extends Series {
  range: SeriesRange
}

export interface SiteDetail extends Site {
  series: Series
}

export interface Thresholds {
  gpuUtilWarn: number
  queueWarn: number
  latencyWarnMs: number
}

export interface AppConfig {
  hub: Hub | null
  pollIntervalSeconds: number
  version: string
  thresholds: Thresholds
  /** Identity forwarded by oauth-proxy (X-Forwarded-User); null without a proxy. */
  user: string | null
}
