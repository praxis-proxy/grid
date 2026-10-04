import type {
  AppConfig,
  FleetSnapshot,
  SeriesRange,
  SeriesResponse,
  Site,
  SiteDetail,
} from '../api/types'

export function makeSite(overrides: Partial<Site> = {}): Site {
  return {
    name: 'aigrid-ds-spoke1',
    displayName: 'Ohio',
    region: 'us-east-2',
    dc: 'aws-us-east-2',
    lat: 40.09,
    lng: -82.75,
    placed: true,
    address: 'gateway.apps.aigrid-ds-spoke1.example.internal',
    health: 'green',
    reasons: [],
    gpus: { total: 64, utilPct: 67 },
    models: [{ name: 'llama-3.1-70b', running: 6 }],
    rps: 42.5,
    p50LatencyMs: 812,
    tokensPerSec: 3140,
    queueDepth: 12,
    tenants: [
      { name: 'research', sharePct: 60 },
      { name: 'platform', sharePct: 40 },
    ],
    lastSeen: '2026-09-06T12:00:00Z',
    lastError: '',
    ...overrides,
  }
}

export function makeSnapshot(overrides: Partial<FleetSnapshot> = {}): FleetSnapshot {
  return {
    generatedAt: '2026-09-06T12:00:00Z',
    hub: { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 },
    sites: [makeSite()],
    summary: {
      gpuTotal: 64,
      gpuUtilPct: 67,
      tokensPerSec: 3140,
      rps: 42.5,
      activeModels: 1,
      activeTenants: 2,
      sitesGreen: 1,
      sitesYellow: 0,
      sitesRed: 0,
    },
    routes: [],
    ...overrides,
  }
}

export function makeSiteDetail(overrides: Partial<SiteDetail> = {}): SiteDetail {
  return {
    ...makeSite(),
    series: {
      step: 30,
      points: [
        { t: '2026-09-06T11:59:00Z', gpuUtil: 60, queueDepth: 10, tokensPerSec: 3000 },
        { t: '2026-09-06T11:59:30Z', gpuUtil: 67, queueDepth: 12, tokensPerSec: 3140 },
      ],
    },
    ...overrides,
  }
}

export function makeSeries(range: SeriesRange = '1h'): SeriesResponse {
  return {
    range,
    step: 30,
    points: [
      { t: '2026-09-06T11:58:00Z', gpuUtil: 55, queueDepth: 20, tokensPerSec: 5800 },
      { t: '2026-09-06T11:58:30Z', gpuUtil: null, queueDepth: null, tokensPerSec: null },
      { t: '2026-09-06T11:59:00Z', gpuUtil: 61, queueDepth: 24, tokensPerSec: 6100 },
    ],
  }
}

export const testConfig: AppConfig = {
  hub: { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 },
  pollIntervalSeconds: 5,
  version: 'test',
  thresholds: { gpuUtilWarn: 90, queueWarn: 50, latencyWarnMs: 5000 },
  user: null,
}
