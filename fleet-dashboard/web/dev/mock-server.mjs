// Stand-in for the Go backend's /api/v1 surface so the SPA can be developed without a cluster.
// Run: npm run mock-api   (listens on :8080, which vite.config.ts proxies /api to)
import { createServer } from 'node:http'

const PORT = Number(process.env.PORT ?? 8080)
const POLL_SECONDS = 5
const THRESHOLDS = { gpuUtilWarn: 90, queueWarn: 50, latencyWarnMs: 5000 }

const hub = { name: 'aigrid-ds-hub', region: 'us-east-1', lat: 38.95, lng: -77.45 }

const seeds = [
  { name: 'aigrid-ds-spoke1', displayName: 'Ohio', region: 'us-east-2', dc: 'aws-us-east-2', lat: 40.09, lng: -82.75, gpus: 64, baseUtil: 67, baseQueue: 12, models: ['llama-3.1-70b', 'qwen2.5-coder-32b'] },
  { name: 'aigrid-ds-spoke2', displayName: 'Oregon', region: 'us-west-2', dc: 'aws-us-west-2', lat: 45.87, lng: -119.69, gpus: 32, baseUtil: 48, baseQueue: 8, models: ['llama-3.1-70b'] },
  { name: 'aigrid-ds-spoke3', displayName: 'London', region: 'eu-west-2', dc: 'aws-eu-west-2', lat: 51.51, lng: -0.13, gpus: 16, baseUtil: 91, baseQueue: 20, models: ['mistral-7b'] },
  { name: 'aigrid-ds-spoke4', displayName: 'Tokyo', region: 'ap-northeast-1', dc: 'aws-ap-northeast-1', lat: 35.68, lng: 139.69, gpus: 8, baseUtil: 35, baseQueue: 3, models: ['llama-3.1-8b'] },
  { name: 'aigrid-ds-spoke5', displayName: 'Sao Paulo', region: 'sa-east-1', dc: 'aws-sa-east-1', lat: -23.55, lng: -46.63, gpus: 4, baseUtil: 0, baseQueue: 0, models: [] },
  { name: 'aigrid-lab', displayName: 'Lab rack', region: 'unknown-lab', dc: 'onprem', lat: null, lng: null, gpus: 2, baseUtil: 20, baseQueue: 0, models: ['llama-3.1-8b'] },
]
const DOWN_SITE = 'aigrid-ds-spoke5'
const startedAt = Date.now()

function drift(base, amplitude, periodSeconds, phase) {
  const t = (Date.now() - startedAt) / 1000
  return base + amplitude * Math.sin((2 * Math.PI * t) / periodSeconds + phase)
}

function buildSite(seed, index, at) {
  const down = seed.name === DOWN_SITE
  const util = down ? null : Math.max(0, Math.min(100, Math.round(drift(seed.baseUtil, 12, 300, index))))
  const queue = down ? null : Math.max(0, Math.round(drift(seed.baseQueue, seed.baseQueue, 240, index * 2)))
  const reasons = []
  let health = 'green'
  if (down) {
    health = 'red'
    reasons.push('unreachable for 2 consecutive polls')
  } else {
    if (util >= 90) {
      health = 'yellow'
      reasons.push(`GPU utilization ${util}% >= 90%`)
    }
    if (queue >= 50) {
      health = 'yellow'
      reasons.push(`queue depth ${queue} >= 50`)
    }
  }
  return {
    name: seed.name,
    displayName: seed.displayName,
    region: seed.region,
    dc: seed.dc,
    lat: seed.lat,
    lng: seed.lng,
    placed: seed.lat !== null && seed.lng !== null,
    address: `gateway.apps.${seed.name}.example.internal`,
    health,
    reasons,
    gpus: { total: seed.gpus, utilPct: util },
    models: down ? [] : seed.models.map((name, i) => ({ name, running: Math.max(0, Math.round(drift(4 + i, 3, 120, i))) })),
    rps: down ? null : Math.round(drift(seed.gpus * 0.8, seed.gpus * 0.2, 150, index) * 10) / 10,
    p50LatencyMs: down ? null : Math.round(drift(900, 300, 200, index)),
    tokensPerSec: down ? null : Math.round(drift(seed.gpus * 55, seed.gpus * 10, 180, index)),
    queueDepth: queue,
    tenants: down
      ? []
      : [
          { name: 'research', sharePct: 55 },
          { name: 'platform', sharePct: 30 },
          { name: 'sandbox', sharePct: 15 },
        ],
    lastSeen: down ? new Date(startedAt).toISOString() : at,
    lastError: down ? 'Get "https://thanos-querier.example.internal/api/v1/query": dial tcp: i/o timeout' : '',
  }
}

function buildSnapshot() {
  const at = new Date().toISOString()
  const sites = seeds.map((seed, i) => buildSite(seed, i, at))
  const withUtil = sites.filter((s) => s.gpus.utilPct !== null)
  const gpuWeighted = withUtil.reduce((acc, s) => acc + s.gpus.total * s.gpus.utilPct, 0)
  const gpuWithUtil = withUtil.reduce((acc, s) => acc + s.gpus.total, 0)
  const summary = {
    gpuTotal: sites.reduce((acc, s) => acc + s.gpus.total, 0),
    gpuUtilPct: gpuWithUtil === 0 ? 0 : Math.round((gpuWeighted / gpuWithUtil) * 10) / 10,
    tokensPerSec: sites.reduce((acc, s) => acc + (s.tokensPerSec ?? 0), 0),
    rps: Math.round(sites.reduce((acc, s) => acc + (s.rps ?? 0), 0) * 10) / 10,
    activeModels: new Set(sites.flatMap((s) => s.models.map((m) => m.name))).size,
    activeTenants: new Set(sites.flatMap((s) => s.tenants.map((t) => t.name))).size,
    sitesGreen: sites.filter((s) => s.health === 'green').length,
    sitesYellow: sites.filter((s) => s.health === 'yellow').length,
    sitesRed: sites.filter((s) => s.health === 'red').length,
  }
  return { generatedAt: at, hub, sites, summary, routes: [] }
}

const RANGE_SECONDS = { '1h': 3600, '6h': 6 * 3600, '24h': 24 * 3600 }
const STEP_BY_RANGE = { '1h': 30, '6h': 120, '24h': 600 }

function seriesPoints(rangeSeconds, step, now) {
  const n = Math.floor(rangeSeconds / step)
  const points = []
  for (let i = n; i >= 0; i--) {
    const tMs = now - i * step * 1000
    const s = tMs / 1000
    points.push({
      t: new Date(tMs).toISOString(),
      gpuUtil: Math.round(58 + 18 * Math.sin(s / 900) + 5 * Math.sin(s / 137)),
      queueDepth: Math.max(0, Math.round(25 + 20 * Math.sin(s / 600 + 1))),
      tokensPerSec: Math.round(6000 + 1800 * Math.sin(s / 800 + 2)),
    })
  }
  // One gap in the middle so the UI's null handling is exercised.
  if (points.length > 10) {
    const gap = points[Math.floor(points.length / 2)]
    gap.gpuUtil = null
    gap.queueDepth = null
    gap.tokensPerSec = null
  }
  return points
}

function streamHandler(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-store',
    Connection: 'keep-alive',
  })
  res.write(': connected\n\n')
  const send = () => res.write(`event: fleet\ndata: ${JSON.stringify(buildSnapshot())}\n\n`)
  // Send the current snapshot immediately on connect, matching the Go
  // server, instead of leaving the client waiting up to POLL_SECONDS for the
  // first event.
  send()
  const fleetTimer = setInterval(send, POLL_SECONDS * 1000)
  const pingTimer = setInterval(() => res.write(': ping\n\n'), 15000)
  req.on('close', () => {
    clearInterval(fleetTimer)
    clearInterval(pingTimer)
  })
}

const server = createServer((req, res) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`)
  const json = (status, body) => {
    res.writeHead(status, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify(body))
  }
  if (url.pathname === '/api/v1/config') {
    const user = req.headers['x-forwarded-user']?.trim() || null
    return json(200, { hub, pollIntervalSeconds: POLL_SECONDS, version: 'mock', thresholds: THRESHOLDS, user })
  }
  if (url.pathname === '/api/v1/fleet') return json(200, buildSnapshot())
  if (url.pathname.startsWith('/api/v1/sites/')) {
    const name = decodeURIComponent(url.pathname.slice('/api/v1/sites/'.length))
    const site = buildSnapshot().sites.find((s) => s.name === name)
    if (!site) return json(404, { error: 'site not found' })
    return json(200, { ...site, series: { step: 30, points: seriesPoints(20 * 60, 30, Date.now()) } })
  }
  if (url.pathname === '/api/v1/series') {
    const range = url.searchParams.get('range') ?? '1h'
    const step = STEP_BY_RANGE[range]
    if (!step) return json(400, { error: 'range must be 1h, 6h or 24h' })
    return json(200, { range, step, points: seriesPoints(RANGE_SECONDS[range], step, Date.now()) })
  }
  if (url.pathname === '/api/v1/stream') return streamHandler(req, res)
  return json(404, { error: 'not found' })
})

server.listen(PORT, () => {
  console.log(`mock fleet API listening on http://localhost:${PORT} (poll every ${POLL_SECONDS}s)`)
})
