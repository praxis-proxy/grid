import { useEffect, useId, useRef, useState } from 'react'
import type { Site, Thresholds } from '../api/types'
import { useNow } from '../hooks/useAgeTicker'
import { useFocusTrap } from '../hooks/useFocusTrap'
import { useSiteDetail } from '../hooks/useSiteDetail'
import { ageSeconds, formatAge, formatCompact, formatMs, formatPct } from '../lib/format'
import { healthLabel } from '../lib/health'
import { tabId, tabPanelId } from '../lib/tabs'
import { crossesThreshold } from '../lib/thresholds'
import HealthShape from './HealthShape'
import AddressRow from './panel/AddressRow'
import HistoryTab from './panel/HistoryTab'
import KpiTile from './panel/KpiTile'
import LastError from './panel/LastError'
import ModelsTable from './panel/ModelsTable'
import Tabs from './panel/Tabs'
import TenantBar from './panel/TenantBar'

export interface SitePanelProps {
  site: Site
  onClose: () => void
  /** The fleet snapshot's generatedAt, so the detail refreshes once per poll while the panel is open. */
  refreshKey?: string
  thresholds: Thresholds
}

type TabKey = 'overview' | 'models' | 'tenants' | 'history'

/** What a down site's tiles show instead of stale numbers. */
export const DOWN_VALUE = '—'

const BADGE_TONE: Record<Site['health'], string> = {
  green: 'bg-healthy/15 text-healthy',
  yellow: 'bg-degraded/15 text-degraded',
  red: 'bg-down/15 text-down',
}

export default function SitePanel({ site, onClose, refreshKey, thresholds }: SitePanelProps) {
  const { detail, loading, error } = useSiteDetail(site.name, refreshKey)
  const [tab, setTab] = useState<TabKey>('overview')
  const panelRef = useRef<HTMLElement>(null)
  const closeButtonRef = useRef<HTMLButtonElement>(null)
  const titleId = useId()
  const now = useNow(site.lastSeen !== null)
  const title = site.displayName || site.name
  const down = site.health === 'red'

  useFocusTrap(panelRef, closeButtonRef)

  // Escape closes from anywhere: operators use it as the universal "back".
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose])

  const updatedAge = ageSeconds(site.lastSeen, now)
  const updatedText = updatedAge === null ? 'never updated' : `updated ${formatAge(updatedAge)}`
  const updatedTitle = site.lastSeen ? new Date(site.lastSeen).toLocaleString() : undefined
  const reasonClass = down ? 'text-down' : 'text-degraded'
  const points = detail?.series.points ?? null
  const tabs = [
    { id: 'overview', label: 'Overview' },
    { id: 'models', label: `Models (${site.models.length})` },
    { id: 'tenants', label: `Tenants (${site.tenants.length})` },
    { id: 'history', label: 'History' },
  ] as const

  return (
    <section
      ref={panelRef}
      role="dialog"
      aria-labelledby={titleId}
      className="fleet-panel flex h-full min-h-0 flex-col bg-surface text-sm"
    >
      <header className="flex items-start gap-2 border-b border-line p-3">
        <span className="pt-1">
          <HealthShape health={site.health} size={11} />
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <h2 id={titleId} className="truncate text-lg font-semibold text-ink">
              {title}
            </h2>
            <span className={`shrink-0 rounded px-1.5 py-0.5 text-xs font-medium ${BADGE_TONE[site.health]}`}>{healthLabel(site.health)}</span>
          </div>
          <p className="truncate text-xs text-ink-2" title={updatedTitle}>
            {[site.region, site.dc, updatedText].filter(Boolean).join(' · ')}
          </p>
          {site.reasons.length > 0 ? (
            <ul className={`mt-1 list-disc pl-4 text-xs ${reasonClass}`}>
              {site.reasons.map((reason) => (
                <li key={reason}>{reason}</li>
              ))}
            </ul>
          ) : null}
        </div>
        <button
          type="button"
          ref={closeButtonRef}
          onClick={onClose}
          aria-label="Close panel"
          className="flex h-7 w-7 items-center justify-center rounded text-lg leading-none text-ink-2 hover:bg-surface-2 hover:text-ink"
        >
          <span aria-hidden="true">×</span>
        </button>
      </header>
      <Tabs label="Site sections" tabs={tabs} active={tab} onChange={setTab} />
      <div role="tabpanel" id={tabPanelId(tab)} aria-labelledby={tabId(tab)} className="min-h-0 flex-1 overflow-y-auto p-3">
        {tab === 'overview' ? (
          <div className="grid grid-cols-2 gap-2">
            <KpiTile
              label="GPU util"
              value={down ? DOWN_VALUE : formatPct(site.gpus.utilPct)}
              hint={`warn at ${thresholds.gpuUtilWarn}%`}
              warn={!down && crossesThreshold(site.gpus.utilPct, thresholds.gpuUtilWarn)}
              points={points}
              field="gpuUtil"
            />
            <KpiTile
              label="Queue"
              value={down ? DOWN_VALUE : formatCompact(site.queueDepth)}
              hint={`warn at ${thresholds.queueWarn}`}
              warn={!down && crossesThreshold(site.queueDepth, thresholds.queueWarn)}
              points={points}
              field="queueDepth"
            />
            <KpiTile
              label="Requests/s"
              value={down ? DOWN_VALUE : formatCompact(site.rps)}
              hint={`p50 ${down ? DOWN_VALUE : formatMs(site.p50LatencyMs)}`}
              warn={!down && crossesThreshold(site.p50LatencyMs, thresholds.latencyWarnMs)}
              points={null}
              field="tokensPerSec"
            />
            <KpiTile
              label="Tokens/s"
              value={down ? DOWN_VALUE : formatCompact(site.tokensPerSec)}
              hint={`${site.gpus.total} GPUs`}
              warn={false}
              points={points}
              field="tokensPerSec"
            />
            {loading ? <p className="col-span-2 text-xs text-ink-2">Loading 20 minute history</p> : null}
            {error ? <p className="col-span-2 text-xs text-down">{error}</p> : null}
          </div>
        ) : null}
        {tab === 'models' ? <ModelsTable models={site.models} /> : null}
        {tab === 'tenants' ? <TenantBar tenants={site.tenants} /> : null}
        {tab === 'history' ? <HistoryTab series={detail?.series ?? null} loading={loading} error={error} /> : null}
      </div>
      <LastError error={site.lastError} />
      <AddressRow address={site.address} />
    </section>
  )
}
