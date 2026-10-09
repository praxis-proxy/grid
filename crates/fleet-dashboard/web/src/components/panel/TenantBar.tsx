import type { SiteTenant } from '../../api/types'

export interface TenantBarProps {
  tenants: SiteTenant[]
}

/** Accent shades only (spec palette); a fleet with more tenants than shades repeats them at reduced opacity. */
const ACCENT_SHADES = ['#2fc4d1', '#1d8a94', '#155f66']

function clampPct(pct: number): number {
  return Math.max(0, Math.min(100, pct))
}

function tenantStyle(i: number): { background: string; opacity: number } {
  const cycle = Math.floor(i / ACCENT_SHADES.length)
  return { background: ACCENT_SHADES[i % ACCENT_SHADES.length], opacity: 1 / (cycle + 1) }
}

/** Stacked share bar with a legend, then the list with exact percentages. */
export default function TenantBar({ tenants }: TenantBarProps) {
  if (tenants.length === 0) return <p className="text-xs text-ink-2">No tenant breakdown configured</p>
  const description = tenants.map((t) => `${t.name} ${Math.round(t.sharePct)}%`).join(', ')
  return (
    <div>
      <div className="flex h-2.5 w-full overflow-hidden rounded bg-line" role="img" aria-label={description}>
        {tenants.map((tenant, i) => (
          <div key={tenant.name} style={{ width: `${clampPct(tenant.sharePct)}%`, ...tenantStyle(i) }} />
        ))}
      </div>
      <ul className="mt-2 flex flex-col gap-1 text-xs">
        {tenants.map((tenant, i) => (
          <li key={tenant.name} className="flex items-center gap-2">
            <span className="inline-block h-2 w-2 shrink-0 rounded-sm" style={tenantStyle(i)} aria-hidden="true" />
            <span className="min-w-0 flex-1 truncate text-ink">{tenant.name}</span>
            <span className="tabular-nums text-ink-2">{Math.round(tenant.sharePct)}%</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
