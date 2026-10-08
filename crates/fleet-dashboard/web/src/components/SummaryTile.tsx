import type { ReactNode } from 'react'

export interface SummaryTileProps {
  label: string
  value: string
  /** Third line: context such as "5 placed · 1 unplaced". */
  sub?: ReactNode
  /** Change line such as "+3 pts vs 1h ago". */
  delta?: string
}

export default function SummaryTile({ label, value, sub, delta }: SummaryTileProps) {
  return (
    <div role="group" aria-label={label} className="flex min-w-0 flex-col justify-between rounded-md border border-line bg-surface-2 px-3 py-2">
      <span className="text-xs uppercase tracking-wide text-ink-2">{label}</span>
      <span className="text-2xl font-semibold tabular-nums text-ink">{value}</span>
      <span className="flex min-w-0 items-baseline gap-2 text-xs text-ink-2">
        {delta ? <span className="shrink-0 tabular-nums text-ink">{delta}</span> : null}
        <span className="min-w-0 truncate">{sub ?? ''}</span>
      </span>
    </div>
  )
}
