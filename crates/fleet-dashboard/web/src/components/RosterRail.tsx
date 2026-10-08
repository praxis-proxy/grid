import { useEffect, useMemo, useState } from 'react'
import type { Health, Site } from '../api/types'
import { useNow } from '../hooks/useAgeTicker'
import { filterSites, fleetGpuTotal, rosterOptionId, SORT_OPTIONS, sortSites, type SortKey } from '../lib/roster'
import HealthShape from './HealthShape'
import RosterRow from './RosterRow'
import Segmented from './Segmented'

export interface RosterRailProps {
  /** Every site, placed or not; unplaced ones get a "no position" tag. */
  sites: Site[]
  selected: string | null
  onSelect: (name: string) => void
  onClear: () => void
  /** From the top bar's status pill; null shows every health. */
  healthFilter: Health | null
  pollIntervalSeconds: number
  collapsed: boolean
  onCollapsedChange: (collapsed: boolean) => void
  /** True when `collapsed` is imposed by a narrow viewport rather than the user's own preference; disables the toggle. */
  forced?: boolean
  /** Renders skeleton rows instead of sites while the first snapshot loads. */
  loading?: boolean
}

const SKELETON_ROWS = 6

function scrollOptionIntoView(name: string): void {
  const el = document.getElementById(rosterOptionId(name))
  if (el && typeof el.scrollIntoView === 'function') el.scrollIntoView({ block: 'nearest' })
}

export default function RosterRail({
  sites,
  selected,
  onSelect,
  onClear,
  healthFilter,
  pollIntervalSeconds,
  collapsed,
  onCollapsedChange,
  forced = false,
  loading = false,
}: RosterRailProps) {
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('worst')
  // Keyboard cursor: the option aria-activedescendant points at. Follows the
  // selection so a click or map selection and the arrow keys agree.
  const [active, setActive] = useState<string | null>(null)
  // Derived-from-props reset (no effect): when a selection is made elsewhere
  // (map, URL) the keyboard cursor should jump straight to that row instead
  // of staying wherever the arrow keys last left it.
  const [prevSelected, setPrevSelected] = useState(selected)
  if (selected !== prevSelected) {
    setPrevSelected(selected)
    setActive(selected)
  }
  const visible = useMemo(() => sortSites(filterSites(sites, query, healthFilter), sort), [sites, query, healthFilter, sort])
  const now = useNow(sites.some((site) => site.health === 'red'))
  const cursor = active ?? selected

  // Keep the selected row in view when the selection is made elsewhere (map, attention strip, deep link).
  useEffect(() => {
    if (selected) scrollOptionIntoView(selected)
  }, [selected])

  const onKeyDown = (event: React.KeyboardEvent<HTMLUListElement>) => {
    if (visible.length === 0) {
      if (event.key === 'Escape') onClear()
      return
    }
    const index = cursor ? visible.findIndex((site) => site.name === cursor) : -1
    const move = (next: number) => {
      const target = visible[Math.max(0, Math.min(visible.length - 1, next))]
      setActive(target.name)
      scrollOptionIntoView(target.name)
    }
    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault()
        move(index + 1)
        break
      case 'ArrowUp':
        event.preventDefault()
        move(index <= 0 ? 0 : index - 1)
        break
      case 'Home':
        event.preventDefault()
        move(0)
        break
      case 'End':
        event.preventDefault()
        move(visible.length - 1)
        break
      case 'Enter':
      case ' ':
        event.preventDefault()
        if (cursor && visible.some((site) => site.name === cursor)) onSelect(cursor)
        break
      case 'Escape':
        event.preventDefault()
        setActive(null)
        onClear()
        break
    }
  }

  const toggle = (
    <button
      type="button"
      aria-expanded={!collapsed}
      aria-label={collapsed ? 'Expand roster' : 'Collapse roster'}
      onClick={() => {
        if (!forced) onCollapsedChange(!collapsed)
      }}
      disabled={forced}
      aria-disabled={forced ? 'true' : undefined}
      title={forced ? 'Roster collapses automatically below 1280 px' : undefined}
      className="flex h-7 w-7 shrink-0 items-center justify-center rounded border border-line text-ink-2 hover:text-ink disabled:cursor-not-allowed disabled:opacity-50"
    >
      <span aria-hidden="true">{collapsed ? '»' : '«'}</span>
    </button>
  )

  if (collapsed) {
    return (
      <nav aria-label="Sites" className="flex h-full w-14 flex-col items-center gap-2 border-r border-line bg-surface py-2">
        {toggle}
        <ul className="flex flex-col items-center gap-1 overflow-y-auto">
          {sortSites(filterSites(sites, '', healthFilter), 'worst').map((site) => (
            <li key={site.name}>
              <button
                type="button"
                title={site.displayName || site.name}
                aria-label={site.displayName || site.name}
                aria-pressed={site.name === selected}
                onClick={() => onSelect(site.name)}
                className="flex h-7 w-7 items-center justify-center rounded hover:bg-surface-2 aria-pressed:bg-surface-2"
              >
                <HealthShape health={site.health} size={10} label="" />
              </button>
            </li>
          ))}
        </ul>
      </nav>
    )
  }

  return (
    <nav aria-label="Sites" className="flex h-full w-[280px] min-h-0 flex-col border-r border-line bg-surface">
      <div className="flex items-center gap-2 border-b border-line p-2">
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search sites"
          aria-label="Search sites"
          className="h-7 min-w-0 flex-1 rounded border border-line bg-bg px-2 text-xs text-ink placeholder:text-ink-3"
        />
        {toggle}
      </div>
      <div className="border-b border-line px-2 py-1.5">
        <Segmented label="Sort sites" options={SORT_OPTIONS} value={sort} onChange={setSort} />
      </div>
      {loading ? (
        <ul aria-label="Loading sites" aria-busy="true" className="flex-1 overflow-hidden">
          {Array.from({ length: SKELETON_ROWS }, (_, i) => (
            <li key={i} className="border-b border-line px-3 py-2">
              <div className="h-3 w-2/3 animate-pulse rounded bg-surface-2" />
              <div className="mt-1.5 h-2 w-1/2 animate-pulse rounded bg-surface-2" />
              <div className="mt-1.5 h-1 animate-pulse rounded bg-surface-2" />
            </li>
          ))}
        </ul>
      ) : (
        <ul
          role="listbox"
          aria-label="Site list"
          tabIndex={0}
          aria-activedescendant={cursor && visible.some((site) => site.name === cursor) ? rosterOptionId(cursor) : undefined}
          onKeyDown={onKeyDown}
          className="min-h-0 flex-1 overflow-y-auto outline-none focus-visible:ring-1 focus-visible:ring-accent"
        >
          {visible.map((site) => (
            <RosterRow key={site.name} site={site} selected={site.name === selected} active={site.name === cursor} now={now} onSelect={onSelect} />
          ))}
          {visible.length === 0 && sites.length > 0 ? <li className="px-3 py-4 text-center text-xs text-ink-2">No sites match</li> : null}
        </ul>
      )}
      <div className="border-t border-line px-3 py-1.5 text-xs text-ink-2">
        {loading
          ? `-- sites · -- GPUs · polled every ${pollIntervalSeconds} s`
          : `${sites.length} sites · ${fleetGpuTotal(sites)} GPUs · polled every ${pollIntervalSeconds} s`}
      </div>
    </nav>
  )
}
