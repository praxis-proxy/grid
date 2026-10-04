import { tabId, tabPanelId } from '../../lib/tabs'

export interface TabSpec<T extends string> {
  id: T
  label: string
}

export interface TabsProps<T extends string> {
  label: string
  tabs: ReadonlyArray<TabSpec<T>>
  active: T
  onChange: (id: T) => void
}

/** WAI-ARIA tabs: one tab stop, arrow keys move between tabs, the active tab is the only tabbable one. */
export default function Tabs<T extends string>({ label, tabs, active, onChange }: TabsProps<T>) {
  const onKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    const index = tabs.findIndex((tab) => tab.id === active)
    let next = index
    if (event.key === 'ArrowRight') next = (index + 1) % tabs.length
    else if (event.key === 'ArrowLeft') next = (index - 1 + tabs.length) % tabs.length
    else if (event.key === 'Home') next = 0
    else if (event.key === 'End') next = tabs.length - 1
    else return
    event.preventDefault()
    onChange(tabs[next].id)
    document.getElementById(tabId(tabs[next].id))?.focus()
  }
  return (
    <div role="tablist" aria-label={label} onKeyDown={onKeyDown} className="flex border-b border-line text-xs">
      {tabs.map((tab) => {
        const selected = tab.id === active
        return (
          <button
            key={tab.id}
            id={tabId(tab.id)}
            type="button"
            role="tab"
            aria-selected={selected}
            aria-controls={selected ? tabPanelId(tab.id) : undefined}
            tabIndex={selected ? 0 : -1}
            onClick={() => onChange(tab.id)}
            className="-mb-px border-b-2 border-transparent px-3 py-2 text-ink-2 hover:text-ink aria-selected:border-accent aria-selected:text-ink"
          >
            {tab.label}
          </button>
        )
      })}
    </div>
  )
}
