export interface SegmentedOption<T extends string> {
  value: T
  label: string
}

export interface SegmentedProps<T extends string> {
  /** Accessible name of the group, e.g. "Sort sites". */
  label: string
  options: ReadonlyArray<SegmentedOption<T>>
  value: T
  onChange: (value: T) => void
  /** Visible prefix before the buttons, e.g. "Links". */
  prefix?: string
  /** sm: 24 px targets (map tools, roster). md: 40 px targets (chart range control). */
  size?: 'sm' | 'md'
}

const SIZE_CLASS = {
  sm: 'h-6 px-2 text-xs',
  md: 'h-10 px-3 text-sm',
}

/** A row of mutually exclusive toggle buttons; the pressed one reads as selected. */
export default function Segmented<T extends string>({ label, options, value, onChange, prefix, size = 'sm' }: SegmentedProps<T>) {
  return (
    <div role="group" aria-label={label} className="inline-flex items-center gap-1.5 text-xs text-ink-2">
      {prefix ? <span>{prefix}</span> : null}
      <span className="inline-flex overflow-hidden rounded border border-line bg-surface">
        {options.map((option) => (
          <button
            key={option.value}
            type="button"
            aria-pressed={option.value === value}
            onClick={() => onChange(option.value)}
            className={`${SIZE_CLASS[size]} text-ink-2 hover:text-ink aria-pressed:bg-surface-2 aria-pressed:text-ink`}
          >
            {option.label}
          </button>
        ))}
      </span>
    </div>
  )
}
