import { LABEL_MODES, LINK_MODES, type LabelMode, type LinkMode } from '../lib/mapModes'
import Segmented from './Segmented'

export interface MapToolsProps {
  links: LinkMode
  onLinksChange: (mode: LinkMode) => void
  labels: LabelMode
  onLabelsChange: (mode: LabelMode) => void
  onFit: () => void
}

/** Top-right map toolbar: link mode, label mode, and "Fit" to re-frame the fleet. */
export default function MapTools({ links, onLinksChange, labels, onLabelsChange, onFit }: MapToolsProps) {
  return (
    <div role="toolbar" aria-label="Map tools" className="flex items-center gap-3 rounded-md border border-line bg-surface/95 px-2 py-1">
      <Segmented label="Links" prefix="Links" options={LINK_MODES} value={links} onChange={onLinksChange} />
      <Segmented label="Labels" prefix="Labels" options={LABEL_MODES} value={labels} onChange={onLabelsChange} />
      <button
        type="button"
        onClick={onFit}
        className="h-6 rounded border border-line bg-surface px-2 text-xs text-ink-2 hover:text-ink"
      >
        Fit
      </button>
    </div>
  )
}
