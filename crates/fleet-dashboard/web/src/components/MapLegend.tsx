import type { Health } from '../api/types'
import { arcPath, gaugeRadius, hexagonPoints, ringRadius } from '../lib/glyph'
import { GAUGE_TRACK_COLOR, HEALTH_ORDER, HUB_COLOR, healthColor, healthWord } from '../lib/health'
import HealthShape from './HealthShape'

const SAMPLE_GPUS = [4, 16, 64]
/** Sample rings are drawn at half size so three of them fit in a 40 px row. */
const SAMPLE_SCALE = 0.5
const SAMPLE_COLOR = healthColor('green')
const HEALTHS: Health[] = [...HEALTH_ORDER].reverse()

/** Sample ring positions, laid out left to right with an 8 px gap; computed once. */
const SAMPLE_LAYOUT = (() => {
  const gap = 8
  const rings: Array<{ gpus: number; cx: number; r: number }> = []
  let x = 0
  for (const gpus of SAMPLE_GPUS) {
    const r = ringRadius(gpus) * SAMPLE_SCALE
    rings.push({ gpus, cx: x + r, r })
    x += 2 * r + gap
  }
  const height = 2 * Math.max(...rings.map((ring) => ring.r))
  return { rings, width: x - gap, height }
})()

function SampleRings() {
  const { rings, width, height } = SAMPLE_LAYOUT
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
      {rings.map((ring) => (
        <circle key={ring.gpus} cx={ring.cx} cy={height / 2} r={ring.r - 1} fill="none" stroke={SAMPLE_COLOR} strokeWidth={1.5} />
      ))}
    </svg>
  )
}

function SampleArc() {
  const r = ringRadius(16) * SAMPLE_SCALE
  const size = 2 * r
  const g = gaugeRadius(r)
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-hidden="true">
      <circle cx={r} cy={r} r={r - 1} fill="none" stroke={SAMPLE_COLOR} strokeWidth={1.5} />
      <circle cx={r} cy={r} r={g} fill="none" stroke={GAUGE_TRACK_COLOR} strokeWidth={2} />
      <path d={arcPath(r, r, g, 0.7)} fill="none" stroke={SAMPLE_COLOR} strokeWidth={2} strokeLinecap="round" />
    </svg>
  )
}

function Row({ children, text }: { children: React.ReactNode; text: string }) {
  return (
    <div className="flex items-center gap-2">
      <span className="flex w-16 shrink-0 justify-center">{children}</span>
      <span className="text-ink-2">{text}</span>
    </div>
  )
}

/** Always-visible key for the map glyphs (bottom left of the map). */
export default function MapLegend() {
  return (
    <aside aria-label="Map legend" className="flex flex-col gap-1.5 rounded-md border border-line bg-surface/95 px-2.5 py-2 text-xs">
      <Row text={`ring size = GPUs (${SAMPLE_GPUS.join(' · ')})`}>
        <SampleRings />
      </Row>
      <Row text="inner arc = GPU utilization">
        <SampleArc />
      </Row>
      <Row text="health">
        <span className="flex items-center gap-2">
          {HEALTHS.map((health) => (
            <HealthShape key={health} health={health} label={healthWord(health)} />
          ))}
        </span>
      </Row>
      <Row text="hub">
        <svg width={16} height={16} viewBox="0 0 16 16" aria-hidden="true">
          <polygon points={hexagonPoints(8, 8, 7)} fill={HUB_COLOR} fillOpacity={0.35} stroke={HUB_COLOR} strokeWidth={1.5} />
        </svg>
      </Row>
      <Row text="registered route hub → site">
        <svg width={40} height={8} viewBox="0 0 40 8" aria-hidden="true">
          <line x1={0} y1={4} x2={40} y2={4} stroke={HUB_COLOR} strokeWidth={1.5} strokeDasharray="2 6" />
        </svg>
      </Row>
    </aside>
  )
}
