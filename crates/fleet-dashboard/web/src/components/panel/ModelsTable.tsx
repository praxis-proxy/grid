import type { SiteModel } from '../../api/types'

export interface ModelsTableProps {
  models: SiteModel[]
}

/** Model name, running count, and a bar proportional to the busiest model. */
export default function ModelsTable({ models }: ModelsTableProps) {
  if (models.length === 0) return <p className="text-xs text-ink-2">No models reporting</p>
  const max = Math.max(1, ...models.map((model) => model.running))
  return (
    <table className="w-full text-xs">
      <thead>
        <tr className="text-ink-2">
          <th className="text-left font-normal">Model</th>
          <th className="w-14 text-right font-normal">Running</th>
          <th className="w-20">
            <span className="sr-only">Share of busiest</span>
          </th>
        </tr>
      </thead>
      <tbody>
        {models.map((model) => (
          <tr key={model.name}>
            <td className="max-w-0 truncate py-0.5 text-ink" title={model.name}>
              {model.name}
            </td>
            <td className="py-0.5 text-right tabular-nums text-ink">{model.running}</td>
            <td className="py-0.5 pl-2">
              <span className="block h-1.5 w-full overflow-hidden rounded bg-line" role="img" aria-label={`${model.running} of ${max}`}>
                <span className="block h-full bg-accent" style={{ width: `${(100 * model.running) / max}%` }} />
              </span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
