import { useEffect, useState } from 'react'
import { errorMessage, fetchSeries } from '../api/client'
import type { SeriesRange, SeriesResponse } from '../api/types'

export const SERIES_REFRESH_MS = 60_000

export interface UseSeriesResult {
  series: SeriesResponse | null
  error: string | null
}

/** Fetches /api/v1/series for the range immediately and every 60s while mounted. */
export function useSeries(range: SeriesRange): UseSeriesResult {
  const [series, setSeries] = useState<SeriesResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loadedRange, setLoadedRange] = useState(range)

  // Reset synchronously during render (React's documented pattern for adjusting state to a
  // prop change, rather than setState-in-effect) so a range switch never leaves the previous
  // range's data on screen under the new range's label while the new fetch is in flight or
  // has failed. The 60s refresh poll below reuses this effect's `load` without touching
  // loadedRange, so a single missed poll doesn't blank an otherwise-fine chart.
  if (range !== loadedRange) {
    setLoadedRange(range)
    setSeries(null)
    setError(null)
  }

  useEffect(() => {
    const controller = new AbortController()
    const load = () => {
      fetchSeries(range, controller.signal)
        .then((loaded) => {
          setSeries(loaded)
          setError(null)
        })
        .catch((err: unknown) => {
          if (!controller.signal.aborted) setError(errorMessage(err))
        })
    }
    load()
    const id = setInterval(load, SERIES_REFRESH_MS)
    return () => {
      clearInterval(id)
      controller.abort()
    }
  }, [range])

  return { series, error }
}
