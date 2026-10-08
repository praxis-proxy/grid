import { useEffect, useState } from 'react'
import { errorMessage, fetchSite } from '../api/client'
import type { SiteDetail } from '../api/types'

export interface UseSiteDetailResult {
  detail: SiteDetail | null
  loading: boolean
  error: string | null
}

interface Loaded {
  name: string
  detail: SiteDetail | null
  error: string | null
}

/**
 * Fetches /api/v1/sites/{name} whenever the name changes, and again whenever
 * `refreshKey` changes (the caller passes the fleet snapshot's generatedAt so
 * the panel refreshes once per poll while it stays open). Results are keyed
 * by name so a stale detail is never shown.
 */
export function useSiteDetail(name: string | null, refreshKey?: string): UseSiteDetailResult {
  const [loaded, setLoaded] = useState<Loaded | null>(null)

  useEffect(() => {
    if (name === null) return
    const controller = new AbortController()
    fetchSite(name, controller.signal)
      .then((detail) => setLoaded({ name, detail, error: null }))
      .catch((err: unknown) => {
        if (!controller.signal.aborted) setLoaded({ name, detail: null, error: errorMessage(err) })
      })
    return () => controller.abort()
  }, [name, refreshKey])

  const current = loaded !== null && loaded.name === name ? loaded : null
  return {
    detail: current?.detail ?? null,
    error: current?.error ?? null,
    loading: name !== null && current === null,
  }
}
