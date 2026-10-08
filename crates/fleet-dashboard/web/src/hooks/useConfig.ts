import { useEffect, useState } from 'react'
import { fetchConfig } from '../api/client'
import type { AppConfig, Thresholds } from '../api/types'

/** Mirrors the Go defaults in internal/queries.DefaultThresholds so tiles label correctly before /config resolves. */
export const DEFAULT_THRESHOLDS: Thresholds = { gpuUtilWarn: 90, queueWarn: 50, latencyWarnMs: 5000 }

export const DEFAULT_CONFIG: AppConfig = {
  hub: null,
  pollIntervalSeconds: 15,
  version: '',
  thresholds: DEFAULT_THRESHOLDS,
  user: null,
}

export interface UseConfigResult extends AppConfig {
  /** True once the initial /api/v1/config request has settled, success or failure. */
  loaded: boolean
}

/** Returns DEFAULT_CONFIG until /api/v1/config resolves; keeps the defaults if it fails. */
export function useConfig(): UseConfigResult {
  const [config, setConfig] = useState<AppConfig>(DEFAULT_CONFIG)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    const controller = new AbortController()
    fetchConfig(controller.signal)
      .then((loadedConfig) => {
        setConfig(loadedConfig)
        setLoaded(true)
      })
      .catch((err: unknown) => {
        if (!controller.signal.aborted) {
          console.warn('config unavailable, using defaults:', err)
          setLoaded(true)
        }
      })
    return () => controller.abort()
  }, [])

  return { ...config, loaded }
}
