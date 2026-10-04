import { useEffect, useState } from 'react'

const TICK_MS = 1000

/**
 * The current time in epoch ms, re-read every `intervalMs` while `enabled`.
 * Components that show ages ("last seen 45s ago") call this so only they
 * re-render on the tick, never the whole tree.
 */
export function useNow(enabled: boolean, intervalMs = TICK_MS): number {
  const [now, setNow] = useState(() => Date.now())

  useEffect(() => {
    if (!enabled) return
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [enabled, intervalMs])

  return now
}

/**
 * Ticks once a second while `lastUpdate` is set, exposing the age of that
 * timestamp in whole seconds. Isolated from `useFleet` so only the component
 * that renders the age (the live pill) re-renders every second.
 */
export function useAgeTicker(lastUpdate: number | null): number | null {
  const now = useNow(lastUpdate !== null)
  return lastUpdate === null ? null : Math.max(0, Math.floor((now - lastUpdate) / TICK_MS))
}
