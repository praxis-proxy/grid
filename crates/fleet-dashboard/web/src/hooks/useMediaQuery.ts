import { useSyncExternalStore } from 'react'

function subscribe(query: string, onChange: () => void): () => void {
  if (typeof window.matchMedia !== 'function') return () => {}
  const list = window.matchMedia(query)
  list.addEventListener('change', onChange)
  return () => list.removeEventListener('change', onChange)
}

function matches(query: string): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia(query).matches
}

/** True while the media query matches. False where matchMedia is unavailable (jsdom). */
export function useMediaQuery(query: string): boolean {
  return useSyncExternalStore(
    (onChange) => subscribe(query, onChange),
    () => matches(query),
    () => false,
  )
}

export const NARROW_SCREEN_QUERY = '(max-width: 1279px)'
export const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)'
