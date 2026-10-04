import { useCallback, useEffect, useState } from 'react'

const PARAM = 'site'

/** Where a selection came from, so the map pans for roster and deep-link picks but not for its own clicks. */
export type SelectionSource = 'url' | 'map' | 'roster'

function readParam(): string | null {
  return new URLSearchParams(window.location.search).get(PARAM)
}

function writeParam(name: string | null): void {
  const url = new URL(window.location.href)
  if (name === null) url.searchParams.delete(PARAM)
  else url.searchParams.set(PARAM, name)
  window.history.replaceState(window.history.state, '', url)
}

export interface SelectedSite {
  selected: string | null
  /** null when nothing is selected. */
  source: SelectionSource | null
  select: (name: string, source?: SelectionSource) => void
  clear: () => void
}

interface Selection {
  name: string | null
  source: SelectionSource | null
}

function fromUrl(): Selection {
  const name = readParam()
  return { name, source: name === null ? null : 'url' }
}

/** Selected site name, mirrored into the ?site= query parameter for deep links. */
export function useSelectedSite(): SelectedSite {
  const [selection, setSelection] = useState<Selection>(fromUrl)

  useEffect(() => {
    const onPopState = () => setSelection(fromUrl())
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [])

  const select = useCallback((name: string, source: SelectionSource = 'roster') => {
    writeParam(name)
    setSelection({ name, source })
  }, [])

  const clear = useCallback(() => {
    writeParam(null)
    setSelection({ name: null, source: null })
  }, [])

  return { selected: selection.name, source: selection.source, select, clear }
}
