import { useEffect, useState } from 'react'

export interface AddressRowProps {
  address: string
}

const COPIED_MS = 1500

const CLIPBOARD_TITLE = 'Copy needs a secure (HTTPS) page'

export default function AddressRow({ address }: AddressRowProps) {
  const [copied, setCopied] = useState(false)
  // navigator.clipboard is only present on secure (HTTPS or localhost) origins.
  const hasClipboard = typeof navigator !== 'undefined' && !!navigator.clipboard

  useEffect(() => {
    if (!copied) return
    const id = setTimeout(() => setCopied(false), COPIED_MS)
    return () => clearTimeout(id)
  }, [copied])

  const copy = () => {
    if (!hasClipboard) return
    void navigator.clipboard
      .writeText(address)
      .then(() => setCopied(true))
      .catch(() => setCopied(false))
  }

  if (!address) return null
  return (
    <div className="flex items-center gap-2 border-t border-line px-3 py-2 text-xs">
      <span className="min-w-0 flex-1 truncate font-mono text-ink-2" title={address}>
        {address}
      </span>
      <button
        type="button"
        onClick={copy}
        disabled={!hasClipboard}
        aria-label={`Copy address ${address}`}
        title={hasClipboard ? undefined : CLIPBOARD_TITLE}
        className="h-6 shrink-0 rounded border border-line px-2 text-ink-2 hover:text-ink disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:text-ink-2"
      >
        {copied ? 'Copied' : 'Copy'}
      </button>
      <span role="status" className="sr-only">
        {copied ? 'Address copied' : ''}
      </span>
    </div>
  )
}
