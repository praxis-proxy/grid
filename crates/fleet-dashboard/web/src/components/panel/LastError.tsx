export interface LastErrorProps {
  error: string
}

/** Collapsed by default; the raw collector error, monospace, text only. */
export default function LastError({ error }: LastErrorProps) {
  if (!error) return null
  return (
    <details className="border-t border-line px-3 py-2 text-xs">
      <summary className="cursor-pointer text-ink-2 hover:text-ink">Last error</summary>
      <pre className="mt-1 max-h-32 overflow-auto whitespace-pre-wrap break-words font-mono text-down">{error}</pre>
    </details>
  )
}
