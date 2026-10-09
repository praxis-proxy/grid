import { fireEvent, render, screen } from '@testing-library/react'
import { useRef } from 'react'
import { describe, expect, it } from 'vitest'
import { focusableWithin, useFocusTrap } from './useFocusTrap'

function Dialog() {
  const ref = useRef<HTMLDivElement>(null)
  const closeRef = useRef<HTMLButtonElement>(null)
  useFocusTrap(ref, closeRef)
  return (
    <div ref={ref} role="dialog" aria-label="Test">
      <button type="button">First</button>
      <button type="button" ref={closeRef}>
        Close
      </button>
      <a href="#last">Last</a>
    </div>
  )
}

describe('useFocusTrap', () => {
  it('focuses the initial element, wraps Tab in both directions, and restores focus on unmount', () => {
    const trigger = document.createElement('button')
    document.body.appendChild(trigger)
    trigger.focus()

    const { unmount } = render(<Dialog />)
    const dialog = screen.getByRole('dialog')
    const first = screen.getByRole('button', { name: 'First' })
    const close = screen.getByRole('button', { name: 'Close' })
    const last = screen.getByRole('link', { name: 'Last' })
    expect(document.activeElement).toBe(close)

    last.focus()
    fireEvent.keyDown(dialog, { key: 'Tab' })
    expect(document.activeElement).toBe(first)

    fireEvent.keyDown(dialog, { key: 'Tab', shiftKey: true })
    expect(document.activeElement).toBe(last)

    unmount()
    expect(document.activeElement).toBe(trigger)
    trigger.remove()
  })
})

describe('focusableWithin', () => {
  it('includes a summary inside a details element', () => {
    const root = document.createElement('div')
    root.innerHTML = '<details><summary>Last error</summary><pre>boom</pre></details>'
    document.body.appendChild(root)
    const items = focusableWithin(root)
    expect(items).toHaveLength(1)
    expect(items[0].tagName).toBe('SUMMARY')
    root.remove()
  })
})
