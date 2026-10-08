import { fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import AddressRow from './AddressRow'

describe('AddressRow', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('copies the address to the clipboard and shows confirmation', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
    render(<AddressRow address="gateway.example.internal" />)
    const button = screen.getByRole('button', { name: 'Copy address gateway.example.internal' })
    expect(button).not.toBeDisabled()
    fireEvent.click(button)
    expect(writeText).toHaveBeenCalledWith('gateway.example.internal')
  })

  it('disables the copy button without throwing when navigator.clipboard is absent', () => {
    vi.stubGlobal('navigator', { ...navigator, clipboard: undefined })
    render(<AddressRow address="gateway.example.internal" />)
    const button = screen.getByRole('button', { name: 'Copy address gateway.example.internal' })
    expect(button).toBeDisabled()
    expect(button).toHaveAttribute('title', 'Copy needs a secure (HTTPS) page')
    expect(() => fireEvent.click(button)).not.toThrow()
  })
})
