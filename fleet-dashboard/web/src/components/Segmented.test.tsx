import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import Segmented from './Segmented'

const options = [
  { value: 'a', label: 'Alpha' },
  { value: 'b', label: 'Beta' },
] as const

describe('Segmented', () => {
  it('marks the current option pressed and shows the prefix', () => {
    render(<Segmented label="Pick" prefix="Mode" options={options} value="b" onChange={() => {}} />)
    expect(screen.getByRole('group', { name: 'Pick' })).toHaveTextContent('Mode')
    expect(screen.getByRole('button', { name: 'Alpha' })).toHaveAttribute('aria-pressed', 'false')
    expect(screen.getByRole('button', { name: 'Beta' })).toHaveAttribute('aria-pressed', 'true')
  })
  it('reports the clicked option', () => {
    const onChange = vi.fn()
    render(<Segmented label="Pick" options={options} value="a" onChange={onChange} />)
    fireEvent.click(screen.getByRole('button', { name: 'Beta' }))
    expect(onChange).toHaveBeenCalledWith('b')
  })
  it('uses 40 px targets in the md size', () => {
    render(<Segmented label="Pick" options={options} value="a" onChange={() => {}} size="md" />)
    expect(screen.getByRole('button', { name: 'Alpha' }).className).toContain('h-10')
  })
})
