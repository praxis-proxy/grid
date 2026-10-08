import { describe, expect, it } from 'vitest'
import { mainColumns } from './layout'

describe('mainColumns', () => {
  it('docks a 340 px panel column only while a site is selected and narrows the roster when collapsed', () => {
    expect(mainColumns(false, false)).toBe('280px minmax(0,1fr)')
    expect(mainColumns(false, true)).toBe('280px minmax(0,1fr) 340px')
    expect(mainColumns(true, true)).toBe('56px minmax(0,1fr) 340px')
    expect(mainColumns(true, false)).toBe('56px minmax(0,1fr)')
  })
})
