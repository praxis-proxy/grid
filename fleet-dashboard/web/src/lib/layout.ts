export const TOP_BAR_HEIGHT = 48
export const BOTTOM_STRIP_HEIGHT = 200
export const ROSTER_WIDTH = 280
export const ROSTER_COLLAPSED_WIDTH = 56
export const PANEL_WIDTH = 340

/** Grid columns for main: roster (280 or 56), map (flexible), panel (340 while a site is selected). */
export function mainColumns(rosterCollapsed: boolean, panelOpen: boolean): string {
  const roster = `${rosterCollapsed ? ROSTER_COLLAPSED_WIDTH : ROSTER_WIDTH}px`
  return panelOpen ? `${roster} minmax(0,1fr) ${PANEL_WIDTH}px` : `${roster} minmax(0,1fr)`
}
