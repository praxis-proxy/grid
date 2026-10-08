/** DOM ids linking a tab to its panel (aria-controls / aria-labelledby). */
export function tabId(id: string): string {
  return `tab-${id}`
}

export function tabPanelId(id: string): string {
  return `tabpanel-${id}`
}
