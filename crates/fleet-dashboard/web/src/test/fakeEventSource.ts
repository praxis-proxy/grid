import type { FleetSnapshot } from '../api/types'

type Listener = (ev: Event) => void

/** Minimal EventSource double. Install with vi.stubGlobal('EventSource', FakeEventSource). */
export class FakeEventSource {
  static instances: FakeEventSource[] = []

  static reset(): void {
    FakeEventSource.instances = []
  }

  readonly url: string
  closed = false
  onerror: ((ev: Event) => void) | null = null
  private readonly listeners = new Map<string, Listener[]>()

  constructor(url: string) {
    this.url = url
    FakeEventSource.instances.push(this)
  }

  addEventListener(type: string, listener: Listener): void {
    const list = this.listeners.get(type) ?? []
    list.push(listener)
    this.listeners.set(type, list)
  }

  close(): void {
    this.closed = true
  }

  emitOpen(): void {
    for (const listener of this.listeners.get('open') ?? []) listener(new Event('open'))
  }

  emitFleet(snapshot: FleetSnapshot): void {
    const ev = new MessageEvent('fleet', { data: JSON.stringify(snapshot) })
    for (const listener of this.listeners.get('fleet') ?? []) listener(ev)
  }

  emitError(): void {
    this.onerror?.(new Event('error'))
  }
}
