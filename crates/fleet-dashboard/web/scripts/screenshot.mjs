// Screenshot a URL with headless Chrome over the DevTools protocol.
//
//   node web/scripts/screenshot.mjs <url> <out.png> [width] [height] [settleMs]
//
// Why not --screenshot: with the SSE stream open, --virtual-time-budget never
// settles, and --timeout in the new headless mode captures whichever frame it
// happens to be on. Driving the page ourselves gives a fixed settle time after
// navigation. A fresh --user-data-dir is used every run because the default
// profile hangs headless Chrome in this environment. No dependencies: Node 22
// ships a WebSocket client. The whole DevTools handshake and capture is one
// try/finally so Chrome and its temp profile are cleaned up on every failure
// path (a DevTools-ready timeout, a dropped WebSocket, a bad target, or a
// capture error), not just the happy path.
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const [url, out, width = '1920', height = '1080', settleMs = '15000'] = process.argv.slice(2)
if (!url || !out) {
  console.error('usage: node web/scripts/screenshot.mjs <url> <out.png> [width] [height] [settleMs]')
  process.exit(2)
}

const profile = mkdtempSync(join(tmpdir(), 'fleet-shot-'))
const chrome = spawn(
  process.env.CHROME ?? 'google-chrome',
  [
    '--headless=new',
    '--no-sandbox',
    '--disable-gpu',
    '--disable-dev-shm-usage',
    '--force-prefers-reduced-motion',
    `--user-data-dir=${profile}`,
    '--hide-scrollbars',
    `--window-size=${width},${height}`,
    '--remote-debugging-port=0',
    'about:blank',
  ],
  { stdio: ['ignore', 'ignore', 'pipe'] },
)

// Wait for Chrome to exit before removing the profile it is still writing to.
// If Chrome has already exited (e.g. it crashed or the binary itself failed
// to start), chrome.exitCode/signalCode is already set and a fresh 'exit'
// listener would never fire, so skip straight to removing the profile.
const cleanup = async () => {
  if (chrome.exitCode === null && chrome.signalCode === null) {
    const exited = new Promise((resolve) => chrome.once('exit', resolve))
    chrome.kill('SIGKILL')
    await exited
  }
  rmSync(profile, { recursive: true, force: true, maxRetries: 5 })
}

try {
  const browserWs = await new Promise((resolve, reject) => {
    let buffer = ''
    chrome.stderr.on('data', (chunk) => {
      buffer += String(chunk)
      const match = buffer.match(/DevTools listening on (ws:\/\/\S+)/)
      if (match) resolve(match[1])
    })
    chrome.on('exit', (code) => reject(new Error(`chrome exited with ${code} before DevTools was ready`)))
    setTimeout(() => reject(new Error('timed out waiting for DevTools')), 30_000)
  })
  const port = new URL(browserWs).port
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()
  const page = targets.find((t) => t.type === 'page')
  if (!page) throw new Error('no page target')

  const ws = new globalThis.WebSocket(page.webSocketDebuggerUrl)
  try {
    await new Promise((resolve, reject) => {
      ws.addEventListener('open', resolve, { once: true })
      ws.addEventListener('error', reject, { once: true })
    })
    let nextId = 1
    const pending = new Map()
    ws.addEventListener('message', (event) => {
      const msg = JSON.parse(String(event.data))
      if (msg.id && pending.has(msg.id)) {
        const { resolve, reject } = pending.get(msg.id)
        pending.delete(msg.id)
        if (msg.error) reject(new Error(msg.error.message))
        else resolve(msg.result)
      }
    })
    const send = (method, params = {}) =>
      new Promise((resolve, reject) => {
        const id = nextId++
        pending.set(id, { resolve, reject })
        ws.send(JSON.stringify({ id, method, params }))
      })

    await send('Emulation.setDeviceMetricsOverride', { width: Number(width), height: Number(height), deviceScaleFactor: 1, mobile: false })
    await send('Page.enable')
    await send('Page.navigate', { url })
    await new Promise((resolve) => setTimeout(resolve, Number(settleMs)))
    const { data } = await send('Page.captureScreenshot', { format: 'png' })
    writeFileSync(out, Buffer.from(data, 'base64'))
    console.log(`wrote ${out} (${width}x${height})`)
  } finally {
    ws.close()
  }
} finally {
  await cleanup()
}
