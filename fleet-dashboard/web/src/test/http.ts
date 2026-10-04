import { vi } from 'vitest'

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

/**
 * Stub global fetch with a path-prefix router. Keys are path prefixes such as
 * '/api/v1/fleet' or '/api/v1/sites/'; the query string is ignored when matching.
 * A route value is JSON-encoded as a 200 response, unless it is already a
 * Response (e.g. built with `jsonResponse(body, 503)`) for a non-200 case.
 */
export function stubFetchRoutes(routes: Record<string, unknown>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
    const path = url.split('?')[0]
    for (const [prefix, body] of Object.entries(routes)) {
      if (path === prefix || path.startsWith(prefix)) {
        return body instanceof Response ? body.clone() : jsonResponse(body)
      }
    }
    return jsonResponse({ error: 'not found' }, 404)
  })
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}
