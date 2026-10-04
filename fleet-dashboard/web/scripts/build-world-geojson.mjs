// One-off conversion of the world-atlas TopoJSON (Natural Earth 110m countries) into a slim
// GeoJSON FeatureCollection for the offline map fallback. Coordinates are rounded to two
// decimals (about 1 km), which is far below the precision visible at world zoom levels.
//
// Usage:
//   curl -sL https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json -o /tmp/countries-110m.json
//   node scripts/build-world-geojson.mjs /tmp/countries-110m.json src/assets/world-110m.geo.json
import { readFileSync, writeFileSync } from 'node:fs'
import { feature } from 'topojson-client'

const [, , input, output] = process.argv
if (!input || !output) {
  console.error('usage: node scripts/build-world-geojson.mjs <countries-110m.json> <out.geo.json>')
  process.exit(1)
}

const topo = JSON.parse(readFileSync(input, 'utf8'))
const geo = feature(topo, topo.objects.countries)

const round = (n) => Math.round(n * 100) / 100
const roundCoords = (c) => (typeof c[0] === 'number' ? [round(c[0]), round(c[1])] : c.map(roundCoords))

const slim = {
  type: 'FeatureCollection',
  features: geo.features.map((f) => ({
    type: 'Feature',
    properties: { name: f.properties.name },
    geometry: { type: f.geometry.type, coordinates: roundCoords(f.geometry.coordinates) },
  })),
}

writeFileSync(output, JSON.stringify(slim))
console.log(`${slim.features.length} features written to ${output}`)
