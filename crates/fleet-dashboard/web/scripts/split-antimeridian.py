#!/usr/bin/env python3
"""Split polygon rings that wrap across the antimeridian in the bundled world map.

Natural Earth's 110m country data stores Russia and Fiji with a single ring
whose longitudes wrap past +-180. Leaflet fills such a ring by drawing a
straight line across the whole map, producing a spurious horizontal band. This
script rewrites the asset so each wrapping ring stays within one -180..180
frame: it unwraps the ring's longitudes to a continuous line, then clips the
polygon into 360 degree strips (Sutherland-Hodgman) and shifts each strip back
into frame.

Antarctica is deliberately left whole: it genuinely surrounds the south pole
and spans every longitude, so its full-width fill is the real continent, not a
wrap artifact. After unwrapping it stays within [-180, 180] and passes through
unchanged.

Idempotent: rings that already stay in frame pass through unchanged, so
re-running never alters the asset. Run from the web/ directory:
python3 scripts/split-antimeridian.py
"""
import json
import os

ASSET = os.path.join(os.path.dirname(__file__), "..", "src", "assets", "world-110m.geo.json")


def unwrap(ring):
    """Remove +-360 jumps so the ring is a continuous polyline in longitude."""
    out = [list(ring[0])]
    for lon, lat in ring[1:]:
        prev = out[-1][0]
        while lon - prev > 180:
            lon -= 360
        while lon - prev < -180:
            lon += 360
        out.append([lon, lat])
    return out


def clip_halfplane(poly, keep_right, x):
    """Sutherland-Hodgman clip of a closed polygon to a vertical half-plane."""
    if not poly:
        return []
    res = []
    n = len(poly)
    for i in range(n):
        cur = poly[i]
        nxt = poly[(i + 1) % n]
        cur_in = (cur[0] >= x) if keep_right else (cur[0] <= x)
        nxt_in = (nxt[0] >= x) if keep_right else (nxt[0] <= x)
        if cur_in:
            res.append(cur)
        if cur_in != nxt_in:
            t = (x - cur[0]) / (nxt[0] - cur[0])
            res.append([x, cur[1] + t * (nxt[1] - cur[1])])
    return res


def split_ring(ring):
    """Return one or more rings, each within a single -180..180 frame."""
    uw = unwrap(ring)
    lons = [p[0] for p in uw]
    lo, hi = min(lons), max(lons)
    if lo >= -180 and hi <= 180:
        return [ring]
    k_lo = int((lo + 180) // 360)
    k_hi = int((hi + 180) // 360)
    rings = []
    for k in range(k_lo, k_hi + 1):
        left, right = k * 360 - 180, k * 360 + 180
        clipped = clip_halfplane(clip_halfplane(uw, True, left), False, right)
        if len(clipped) < 3:
            continue
        shifted = [[p[0] - k * 360, p[1]] for p in clipped]
        if shifted[0] != shifted[-1]:
            shifted.append(list(shifted[0]))
        rings.append(shifted)
    return rings


def split_polygon(poly):
    """poly is [outer, hole...]; split each ring and regroup by frame."""
    new_polys = {}
    for ri, ring in enumerate(poly):
        for part in split_ring(ring):
            key = round(sum(p[0] for p in part) / len(part) / 360)
            new_polys.setdefault(key, [None, []])
            if ri == 0:
                new_polys[key][0] = part
            else:
                new_polys[key][1].append(part)
    out = []
    for _, (outer, holes) in sorted(new_polys.items()):
        if outer:
            out.append([outer] + holes)
    return out


def main():
    with open(ASSET) as f:
        data = json.load(f)
    changed = 0
    for feat in data["features"]:
        g = feat["geometry"]
        if g["type"] == "Polygon":
            polys = split_polygon(g["coordinates"])
            if len(polys) == 1:
                g["coordinates"] = polys[0]
            else:
                g["type"] = "MultiPolygon"
                g["coordinates"] = polys
                changed += 1
        elif g["type"] == "MultiPolygon":
            new = []
            before = len(g["coordinates"])
            for poly in g["coordinates"]:
                new.extend(split_polygon(poly))
            if len(new) != before:
                changed += 1
            g["coordinates"] = new
    with open(ASSET, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"split rings in {changed} feature(s); wrote {ASSET}")


if __name__ == "__main__":
    main()
