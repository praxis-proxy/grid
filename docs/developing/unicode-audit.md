# Repository Unicode audit

The audit covers the tracked repository text after integrating main at
`af82146cf9d23fa1fa5d7ffb587fb5053ca23a92`. All 926 regular files decode as
UTF-8 and were scanned, including source, documentation, workflows, scripts,
manifests, lockfiles, and this report. The two tracked symbolic links have ASCII
targets:
`.claude/CLAUDE.md` and `tests/e2e/topologies/grid-glb-demo/deploy`. Their
targets are already in the scanned set. No tracked binary, vendor directory, or
text file was excluded.

Typographic dash punctuation, prose ellipses, and decorative box-drawing
separators were replaced with ASCII punctuation or ordinary section comments.
Generated CRD descriptions carry the same replacements as their Rust source.
The documentation index uses colons consistently between links and descriptions.
Links, command arguments, configuration values, and executable Rust tokens are
preserved; changed runtime strings are diagnostic or assertion messages.

No non-breaking spaces, typographic quotes, zero-width or bidirectional control
characters, private-use characters, or non-ASCII identifier lookalikes remain.
The remaining non-ASCII characters occur in 73 files and are intentional:

| Code points | Count | Purpose |
|---|---:|---|
| U+2014 | 3 | Dashboard missing-data value and two assertions |
| U+2026 | 3 | `safe_truncate_str` suffix and two assertions |
| U+2500 | 208 | Diagram edges and four `cargo tree` matching characters |
| U+2502, U+250C, U+2510 | 57 | Diagram edges |
| U+2514, U+2518, U+252C, U+2534 | 20 | Diagram edges |
| U+25BC | 4 | Direction markers in a provider-flow diagram |
| U+2190, U+2192, U+2193 | 406 | Flow and relationship arrows |
| U+00B1, U+00D7, U+2248, U+2264, U+2265, U+227B | 50 | Math and ordering |
| U+00B7 | 50 | Dashboard display separators and their assertions |
| U+00AB, U+00BB | 2 | Dashboard collapse and expand controls |
| U+00E9, U+20AC | 2 | Multibyte truncation and tool-name test fixtures |
| U+00A7 | 2 | RFC section notation |
| U+274C | 1 | Visible warning in an example README |

The missing-data value is `DOWN_VALUE` in the dashboard's `SitePanel.tsx`.
Changing it would change the UI contract. `crates/xtask/src/env.rs` uses a Unicode
ellipsis to mark truncated evidence. The diagram characters are confined to
`docs/adding-provider.md`, `docs/installation/existing-clusters.md`, and
`crates/swim/src/crypto.rs`; `.github/workflows/tests.yaml` also matches the actual
box-drawing output of `cargo tree`. The accented character in the dashboard
metrics client's test and the euro sign in the MCP probe test exercise UTF-8
byte boundaries. These are functional fixtures, not prose punctuation.

## Reproduce the inventory

Run this from the repository root. It reads every tracked regular file without
following symbolic links and prints each non-ASCII code point's count:

```console
python3 - <<'PY'
import collections
import pathlib
import subprocess
import unicodedata

paths = subprocess.check_output(['git', 'ls-files', '-z']).decode().split('\0')
counts = collections.Counter()
regular = 0
for name in filter(None, paths):
    path = pathlib.Path(name)
    if path.is_symlink():
        continue
    text = path.read_text(encoding='utf-8')
    counts.update(char for char in text if ord(char) > 127)
    regular += 1
print(f'{regular} regular UTF-8 files scanned')
for char, count in sorted(counts.items()):
    print(f'U+{ord(char):04X} {unicodedata.name(char)}: {count}')
PY
```

The existing Unicode Safety Check workflow continues to detect hidden Unicode
in PR diffs. It does not ban the intentional visible characters above; an
additional blanket ASCII gate would reject valid fixtures and diagrams.
