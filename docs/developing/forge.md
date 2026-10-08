# Forge tooling

Grid consumes the `praxis-forge` CLI from
[Praxis Forge](https://github.com/praxis-proxy/forge). Grid owns its topology
files, qualification assertions, and image preparation. Generic environment
orchestration lives upstream.

## Install and run

From the Grid repository root:

```console
./scripts/forge.sh install
./scripts/forge.sh --version
./scripts/forge.sh --config tests/e2e/topologies/grid-provider-traffic/forge.yaml config validate
```

The installer uses `cargo install --locked --git` with the exact commit in
`scripts/forge-revision`. It requires Grid's Rust toolchain, Cargo, Bash, and
`sha256sum`. It stores the executable below
`target/tools/forge/<revision>/<compiler-fingerprint>/bin/`. The compiler
fingerprint includes the Rust host target, release, and commit. The installation
records the repository, source revision, compiler fingerprint, host target, and
binary checksum. Resolution checks that metadata and checksum and reports the
chosen executable on stderr. A Cargo version string alone does not establish
which upstream commit was installed.

`./scripts/forge.sh path` returns the absolute executable path. The xtask
qualifications and both Forge E2E shell scripts use this same resolver.
Install explicitly before running a qualification; missing or invalid cache
entries produce the installation command. Old `target/debug/praxis-forge`
executables and arbitrary `PATH` installations are not fallback candidates.

Use an explicit executable override for an upstream development build:

```console
FORGE_BIN=/absolute/path/to/praxis-forge cargo xtask env run-grid-glb-demo --help
```

An override may also name a command on `PATH`. The resolver checks that it is
an executable file and reports that the upstream pin is bypassed. Unset
`FORGE_BIN` before installing the pinned build. The wrapper preserves the
caller's working directory when running Forge.

## Compatibility requirements

The initial candidate, `7c83f60cc5677b79318aba7508b7e2a39a1a1fe2`, cannot be used
for this migration. Grid's persisted state includes `networkCreatedByForge`;
that candidate rejects the field and removes reused networks on teardown.
It also lacks Grid's Docker/Podman dual-stack subnet extraction. The pinned
[upstream prerequisite](https://github.com/praxis-proxy/forge/pull/23)
preserves these contracts:

- Read Grid state and treat missing historical ownership or network instance IDs
  conservatively. Legacy networks are preserved, including those with an old
  `networkCreatedByForge: true` flag: that flag cannot distinguish replacement
  networks that share a name and labels.
- Record a newly created network's runtime ID and retain ownership across
  repeated bring-up only when that instance still matches. Deletion uses the
  recorded runtime ID and existing environment-label checks. A replacement
  network cannot inherit the earlier lifecycle's deletion authority.
- Checkpoint a per-attempt creation marker before allocating a network. If ID
  lookup fails, repeated bring-up or teardown recovers the ID only when the
  network's marker and environment labels match. A replacement under the same
  name cannot inherit the failed attempt.
- Preserve reused networks during teardown, including `down --force`.
  Existing same-environment labels remain necessary before deleting an owned
  network. Force suppresses confirmation; it does not establish ownership.
- Accept Docker and Podman inspect shapes and select IPv4 when IPv6 comes first,
  both for environment networking and MetalLB pool allocation.

Keep existing state and resources intact while upgrading. Do not erase the
ownership field or discard a state directory to make the old candidate load.
After migration, review any retained legacy network and remove it explicitly
only after verifying that no environment still uses it. Never fill a missing
network ID from a same-name network to manufacture ownership. Cluster cleanup
continues normally even when network deletion authority is unavailable.
When changing the pin, review upstream CLI, configuration, and state changes
and use the existing release qualification procedures. `--log`/`FORGE_LOG`
from the old in-tree CLI is absent upstream; Grid has no caller of that option.
Upstream also validates more configuration fields and reports unhealthy
services as bring-up failures, so acceptance requires runtime qualification.

## CI and upgrades

The Helm workflow builds the retired Grid CLI from immutable Grid commit
`a4a7d7168ec36e59f31bcb9be0d11d6dd69c4974` alongside the upstream pin. It
validates every `tests/e2e/topologies/*/forge.yaml` with both binaries, from its
own directory and from an unrelated directory using an absolute configuration
path. Parsed configurations and plans must match across all four combinations;
the workflow uploads the full comparison evidence. This baseline build is the
only remaining `cargo -p forge` invocation, explicitly isolated from Grid's
current workspace and production resolver.

A disposable Kind fixture then creates resources and state with the retired CLI
and resumes them with upstream. It verifies repeated bring-up and teardown,
relocated template assets and config-relative script arguments while preserving
the caller's exec working directory for both CLIs, preservation of a
pre-existing network, and cleanup after an injected cluster-creation failure.
This checks lifecycle compatibility independently of the static plan comparison.

Existing standalone gateway and hub/site jobs use the same pinned installation.
The triggered GLB workflow installs the pin from the selected Grid checkout and
provides representative multi-site routing evidence. Dispatch that workflow for
the exact PR head when changing this pin. These jobs cover representative live
scenarios; the static comparison covers all nine topology configurations.
Upstream CI also executes the compatibility regressions in the prerequisite PR.

To upgrade, change the revision file to a reviewed 40-character upstream SHA,
reinstall, and repeat the compatibility and runtime checks. Cache entries are
separated by revision and compiler. Roll back source changes through the normal
Git workflow, retaining the state needed by the binary that created resources;
a prior revision must support that state's schema before it can safely clean up.
