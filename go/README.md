# github.com/praxis-proxy/grid/go  -  Go API types for Grid CRDs

Go structs for the four public Grid CRDs (`grid.praxis.fast/v1alpha1`),
derived from the canonical Rust definitions in `crates/operator/src/crd`.

External Go controllers can import this module instead of using
`unstructured.Unstructured`, gaining compile-time field checking, clearer
resource construction, and a shared, release-versioned API contract.

## Requirements

- Go 1.23+
- `k8s.io/apimachinery` v0.32.x (currently v0.32.1)

The module is intended to track Grid CRD versions. After a Grid operator upgrade that changes the CRD schemas, regenerate and release this module before relying on full-object updates.

## Install

```sh
go get github.com/praxis-proxy/grid/go@latest
```

## Usage

Register the types with your controller-runtime scheme:

```go
import (
    utilruntime "k8s.io/apimachinery/pkg/util/runtime"
    v1alpha1 "github.com/praxis-proxy/grid/go/api/v1alpha1"
)

func init() {
    utilruntime.Must(v1alpha1.AddToScheme(scheme))
}
```

Then use typed objects normally:

```go
var ip v1alpha1.InferenceProvider
if err := r.Get(ctx, req.NamespacedName, &ip); err != nil {
    return ctrl.Result{}, client.IgnoreNotFound(err)
}
```


## Covered CRDs

| Kind | Group/Version |
|------|---------------|
| `InferenceProvider` | `grid.praxis.fast/v1alpha1` |
| `GridNetwork` | `grid.praxis.fast/v1alpha1` |
| `GridSite` | `grid.praxis.fast/v1alpha1` |
| `AgentToolProvider` | `grid.praxis.fast/v1alpha1` |

## Keeping types in sync

The Go types in `api/v1alpha1/zz_generated_*.go` are generated from the
deployed CRD schemas in `deploy/crds/`. After every `make generate-crds` run,
regenerate and verify:

```sh
make generate-go-types   # regenerates from deploy/crds/*.yaml, then go vet
make go-types-check      # runs serialization and drift tests
```

The generator lives at `go/cmd/gen-types/main.go` and reads the OpenAPI v3
schemas embedded in the CRD manifests. Type names are derived from the CRD
field paths; well-known shared types (SecretRef, AuthConfig, SelectorConfig,
EndpointTLSConfig, etc.) keep stable friendly names via detectors in the
generator. Enum values get a matching string-type alias with constants.

A `go-types-check` failure means a field was added, renamed, or restructured
in the Rust CRDs and the Go types need to be regenerated.

## Version skew and patch safety

These types do **not** prevent field loss from API version skew.

A full-object update (`client.Update`, a PUT-equivalent) serialises only the
fields declared in this module. Any field introduced by a newer installed Grid
schema  -  but absent from this module version  -  will be silently dropped from
the live object.

To avoid unintended field loss:

- Prefer **server-side apply** (`types.ApplyPatchType`) and declare only the
  fields your controller owns. SSA merges your intent with fields set by other
  managers rather than replacing the full object.
- Avoid `client.Update` on resources that other controllers or users may have
  extended with fields not present in this module version.
- After upgrading the Grid operator, update this module to a release that
  includes the new schema fields before performing any full-object updates.

## Releases and versioning

This module lives in a subdirectory of the `praxis-proxy/grid` repository.
Tags follow the submodule convention: `go/v0.x.y` (not `v0.x.y`), so
`go get` resolves the correct tree:

```sh
go get github.com/praxis-proxy/grid/go@v0.1.0
```

Tags follow the `go/vX.Y.Z` convention required by Go's module proxy for
subdirectory modules. Tags are intended to align with operator releases
so the Go types match the CRD schemas shipped in that release. See
`make release-go-types` for the tagging workflow.
