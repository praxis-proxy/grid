# github.com/praxis-proxy/grid/go  -  Go API types for Grid CRDs

Go structs for the public Grid CRDs (`grid.praxis.fast/v1alpha1`),
derived from the canonical Rust definitions in `crates/operator/src/crd`.

External Go controllers can import this module instead of using
`unstructured.Unstructured`, gaining compile-time field checking, clearer
resource construction, and a shared, release-versioned API contract.

## Requirements

- Go 1.24+
- `k8s.io/apimachinery` v0.32.x (currently v0.32.13)

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
| `SiteInvite` | `grid.praxis.fast/v1alpha1` |

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
field paths; well-known shared types (ObjectKeyRef, SecretKeyRef, AuthConfig, SelectorConfig,
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

- **Do not use these types for server-side apply.** The generated structs are
  not valid partial-apply configurations. Required scalar fields (`Endpoint`,
  `GridNetworkRef`, etc.) have no `omitempty` and always serialise, even when
  zero. `encoding/json` also always emits struct-value fields regardless of
  `omitempty`. Sending a partially-filled typed object with
  `types.ApplyPatchType` will claim those empty fields for your field manager
  and can overwrite live values or fail CRD validation.
- For **partial changes**, use `client.Patch` with `client.MergeFrom(original)`.
  It diffs the modified object against the original and sends only the changed
  fields, leaving unset fields untouched.
- For **server-side apply**, use `unstructured.Unstructured` or a dedicated
  apply-configuration type that expresses only the fields you own.
- Avoid `client.Update` on resources that other controllers or users may have
  extended with fields not present in this module version.
- After upgrading the Grid operator, update this module to a release that
  includes the new schema fields before performing any full-object updates.

## Releases and versioning

This module lives in a subdirectory of the `praxis-proxy/grid` repository.
Tags use the `go/vX.Y.Z` prefix required by Go's module proxy for subdirectory
modules (not a bare `vX.Y.Z`), so `go get` resolves the correct tree:

```sh
go get github.com/praxis-proxy/grid/go@v0.1.0
```

Tags are intended to align with operator releases so the Go types match the CRD
schemas shipped in that release. Tagging is a manual step not yet covered by
the operator release automation in `docs/release.md`. After regenerating and
merging Go types for a release, create and push the tag by hand:

```sh
git tag go/v<X>.<Y>.<Z>
git push upstream go/v<X>.<Y>.<Z>
```
