# Grid enrollment service

A standalone service that lets an inference provider join a grid. A grid-admin
mints a single-use token that pins a site name. The site sends the token and a
certificate signing request, and the service signs a certificate under the
pinned name and returns it, the grid CA certificate, and an enrollment id in the
same response, with no separate approval step. The service is Kubernetes-free
by default. It holds the grid certificate authority and signs requests directly
rather than through the Kubernetes CSR API, so a provider with no cluster of its
own can use it.

The `api/enrollment-v1alpha1.yaml` spec still describes an earlier design and is
rewritten in a follow-up. Until then the flow above and the configuration below
describe the service as it runs.

## Build

The binary:

```
cargo build --release -p enrollment --bin enrollment
```

The container image. The build context is the repository root:

```
podman build -f deploy/enrollment/Containerfile -t enrollment .
```

## Run

The service reads its configuration from the environment.

Certificate authority, required. The service holds the grid CA and signs with
it. `ENROLLMENT_CA_CERT` and `ENROLLMENT_CA_KEY` are filesystem paths to the CA
certificate and its private key. Treat the key as the secret the grid is defined
by. `ENROLLMENT_CA_COMMON_NAME` names the CA and defaults to `grid-ca`.
`ENROLLMENT_CERT_LIFETIME_SECS` bounds an issued certificate and defaults to the
built-in site lifetime.

Server TLS, required. The service signs CSRs with the grid CA, so it must not
serve in the clear. `ENROLLMENT_TLS_CERT` and `ENROLLMENT_TLS_KEY` are filesystem
paths to the certificate and key it presents. Both are required, and a missing
one is a hard startup failure before any socket is bound. The certificate is
reloaded from disk periodically, so a rotated secret is picked up without a
restart.

Store. `DB_CONNECTION_URL` is a Postgres connection string, named to match MaaS
so a deployment beside it points at the database already there. It must require
TLS: set `sslmode=verify-full` with a CA bundle, or at least `sslmode=require`.
`disable`, `allow`, and `prefer` permit a plaintext fallback and are refused at
startup. When the variable is unset the records are kept in memory and lost on
restart, which suits a local trial and nothing else.

Listen address. `ENROLLMENT_LISTEN_ADDR` defaults to `0.0.0.0:8080`, HTTPS. On a
stop signal, SIGINT or SIGTERM, the service drains in-flight requests before it
exits, so a rolling deploy does not cut off an enrollment mid-issue.

Grid-admin authorization. `ENROLLMENT_AUTHZ` selects who may mint and revoke
tokens. `local`, the default, reads a grid-admin token table from
`ENROLLMENT_GRID_ADMIN_TOKENS`, one `name:token` line per grid-admin, where an
empty table admits nobody and is the safe direction. `kube` defers to Kubernetes
RBAC through TokenReview and SubjectAccessReview on the `enrollmenttokens`
resource and needs a build with `--features sar`. An unknown value, or a backend
this binary was not built with, fails closed rather than falling back to the
token table.

The image built by `deploy/enrollment/Containerfile` is the standalone build,
without the `sar` feature, so `ENROLLMENT_AUTHZ=kube` fails closed at startup in
it. A Kubernetes-RBAC image is a separate `--features sar` build, tracked as a
follow-up.
