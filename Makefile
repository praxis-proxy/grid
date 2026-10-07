# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
V                ?=
NIGHTLY_RUSTFMT   ?= nightly-2026-03-28
KIND_CLUSTER_NAME ?= praxis-grid
PROJECT_IMAGE    ?= praxis-grid:dev

ifneq ($(V),)
  _NOCAPTURE := -- --nocapture
endif

.PHONY: all build release check clean \
	test test-unit lint gateway-lint lint-extra fmt doc audit \
	generate-api-types codegen-check generate-crds crds-check \
	coverage coverage-check \
	mutants semver publish-dry-run \
	require-container-engine \
	images container operator-image gateway-image \
	mock-providers-image overlay-sync-image fleet-dashboard-image fleet-dashboard-web glb-demo-images \
	kind-up kind-down \
	dev-env dev-push \
	setup-hooks \
	helm-lint helm-test praxis-gateway-e2e \
	help

# -------------------------------------------------------------------
# All
# -------------------------------------------------------------------

all: build fmt lint test audit

# -------------------------------------------------------------------
# Build
# -------------------------------------------------------------------

build:
	cargo build --workspace

release:
	cargo build --workspace --release

check:
	cargo check --workspace

clean:
	cargo clean

# -------------------------------------------------------------------
# Test
# -------------------------------------------------------------------

test: test-unit

test-unit:
	cargo test --locked --workspace $(_NOCAPTURE)

# -------------------------------------------------------------------
# Quality
# -------------------------------------------------------------------

lint: gateway-lint
	cargo clippy --locked --workspace --all-targets -- -D warnings
	cargo +$(NIGHTLY_RUSTFMT) fmt --all -- --check
	cargo machete

gateway-lint:
	cargo clippy --manifest-path gateway/Cargo.toml --workspace --all-targets -- -D warnings
	cargo +$(NIGHTLY_RUSTFMT) fmt --manifest-path gateway/Cargo.toml --all -- --check
	cargo machete gateway
	@set -eu; \
	  tree="$$(cargo tree --manifest-path gateway/Cargo.toml -p gateway -e normal --target all --prefix none --format '{p}')"; \
	  printf '%s\n' "$$tree" | grep -q '^openssl-sys ' || { echo "positive control failed: openssl-sys is not in the tree" >&2; exit 1; }; \
	  if printf '%s\n' "$$tree" | grep -q '^ring '; then echo "ring is in the gateway's normal dependency tree" >&2; exit 1; fi

fmt:
	cargo +$(NIGHTLY_RUSTFMT) fmt --all

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items

# -------------------------------------------------------------------
# Codegen
# -------------------------------------------------------------------

# Regenerate the enrollment wire types from api/enrollment-v1alpha1.yaml.
generate-api-types:
	cargo run --quiet -p xtask -- generate-api-types

# Fail if the checked-in wire types no longer match the spec.
codegen-check:
	cargo run --quiet -p xtask -- check-api-types

# Regenerate the CRD manifests in deploy/crds and charts/grid-operator/templates/crds
# from the Rust types in operator/src/crd.
generate-crds:
	./scripts/generate-deployment-crds.sh

# Fail if the checked-in CRD manifests no longer match the Rust types.
crds-check:
	./scripts/generate-deployment-crds.sh --check

audit:
	cargo audit
	cargo deny check

lint-extra:
	typos
	taplo fmt --check
	shellcheck .hooks/pre-commit
	actionlint

mutants:
	cargo mutants --workspace

semver:
	cargo semver-checks

publish-dry-run:
	cargo package --workspace --allow-dirty

coverage:
	cargo llvm-cov --workspace --html --output-dir target/coverage \
		--exclude xtask \
		--ignore-filename-regex '(target/|tests/)'

# Coverage gate is 80% lines; ratchet up incrementally.
coverage-check:
	cargo llvm-cov --workspace --json \
		--exclude xtask \
		--ignore-filename-regex '(target/|tests/)' \
		--fail-under-lines 80 \
		--output-path coverage.json

# -------------------------------------------------------------------
# Container
# -------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Build provenance passed into every image. .dockerignore excludes .git, so the
# build cannot resolve these itself and an image built without them says so.
# ---------------------------------------------------------------------------
# Both are git-derived and reach a shell recipe below, and a tag is whatever
# someone named it, so drop anything outside the characters a tag or a describe
# string legitimately uses rather than trusting the value.
# The filter is the last command in each pipeline and succeeds on empty input,
# so test the captured value rather than the pipeline's exit status.
SAFE = tr -cd 'A-Za-z0-9._/+-'
GRID_GIT_COMMIT ?= $(shell c=$$(git rev-parse HEAD 2>/dev/null | $(SAFE)); echo "$${c:-unknown}")
GRID_GIT_VERSION ?= $(shell v=$$(git describe --tags --always 2>/dev/null | $(SAFE)); echo "$${v:-$(shell grep -m1 '^version' Cargo.toml | cut -d'"' -f2)}")
# Empty output is clean only when git status SUCCEEDED; a failed status also
# prints nothing, and reading that as clean labels an unknown tree clean.
GRID_GIT_TREE_STATE ?= $(shell s=$$(git status --porcelain 2>/dev/null) && { test -z "$$s" && echo clean || echo dirty; } || echo unknown)
GRID_BUILD_DATE ?= $(shell date -u +%Y%m%d)
BUILD_ARGS = --build-arg 'GRID_GIT_COMMIT=$(GRID_GIT_COMMIT)' \
	--build-arg 'GRID_GIT_VERSION=$(GRID_GIT_VERSION)' \
	--build-arg 'GRID_GIT_TREE_STATE=$(GRID_GIT_TREE_STATE)' \
	--build-arg 'GRID_BUILD_DATE=$(GRID_BUILD_DATE)'

require-container-engine:
ifndef CONTAINER_ENGINE
	$(error No container engine found. Install podman or docker)
endif

container: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -t $(PROJECT_IMAGE) -f Containerfile .

images: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -t $(PROJECT_IMAGE) -f Containerfile .

operator-image: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f deploy/operator/Containerfile -t grid-operator:latest .

gateway-image: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f deploy/gateway/Containerfile -t grid-gateway:latest .

mock-providers-image: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f mock-providers/Containerfile -t grid-mock-providers:latest .

overlay-sync-image: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f overlay-sync/Containerfile -t grid-overlay-sync:latest .

fleet-dashboard-image: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f fleet-dashboard/Containerfile -t grid-fleet-dashboard:latest .

# Builds the dashboard UI and stages it where fleet-dashboard/build.rs embeds it.
fleet-dashboard-web:
	npm --prefix fleet-dashboard/web ci --no-audit --no-fund
	npm --prefix fleet-dashboard/web run build
	rm -rf fleet-dashboard/webui/dist && mkdir -p fleet-dashboard/webui && cp -r fleet-dashboard/web/dist fleet-dashboard/webui/dist

# GLB demo images — deterministic :glb-demo tags, no :latest dependency.
glb-demo-images: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f deploy/operator/Containerfile -t grid-operator:glb-demo .
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -f mock-providers/Containerfile -t grid-mock-providers:glb-demo .

# -------------------------------------------------------------------
# Helm
# -------------------------------------------------------------------

helm-lint:
	./scripts/verify-helm-chart.sh

helm-test:
	KIND=1 ./scripts/verify-helm-chart.sh

praxis-gateway-e2e:
	./scripts/e2e-praxis-gateway.sh

# -------------------------------------------------------------------
# KIND
# -------------------------------------------------------------------

kind-up: images
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) \
	bash hack/setup-kind.sh

kind-down:
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) \
	bash hack/teardown-kind.sh

# -------------------------------------------------------------------
# Iterative Development
# -------------------------------------------------------------------

dev-env: images
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) \
	bash hack/setup-kind.sh

dev-push: | require-container-engine
	$(CONTAINER_ENGINE) build $(BUILD_ARGS) -t $(PROJECT_IMAGE) -f Containerfile .
	kind load docker-image $(PROJECT_IMAGE) --name $(KIND_CLUSTER_NAME)

# -------------------------------------------------------------------
# Dev Setup
# -------------------------------------------------------------------

setup-hooks:
	@ln -sf ../../.hooks/pre-commit .git/hooks/pre-commit
	@echo "Git hooks installed"

# -------------------------------------------------------------------
# Help
# -------------------------------------------------------------------

help:
	@echo "Variables:"
	@echo "  V=1                show test output (--nocapture)"
	@echo "  CONTAINER_ENGINE   container runtime (auto-detected)"
	@echo "  KIND_CLUSTER_NAME  KIND cluster name"
	@echo "  PROJECT_IMAGE      container image tag"
	@echo ""
	@echo "Top-level:"
	@echo "  all              build + fmt + lint + test + audit"
	@echo ""
	@echo "Build:"
	@echo "  build            cargo build --workspace"
	@echo "  release          cargo build --workspace --release"
	@echo "  check            cargo check --workspace"
	@echo "  clean            cargo clean"
	@echo ""
	@echo "Test:"
	@echo "  test             run workspace tests (ignored tests excluded)"
	@echo ""
	@echo "Quality:"
	@echo "  lint             root checks + gateway-lint"
	@echo "  gateway-lint     Gateway Clippy + rustfmt + machete + no-ring check"
	@echo "  lint-extra       typos + taplo + shellcheck + actionlint"
	@echo "  fmt              format with nightly rustfmt"
	@echo "  doc              build docs with warnings denied"
	@echo "  audit            cargo audit + cargo deny"
	@echo "  coverage         HTML coverage report"
	@echo "  coverage-check   fail if line coverage < 80%%"
	@echo "  mutants          mutation testing (cargo mutants)"
	@echo "  semver           semver compatibility check"
	@echo "  publish-dry-run  cargo package verification"
	@echo ""
	@echo "Helm:"
	@echo "  helm-lint        lint, template, schema, CRD sync, package"
	@echo "  helm-test        helm-lint + Kind install/upgrade/test/uninstall"
	@echo "  praxis-gateway-e2e  Forge Kind run of the standalone gateway chart"
	@echo ""
	@echo "Container:"
	@echo "  container            build container image"
	@echo "  images               build container image"
	@echo "  operator-image       build operator container image"
	@echo "  gateway-image        build gateway container image"
	@echo "  mock-providers-image build mock-providers container image"
	@echo "  overlay-sync-image   build overlay-sync sidecar image"
	@echo "  fleet-dashboard-image build fleet dashboard image (opt-in hub web UI)"
	@echo "  fleet-dashboard-web   build the dashboard UI and stage it for cargo build"
	@echo "  glb-demo-images      build all Grid images tagged :glb-demo"
	@echo ""
	@echo "KIND:"
	@echo "  kind-up          build image + create/reuse Kind base (Gateway API, MetalLB)"
	@echo "  kind-down        delete cluster"
	@echo ""
	@echo "Dev Setup:"
	@echo "  setup-hooks      install git pre-commit hook"
	@echo ""
	@echo "Development:"
	@echo "  dev-env          build image + create/reuse Kind development base"
	@echo "  dev-push         build + load image into Kind (no rollout)"
