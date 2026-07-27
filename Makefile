# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
V                ?=
NIGHTLY_RUSTFMT   ?= nightly-2026-03-28
KIND_CLUSTER_NAME ?= praxis-grid
PROJECT_IMAGE    ?= praxis-grid:dev
KUBECTL          ?= kubectl --context kind-$(KIND_CLUSTER_NAME)

ifneq ($(V),)
  _NOCAPTURE := -- --nocapture
endif

.PHONY: all build release check clean \
	test test-unit lint fmt doc audit \
	coverage coverage-check \
	require-container-engine \
	images container operator-image overlay-sync-image \
	mock-providers-image glb-demo-images \
	kind-up kind-down \
	dev-env dev-push dev-integration \
	setup-hooks \
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
	cargo test --workspace $(_NOCAPTURE)

# -------------------------------------------------------------------
# Quality
# -------------------------------------------------------------------

lint:
	cargo clippy --workspace --all-targets -- -D warnings
	cargo +$(NIGHTLY_RUSTFMT) fmt --all -- --check
	cargo machete

fmt:
	cargo +$(NIGHTLY_RUSTFMT) fmt --all

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items

audit:
	cargo audit
	cargo deny check

coverage:
	cargo llvm-cov --workspace --html --output-dir target/coverage \
		--exclude xtask \
		--ignore-filename-regex '(target/|tests/)'

coverage-check:
	cargo llvm-cov --workspace --json \
		--exclude xtask \
		--ignore-filename-regex '(target/|tests/)' \
		--fail-under-lines 80 \
		--output-path coverage.json

# -------------------------------------------------------------------
# Container
# -------------------------------------------------------------------

require-container-engine:
ifndef CONTAINER_ENGINE
	$(error No container engine found. Install podman or docker)
endif

container: | require-container-engine
	$(CONTAINER_ENGINE) build -t $(PROJECT_IMAGE) -f Containerfile .

images: | require-container-engine
	$(CONTAINER_ENGINE) build -t $(PROJECT_IMAGE) -f Containerfile .

operator-image: | require-container-engine
	$(CONTAINER_ENGINE) build -f deploy/operator/Containerfile -t grid-operator:latest .

overlay-sync-image: | require-container-engine
	$(CONTAINER_ENGINE) build -f deploy/overlay-sync/Containerfile -t grid-overlay-sync:latest .

mock-providers-image: | require-container-engine
	$(CONTAINER_ENGINE) build -f mock-providers/Containerfile -t grid-mock-providers:latest .

# GLB demo images — deterministic :glb-demo tags, no :latest dependency.
glb-demo-images: | require-container-engine
	$(CONTAINER_ENGINE) build -f deploy/operator/Containerfile -t grid-operator:glb-demo .
	$(CONTAINER_ENGINE) build -f mock-providers/Containerfile -t grid-mock-providers:glb-demo .

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
	$(CONTAINER_ENGINE) build -t $(PROJECT_IMAGE) -f Containerfile .
	kind load docker-image $(PROJECT_IMAGE) --name $(KIND_CLUSTER_NAME)

dev-integration:
	@kind get kubeconfig --name $(KIND_CLUSTER_NAME) > /tmp/kind-$(KIND_CLUSTER_NAME).kubeconfig
	KUBECONFIG=/tmp/kind-$(KIND_CLUSTER_NAME).kubeconfig \
	cargo test --features integration -- --ignored $(if $(V),--nocapture,)

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
	@echo "  all              build + lint + test + audit"
	@echo ""
	@echo "Build:"
	@echo "  build            cargo build --workspace"
	@echo "  release          cargo build --workspace --release"
	@echo "  check            cargo check --workspace"
	@echo "  clean            cargo clean"
	@echo ""
	@echo "Test:"
	@echo "  test             run all tests"
	@echo ""
	@echo "Quality:"
	@echo "  lint             clippy + rustfmt check + machete"
	@echo "  fmt              format with nightly rustfmt"
	@echo "  doc              build docs with warnings denied"
	@echo "  audit            cargo audit + cargo deny"
	@echo "  coverage         HTML coverage report"
	@echo "  coverage-check   fail if line coverage < 80%%"
	@echo ""
	@echo "Container:"
	@echo "  container            build container image"
	@echo "  images               build container image"
	@echo "  operator-image       build operator container image"
	@echo "  overlay-sync-image   build overlay-sync container image"
	@echo "  mock-providers-image build mock-providers container image"
	@echo "  glb-demo-images      build all Grid images tagged :glb-demo"
	@echo ""
	@echo "KIND:"
	@echo "  kind-up          create cluster + deploy"
	@echo "  kind-down        delete cluster"
	@echo ""
	@echo "Dev Setup:"
	@echo "  setup-hooks      install git pre-commit hook"
	@echo ""
	@echo "Development:"
	@echo "  dev-env          create/reuse persistent cluster"
	@echo "  dev-push         build + load + rollout"
	@echo "  dev-integration  run integration tests"
