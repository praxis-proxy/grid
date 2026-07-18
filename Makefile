# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

V ?=
NIGHTLY_RUSTFMT   ?= nightly-2026-03-28
KIND_CLUSTER_NAME ?= praxis-dev
PROJECT_IMAGE    ?= project:dev
KUBECTL          ?= kubectl --context kind-$(KIND_CLUSTER_NAME)

ifneq ($(V),)
  _NOCAPTURE := -- --nocapture
endif

.PHONY: all build release check clean \
	test test-unit lint fmt doc audit \
	coverage coverage-check \
	images container operator-image kind-up kind-down \
	dev-env dev-push dev-integration \
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

container:
	podman build -t $(PROJECT_IMAGE) -f Containerfile . || \
	docker build -t $(PROJECT_IMAGE) -f Containerfile .

operator-image:
	cargo build -p operator --bin operator --release
	mkdir -p target/operator-ctx
	cp target/release/operator target/operator-ctx/operator
	podman build -f deploy/operator/Containerfile -t grid-operator:latest target/operator-ctx/ || \
	docker build -f deploy/operator/Containerfile -t grid-operator:latest target/operator-ctx/
	rm -rf target/operator-ctx

images:
	docker build -t $(PROJECT_IMAGE) -f Containerfile .

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

dev-push:
	docker build -t $(PROJECT_IMAGE) -f Containerfile .
	kind load docker-image $(PROJECT_IMAGE) --name $(KIND_CLUSTER_NAME)

dev-integration:
	@kind get kubeconfig --name $(KIND_CLUSTER_NAME) > /tmp/kind-$(KIND_CLUSTER_NAME).kubeconfig
	KUBECONFIG=/tmp/kind-$(KIND_CLUSTER_NAME).kubeconfig \
	cargo test --features integration -- --ignored $(if $(V),--nocapture,)

# -------------------------------------------------------------------
# Help
# -------------------------------------------------------------------

help:
	@echo "Variables:"
	@echo "  V=1                show test output (--nocapture)"
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
	@echo "  test             run all tests (alias for test-unit)"
	@echo "  test-unit        cargo test --workspace"
	@echo ""
	@echo "Quality:"
	@echo "  lint             clippy + rustfmt check"
	@echo "  fmt              format with nightly rustfmt"
	@echo "  doc              build docs with warnings denied"
	@echo "  audit            cargo audit + cargo deny"
	@echo "  coverage         HTML coverage report"
	@echo "  coverage-check   fail if line coverage < 80%%"
	@echo ""
	@echo "Operator:"
	@echo "  operator-image   build operator container image"
	@echo ""
	@echo "KIND:"
	@echo "  kind-up          create cluster + deploy"
	@echo "  kind-down        delete cluster"
	@echo ""
	@echo "Development:"
	@echo "  dev-env          create/reuse persistent cluster"
	@echo "  dev-push         build + load + rollout"
	@echo "  dev-integration  run integration tests"
