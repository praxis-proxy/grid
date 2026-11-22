# syntax=docker/dockerfile:1

# Test-only glibc image for the opt-in stream-memory qualification. The
# production Containerfile remains the musl build used for the comparison.
FROM rust:1.96-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG GRID_GIT_COMMIT=unknown
ARG GRID_GIT_VERSION=unknown
ARG GRID_GIT_TREE_STATE=unknown
ARG GRID_BUILD_DATE=unknown
ARG GRID_QUALIFICATION_SOURCE_SHA256=unknown
ENV GRID_GIT_COMMIT=${GRID_GIT_COMMIT} \
    GRID_GIT_VERSION=${GRID_GIT_VERSION} \
    GRID_GIT_TREE_STATE=${GRID_GIT_TREE_STATE} \
    GRID_BUILD_DATE=${GRID_BUILD_DATE}

WORKDIR /src
COPY . .

RUN cargo build --locked --release --manifest-path crates/gateway/Cargo.toml --bin grid-gateway

FROM debian:bookworm-slim AS runtime

ARG GRID_GIT_COMMIT=unknown
ARG GRID_GIT_VERSION=unknown
ARG GRID_QUALIFICATION_SOURCE_SHA256=unknown
LABEL org.opencontainers.image.revision=${GRID_GIT_COMMIT} \
    org.opencontainers.image.version=${GRID_GIT_VERSION} \
    org.opencontainers.image.source="https://github.com/praxis-proxy/grid" \
    org.opencontainers.image.description="Test-only glibc grid-gateway for stream-memory qualification" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.grid.qualification.memory.variant="gnu-jemalloc" \
    org.grid.qualification.memory.allocator="jemalloc" \
    org.grid.qualification.source-sha256=${GRID_QUALIFICATION_SOURCE_SHA256}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 101 grid \
    && useradd --system --uid 100 --gid 101 --home-dir /nonexistent --shell /usr/sbin/nologin grid

COPY --from=builder --chown=root:root --chmod=0555 \
    /src/crates/gateway/target/release/grid-gateway /usr/local/bin/grid-gateway

USER 100:101
ENTRYPOINT ["grid-gateway"]
