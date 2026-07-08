# Multi-stage build for mock AI provider servers.
# Produces a single binary that serves any provider via --provider flag.
#
# Usage:
#   docker build -t mock-providers -f mock-providers/Dockerfile .
#   docker run --rm -p 10001:8080 mock-providers --provider openai

FROM rust:1.96-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY Cargo.toml Cargo.lock clippy.toml rustfmt.toml deny.toml ./
COPY .cargo .cargo
COPY mock-providers mock-providers
COPY xtask xtask
RUN cargo build --release --package mock-providers

FROM alpine:3.22
RUN adduser -D -u 1000 app
COPY --from=builder /build/target/release/mock-providers /usr/local/bin/
USER app
EXPOSE 8080
ENTRYPOINT ["mock-providers"]
