# syntax=docker/dockerfile:1.6
#
# Multi-stage Rust build with proper dependency caching via cargo-chef.
# cargo-chef extracts a reproducible "recipe" of the workspace's deps, so
# when only application source changes the dep-compile layer can reuse its
# cache. The previous dummy-main trick only worked when Cargo.toml was
# byte-identical across builds.

FROM rust:1.91-slim-bookworm AS chef
WORKDIR /app
# mold is dramatically faster than ld for crates with many symbols
# (aws-sdk-s3 + polars together generate hundreds of thousands).
RUN apt-get update && apt-get install -y --no-install-recommends mold clang && \
    rm -rf /var/lib/apt/lists/* && \
    cargo install cargo-chef --locked --version 0.1.68
# Tell cargo to link with mold via clang. ~30-60s off the final link.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-fuse-ld=mold"
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=clang
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-fuse-ld=mold"

# ---------- Stage 1: capture the dep recipe ----------
FROM chef AS planner
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# ---------- Stage 2: build deps against the recipe ----------
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Cook the recipe. This layer is cached until Cargo.lock or a dep feature
# changes. Everything heavy (polars, aws-sdk-s3) gets built here exactly
# once per dep-graph change.
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy real source and rebuild only our crate. Deps are cached from
# the cook step above.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --bin karet-worker

# ---------- Stage 3: runtime image ----------
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/karet-worker ./karet-worker
EXPOSE 8080
ENTRYPOINT ["./karet-worker"]
