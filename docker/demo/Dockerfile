FROM rust:1.95-bookworm AS builder

WORKDIR /build
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/discovery /usr/local/bin/discovery
COPY --from=builder /build/target/release/peer_runner /usr/local/bin/peer_runner
