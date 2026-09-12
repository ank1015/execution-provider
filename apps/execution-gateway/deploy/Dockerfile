FROM rust:1-bookworm AS build

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY apps apps
COPY packages packages

RUN cargo build --release --locked -p execution-gateway

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/target/release/execution-gateway /usr/local/bin/execution-gateway

USER 65532:65532

ENTRYPOINT ["execution-gateway"]
CMD ["serve"]
