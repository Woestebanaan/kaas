# syntax=docker/dockerfile:1.7
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin skafka-operator && \
    cp target/release/skafka-operator /skafka-operator

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 skafka
COPY --from=build /skafka-operator /usr/local/bin/skafka-operator
USER 10001
ENTRYPOINT ["/usr/local/bin/skafka-operator"]
