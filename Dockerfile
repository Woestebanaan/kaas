# syntax=docker/dockerfile:1
# Broker image. Multi-stage, distroless, target size <20MB.

FROM golang:1.22-alpine AS builder
WORKDIR /src

# Cache go module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS="-trimpath" \
    go build -ldflags="-s -w" -o /skafka ./cmd/skafka

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /skafka /skafka

USER nonroot:nonroot
EXPOSE 9092 9093 8080
ENTRYPOINT ["/skafka"]
