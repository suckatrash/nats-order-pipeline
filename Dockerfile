FROM golang:1.25 AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o /app/order-api ./cmd/order-api && \
    CGO_ENABLED=0 go build -o /app/processor ./cmd/processor && \
    CGO_ENABLED=0 go build -o /app/notifier ./cmd/notifier && \
    CGO_ENABLED=0 go build -o /app/analytics ./cmd/analytics && \
    CGO_ENABLED=0 go build -o /app/enricher ./cmd/enricher

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/ /app/

ENTRYPOINT ["/app/order-api"]
