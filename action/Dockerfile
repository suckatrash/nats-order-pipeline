FROM golang:1.25 AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /impact-analysis .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /impact-analysis /usr/local/bin/impact-analysis

ENTRYPOINT ["/usr/local/bin/impact-analysis"]
