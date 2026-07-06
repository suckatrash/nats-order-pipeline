# nats-order-pipeline

Test application for validating the [Insights](https://github.com/ConnectEverything/insights) impact-analysis skill. Deploys a multi-service order processing pipeline on NATS JetStream, then uses a custom GitHub Action to run Claude-powered impact analysis on PRs that change infrastructure config.

## Architecture

```
HTTP POST /order → order-api ──▶ ORDERS.created
                                      │
                   processor  ◀───────┘──▶ ORDERS.processed / ORDERS.rejected
                                      │
                   notifier   ◀───────┘──▶ (log output)
                                      │
                   analytics  ◀── ORDERS.* ──▶ ANALYTICS.summary
```

### Streams

| Stream | Subjects | Replicas | Retention | Max Bytes |
|---|---|---|---|---|
| `ORDERS` | `ORDERS.>` | 3 | limits | 5 GiB |
| `ANALYTICS` | `ANALYTICS.>` | 1 | workqueue | 1 GiB |

### Consumers

| Consumer | Stream | Filter | Max Ack Pending | Ack Wait |
|---|---|---|---|---|
| `order-processor` | ORDERS | `ORDERS.created` | 1000 | 30s |
| `order-notifier` | ORDERS | `ORDERS.processed` | 100 | 10s |
| `order-analytics` | ORDERS | `ORDERS.>` | 5000 | default |

### Services

| Service | Role | Traffic |
|---|---|---|
| `order-api` | HTTP API + publisher + traffic generator | 10-50 orders/sec (configurable via `ORDER_RATE`) |
| `processor` | Consumes created orders, publishes processed/rejected | ~50ms simulated latency, ~5% rejection |
| `notifier` | Consumes processed orders, logs notifications | Occasional back-pressure simulation |
| `analytics` | Consumes all order events, publishes periodic summaries | 30s batch window |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NATS_URL` | `nats://nats:4222` | NATS server URL |
| `NATS_CREDS` | (none) | Path to NATS credentials file |
| `ORDER_RATE` | `20` | Orders per second (order-api only) |

## Local Development

```bash
# Build all services
go build ./cmd/...

# Run individual services (requires a NATS server)
NATS_URL=nats://localhost:4222 go run ./cmd/order-api
NATS_URL=nats://localhost:4222 go run ./cmd/processor
NATS_URL=nats://localhost:4222 go run ./cmd/notifier
NATS_URL=nats://localhost:4222 go run ./cmd/analytics
```

## Docker

```bash
# Build the multi-service image
docker build -t order-pipeline .

# Run services (override entrypoint per service)
docker run --rm order-pipeline /app/order-api
docker run --rm order-pipeline /app/processor
docker run --rm order-pipeline /app/notifier
docker run --rm order-pipeline /app/analytics
```

## Impact Analysis Action

The `action/` directory contains a custom GitHub Action that runs Claude-powered impact analysis on PRs. It:

1. Reads the PR diff
2. Loads the impact-analysis skill from `.claude/skills/impact-analysis/SKILL.md`
3. Runs a Claude API tool-use loop, executing queries against a live Insights instance over NATS
4. Posts the analysis as a PR comment

### Required Secrets

| Secret | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API access |
| `INSIGHTS_NATS_SERVER` | NATS server URL (e.g. `nats://nats1.gcp.iamusingtheinternet.com:4222`) |
| `INSIGHTS_NATS_CREDS` | NATS credentials file content (read-only query access) |

### Test Scenarios

| PR Change | Expected Findings |
|---|---|
| Reduce ORDERS replicas 3→1 | HA loss, 3 consumers affected |
| Delete ORDERS stream | 3 consumers destroyed, pipeline broken |
| Lower processor max-ack-pending 1000→50 | Current pending exceeds 50, back-pressure |
| Change processor subject filter | Subject mismatch, messages stop flowing |
| Change ORDERS retention to 1h | Analytics lag may exceed 1h, data loss risk |
| Remove notifier service | Unprocessed notifications |

## Deployment

Deployed to k8s via ArgoCD using the manifests in [nats-argocd-stack](https://github.com/suckatrash/nats-argocd-stack). Services create their own streams and consumers on startup.
