# jetstream.AccountInfo

Fetch JetStream account limits and usage ($JS.API.INFO).

- **Tier: low** — score 3 (invocation 3 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.JetStream.AccountInfo` — `func(ctx context.Context) (*AccountInfo, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: bounded
- Choke points: shared-api-pool

## Flow

- **1.** The client publishes an empty request to $JS.API.INFO in [jetstream.go/AccountInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) and awaits the reply; the request is dispatched through the shared $JS.API worker pool, and in clustered mode only the JetStream meta leader answers it in [jetstream\_api.go/jsAccountInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader aggregates the account's limits and usage from in-memory counters in [jetstream.go/JetStreamUsage](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream.go) — a bounded scan over the account's stream/consumer reservations — and the handler in [jetstream\_api.go/jsAccountInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) returns them as a JSApiAccountInfoResponse; no Raft or disk I/O is involved.

## In practice

- **Scales with account asset count** — The usage figures are aggregated over every stream and consumer the account reserves, so a very large account makes each call do more work even though it stays a bounded in-memory scan. Poll it periodically to track quota headroom rather than calling it on a hot path.
- **A cheap meta-leader read** — There is no Raft or disk I/O — the meta leader answers from in-memory usage counters — so a single call is inexpensive. In a cluster every call still funnels to that one leader, so a fleet all polling account info concentrates load on it.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).AccountInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsAccountInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
