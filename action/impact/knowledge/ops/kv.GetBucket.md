# kv.GetBucket

Bind a KV bucket handle by validating the backing stream (STREAM.INFO).

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValueManager.KeyValue` — `func(ctx context.Context, bucket string) (KeyValue, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client's [kv.go/KeyValue](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) maps the bucket to its backing stream KV\_\<bucket> and sends a STREAM.INFO request on $JS.API.STREAM.INFO.KV\_\<bucket>; the server dispatches it through the shared $JS.API worker pool to the stream leader, which reads in-memory stream state in [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) (no disk or raft).
- **2.** The leader replies from [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) with the stream's config and state; the client's [kv.go/mapStreamToKVS](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) verifies MaxMsgsPerSubject>=1 for a valid KV layout and caches the info into a bound KeyValue handle that subsequent data operations reuse.

## In practice

- **Cache the bucket handle** — Binding a bucket costs one STREAM.INFO round trip, and the returned handle is reusable — every subsequent Get, Put, or Delete rides it with no further INFO. Bind once at startup and share the handle; re-binding per operation adds a round trip to every hot path.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/kv.go — (\*jetStream).KeyValue](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
