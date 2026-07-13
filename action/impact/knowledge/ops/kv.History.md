# kv.History

Read all stored revisions of a key via a temporary ordered consumer.

- **Tier: high** — score 6 (invocation 6 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.History` — `func(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error)`
- Pattern: multi-request; round trips: variable
- Wire messages: consumer create + revision deliveries + teardown
- Disk I/O: read; Raft: none; server state: ephemeral; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** [kv.go/History](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) opens an ephemeral ordered push consumer via $JS.API.CONSUMER.CREATE on the KV\_\<bucket> stream, filtered to the key's $KV.\<bucket>.\<key> subject with DeliverAll so every stored revision replays, and the create request rides the shared $JS.API pool to [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The consumer's delivery loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) walks the filtered subject, loading each revision from the stream's filestore via [filestore.go/LoadNextMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) — a bounded read capped by the bucket's history depth (≤64).
- **3.** [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) pushes each revision down the ordered consumer's deliver subject to the client, which in [kv.go/History](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) appends entries until pending drains to zero, then stops the watcher and tears down the ephemeral consumer.

## In practice

- **Scales with history depth** — The temporary consumer replays every stored revision of the key, so cost grows with how deep the key's history runs. A bucket caps history at 64 revisions, which bounds the sweep and keeps the consumer short-lived, but a key kept at full depth still costs far more to read than fetching its current value.
- **Amortize the consumer setup** — Each call spins up an ephemeral ordered consumer and tears it down once the replay drains, so even a single-revision key pays that create-and-teardown overhead. Avoid calling History on a hot path; read it once and cache the revisions when you need them repeatedly.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `jetstream.GetMsg`, `kv.Get`, `obj.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).History](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
