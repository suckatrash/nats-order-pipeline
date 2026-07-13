# kv.Keys

List keys by draining a headers-only watcher over the whole bucket.

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Keys` — `func(ctx context.Context, opts ...WatchOpt) ([]string, error)`
- Symbol: `jetstream.KeyValue.ListKeys` — `func(ctx context.Context, opts ...WatchOpt) (KeyLister, error)`
- Symbol: `jetstream.KeyValue.ListKeysFiltered` — `func(ctx context.Context, filters ...string) (KeyLister, error)`
- Pattern: multi-request; round trips: variable
- Wire messages: consumer create + one headers-only delivery per key + teardown
- Disk I/O: read; Raft: none; server state: ephemeral; scan: full
- Choke points: shared-api-pool

## Flow

- **1.** [kv.go/Keys](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) creates a temporary headers-only consumer over the whole bucket, through the $JS.API pool.
- **2.** There is no key index, so [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) replays every key's latest-revision headers from the store — a full-bucket read.
- **3.** [kv.go/ListKeys](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) drains the headers stream key-by-key as [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) delivers each header, then tears down the temporary consumer.

## In practice

- **Scales with key count** — There is no key index; listing walks a temporary watcher over the whole bucket and delivers one headers-only message per key, so cost grows linearly with the number of keys. On a large bucket every call is a full sweep.
- **Cache the key list** — Because each call re-scans the entire bucket, avoid listing keys per request or on a hot path. Cache the result and refresh it on a schedule, or track keys yourself as you write them, when you need the set often.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Keys](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go), [jetstream/kv.go — (\*kvStore).ListKeys](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
