# obj.Seal

Seal the bucket read-only via a stream config update.

- **Tier: high** — score 10 (invocation 10 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.Seal` — `func(ctx context.Context) error`
- Pattern: multi-request; round trips: 2
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** Seal in [object.go/Seal](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) first calls the stream's Info in [stream.go/Info](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go), sending $JS.API.STREAM.INFO to [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) to read the bucket's current stream config as the base it will edit.
- **2.** It flips Sealed=true on that config and resubmits the whole config through [jetstream.go/UpdateStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) as $JS.API.STREAM.UPDATE, which the shared $JS.API worker pool dispatches to [jetstream\_api.go/jsStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) on the meta leader.
- **3.** The meta leader validates the sealed edit in [jetstream\_cluster.go/jsClusteredStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) and proposes an updateStreamAssignment to the JetStream meta Raft group, where a quorum of meta peers must replicate and commit it.
- **4.** Each peer applies the committed assignment in [stream.go/updateWithAdvisory](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) and persists the sealed config to its stream meta file via [filestore.go/writeStreamMeta](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go); sealing also forces MaxAge=0, DiscardNew, and DenyDelete/DenyPurge, and is irreversible.
- **5.** The meta leader replies from [jetstream\_api.go/jsStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) with the updated StreamInfo reflecting sealed=true.

## In practice

- **Sealing is irreversible** — A sealed bucket can never be unsealed, reopened for writes, or have entries deleted or purged. Make sealing a deliberate, one-time decision at the end of a bucket's life, not a step you can walk back if requirements change.
- **Cost is independent of bucket size** — Sealing rewrites only the stream's configuration, never the stored objects, so it costs the same whether the bucket holds one object or millions. The expense is a single meta-layer config commit, cheap in bytes but a full cluster-coordinated change.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/object.go — (\*obs).Seal](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
