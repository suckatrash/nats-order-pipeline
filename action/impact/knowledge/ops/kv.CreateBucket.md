# kv.CreateBucket

Create or update a KV bucket — a stream named KV\_\<bucket> with per-subject history limits.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValueManager.CreateKeyValue` — `func(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)`
- Symbol: `jetstream.KeyValueManager.CreateOrUpdateKeyValue` — `func(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)`
- Symbol: `jetstream.KeyValueManager.UpdateKeyValue` — `func(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client renders the KV config in [kv.go/CreateKeyValue](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) into a stream named KV\_\<bucket> (MaxMsgsPerSubject=history, AllowDirect, denied deletes, rollup) and sends $JS.API.STREAM.CREATE through the shared $JS.API request pool to the meta leader, which processes it in [jetstream\_api.go/jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader selects replica peers and builds the stream's Raft group in [jetstream\_cluster.go/jsClusteredStreamRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), then proposes an add-stream assignment to the JetStream meta Raft group; a quorum of meta peers must commit the assignment before any node instantiates the bucket.
- **3.** On applying the committed assignment each assigned replica instantiates the stream and creates its file store in [stream.go/setupStore](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), fsync-ing the stream config to meta.inf in [filestore.go/writeStreamMeta](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go); no message data is written since the bucket starts empty.
- **4.** Once the assignment commits and the stream leader is elected, [jetstream\_cluster.go/processStreamAssignmentResults](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) returns the StreamInfo response, which the client maps to a KV bucket handle.

## In practice

- **A bucket is just a stream** — KV has no dedicated server machinery; creating a bucket provisions a JetStream stream with per-subject history limits, direct-get, denied deletes, and rollup enabled, so it pays the full stream-create cost — a meta-Raft proposal, a quorum commit, and per-replica file-store setup.
- **Scales with replicas and history** — Cost grows with replica count, since each added replica is another peer that instantiates the stream and must commit through the meta Raft group, and with history depth (max-msgs-per-subject), which sets how many revisions each key retains and thus the bucket's eventual on-disk footprint.
- **Create-or-update costs an extra round** — CreateKeyValue falls back to an update-and-info round trip when the stream already exists, so an idempotent create against a live bucket costs more than a first-time create; UpdateKeyValue skips initial provisioning when you only need to change config.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/kv.go — (\*jetStream).CreateKeyValue](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
