# obj.CreateBucket

Create or update an object store bucket — a stream named OBJ\_\<bucket> for chunk and meta subjects.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStoreManager.CreateObjectStore` — `func(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)`
- Symbol: `jetstream.ObjectStoreManager.CreateOrUpdateObjectStore` — `func(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)`
- Symbol: `jetstream.ObjectStoreManager.UpdateObjectStore` — `func(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** [object.go/CreateObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) derives a StreamConfig named OBJ\_\<bucket> (chunk subject $O.\<bucket>.C.> and metadata subject $O.\<bucket>.M.>) and sends it on $JS.API.STREAM.CREATE, where a shared $JS.API worker on the meta leader validates the config in [jetstream\_api.go/jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader proposes the OBJ\_\<bucket> stream assignment to the metagroup Raft in [jetstream\_cluster.go/jsClusteredStreamRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go); a quorum of the metagroup must commit the assignment before it takes effect.
- **3.** Each assigned peer applies the committed assignment in [jetstream\_cluster.go/processStreamAssignment](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), creates the OBJ\_\<bucket> stream and its filestore via [stream.go/addStream](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), and persists the stream config and state to disk.
- **4.** Once the new stream's Raft group elects a leader in [jetstream\_cluster.go/processStreamLeaderChange](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), the server returns the StreamInfo (JSApiStreamCreateResponse) to the client.

## In practice

- **A stream create under the hood** — Creating a bucket provisions a full JetStream stream through the metagroup, so it carries the same control-plane cost as any stream create rather than a lightweight data write; the one backing stream then serves both the chunk and metadata subjects. Do it at setup time, not per object or per request.
- **Scales with replica count** — Each extra replica is another peer that must apply the assignment and persist the new stream's config and filestore, and every replica takes part in the quorum commit, so higher replication multiplies both the create cost and the ongoing footprint. Choose replication for durability needs, not by default.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/object.go — (\*jetStream).CreateObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
