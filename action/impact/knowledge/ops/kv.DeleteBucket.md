# kv.DeleteBucket

Delete a KV bucket (stream delete of KV\_\<bucket>).

- **Tier: high** — score 7 (invocation 7 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValueManager.DeleteKeyValue` — `func(ctx context.Context, bucket string) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client resolves the bucket to stream KV\_\<bucket> in [kv.go/DeleteKeyValue](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) and sends an empty $JS.API.STREAM.DELETE.KV\_\<bucket> request through the shared $JS.API worker pool to the meta leader, which handles it in [jetstream\_api.go/jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader proposes a delete-stream assignment for KV\_\<bucket> to the metagroup Raft in [jetstream\_cluster.go/jsClusteredStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go); a quorum of meta peers must replicate and commit the removal.
- **3.** Once committed, every replica applies the removal in [jetstream\_cluster.go/processStreamRemoval](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), stopping the stream in [stream.go/stop](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) and deleting all on-disk KV\_\<bucket> assets in [filestore.go/Delete](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) — message blocks, per-subject index, and cascaded consumer state — scaling with stored key/history volume.
- **4.** After the removal is committed and applied, the meta leader returns the JSApiStreamDeleteResponse success ack to the client from [jetstream\_cluster.go/processClusterDeleteStream](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go).

## In practice

- **Same cost as deleting a stream** — A KV bucket is just a stream, so deleting it is identical to a stream delete: a meta-Raft delete-stream proposal, a quorum commit, and a per-replica teardown. There is no lighter KV-specific path.
- **Scales with stored volume** — The on-disk teardown removes every message block, the per-subject index, and cascaded consumer state on each replica, so a bucket holding many keys or deep history costs proportionally more to delete than a nearly empty one.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/kv.go — (\*jetStream).DeleteKeyValue](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
