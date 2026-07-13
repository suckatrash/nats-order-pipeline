# obj.DeleteBucket

Delete an object store bucket (stream delete).

- **Tier: high** — score 7 (invocation 7 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStoreManager.DeleteObjectStore` — `func(ctx context.Context, bucket string) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** [object.go/DeleteObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) maps the bucket to its backing stream OBJ\_\<bucket> and sends an empty STREAM.DELETE request on $JS.API.STREAM.DELETE.OBJ\_\<bucket>, serviced by the meta leader's shared $JS.API handler pool in [jetstream\_api.go/jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader proposes a delete-stream assignment via [jetstream\_cluster.go/jsClusteredStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) to the JetStream meta Raft group; a quorum must commit the removal before [jetstream\_cluster.go/processStreamRemoval](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) applies the assignment.
- **3.** As the committed assignment is applied, each replica stops the stream and tears down its Raft node in [stream.go/stop](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), then removes the entire message-block store directory from disk via [filestore.go/Delete](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go), scaling with the bucket's stored object volume.
- **4.** Once [jetstream\_cluster.go/processClusterDeleteStream](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) finishes tearing down the stream cluster-wide, a JSApiStreamDeleteResponse ack is returned to the client from [jetstream\_api.go/jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).

## In practice

- **Really a stream delete** — Deleting an object store bucket is exactly a JetStream stream delete on the backing stream: a metagroup control-plane operation, not a per-object cleanup. There is nothing object-store-specific to tune here — it inherits the stream-delete cost and semantics.
- **Not a per-job resource** — Because each delete is a metagroup round trip that tears down the stream and its data on every replica, creating and deleting buckets per job or per request is an expensive anti-pattern. Provision buckets to live and delete them only when decommissioning.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/object.go — (\*jetStream).DeleteObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
