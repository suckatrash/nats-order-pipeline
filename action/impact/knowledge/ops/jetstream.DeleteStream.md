# jetstream.DeleteStream

Delete a stream and all its messages, consumers, and raft state.

- **Tier: high** — score 7 (invocation 7 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamManager.DeleteStream` — `func(ctx context.Context, stream string) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client sends an empty request in [jetstream.go/DeleteStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) on $JS.API.STREAM.DELETE.\<name>, dispatched through the shared $JS.API service pool to [jetstream\_api.go/jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) on the meta leader, and awaits a single ack.
- **2.** The meta leader proposes a delete-stream assignment in [jetstream\_cluster.go/jsClusteredStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) onto the metagroup Raft log; a quorum of meta peers must commit the removal before it takes effect.
- **3.** Applying the committed removal in [jetstream\_cluster.go/processStreamRemoval](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), each replica shuts down the stream's Raft node in [stream.go/stop](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), cascades deletes to every one of the stream's consumers, and removes the message blocks and per-subject index from its filestore via [filestore.go/Delete](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go).
- **4.** Once the stream and its consumers are fully torn down, the meta leader returns a success ack to the caller in [jetstream\_cluster.go/processClusterDeleteStream](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go).

## In practice

- **Scales with stored data volume** — Applying the delete removes every message block and the per-subject index from each replica's file store, so the cost tracks how much data the stream holds. Tearing down a multi-gigabyte stream is real disk I/O on every replica, not a metadata flick.
- **Cascades to every consumer** — Deleting a stream also tears down all of its consumers, so the cost scales with consumer count as well as data volume. Each cascaded consumer is its own teardown on every replica, so a stream fanned out to many consumers is proportionally more expensive to drop.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).DeleteStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
