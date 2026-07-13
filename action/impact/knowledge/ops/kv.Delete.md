# kv.Delete

Mark a key deleted by publishing a DEL tombstone (history retained).

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Delete` — `func(ctx context.Context, key string, opts ...KVDeleteOpt) error`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (tombstone publish + PubAck)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** The client's [kv.go/Delete](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) publishes an empty message stamped with the KV-Operation: DEL header to $KV.\<bucket>.\<key> via [publish.go/PublishMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/publish.go) and awaits a PubAck, while the stream leader ingests the tombstone in [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) as an ordinary write, not a removal.
- **2a.** At R>1 the leader proposes the tombstone to the stream's Raft group in [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go); followers replicate the entry and a quorum must commit.
- **2b.** Concurrently, each replica appends the tombstone to its store and updates the per-subject index in [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go); prior revisions stay in the log within the bucket's history depth (fsync batched on SyncInterval, default 2m).
- **3.** [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) returns the PubAck once the tombstone write is durable across the quorum.

## In practice

- **Delete is a write, not a removal** — A KV delete publishes a DEL-tombstone: an empty message carrying a KV-Operation header that is persisted like any other record. It never reclaims space — prior revisions stay in the log within the bucket's history depth, so a delete grows the stream rather than shrinking it.
- **Scales with the replica quorum** — At more than one replica the tombstone is a Raft proposal that must replicate to followers and commit across a quorum before the PubAck returns, so delete latency and cost rise with replication factor exactly as an ordinary KV put does.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Delete](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go)
