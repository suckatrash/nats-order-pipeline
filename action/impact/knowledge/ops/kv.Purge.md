# kv.Purge

Purge a key's history: rollup tombstone that erases prior revisions of the subject.

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Purge` — `func(ctx context.Context, key string, opts ...KVDeleteOpt) error`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (rollup publish + PubAck)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** [kv.go/Purge](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) publishes the delete marker to the stream leader on $KV.\<bucket>.\<key> carrying KV-Operation: PURGE and Nats-Rollup: sub, then blocks awaiting the PubAck as [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) receives it.
- **2a.** At R>1 [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes the purge marker to the stream's Raft group; followers replicate the entry and a quorum must commit before it is applied.
- **2b.** Concurrently, each replica appends the marker to its store via [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) and, honoring Nats-Rollup: sub, [stream.go/processJetStreamMsgWithRollup](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) runs a subject-scoped PurgeEx (Keep=1) that deletes every earlier revision of the key, with the fsync batched on SyncInterval (default 2m).
- **3.** Once the marker is committed across the quorum and the leader has applied the store append plus subject rollup, [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) returns the PubAck, leaving the purge marker as the key's only current revision.

## In practice

- **One write, many deletions** — A purge is a single published marker, but the Nats-Rollup: sub header makes the server delete every earlier revision of the key as part of the same store append. Server-side work therefore scales with how deep the key's history was, even though the client sees only one publish and one PubAck.
- **Replication gates the ack** — With more than one replica the leader must propose the purge marker to the stream's Raft group and wait for a quorum to commit before it can ack. Purge latency then tracks replica count and the slowest committing follower, on top of the local store and rollup work.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Purge](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [server/filestore.go — (\*fileStore).StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
