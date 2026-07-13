# jetstream.PurgeStream

Remove messages from a stream, optionally filtered by subject / sequence / keep-count.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Stream.Purge` — `func(ctx context.Context, opts ...StreamPurgeOpt) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: stream-propose; server state: none; scan: none
- Choke points: shared-api-pool, asset-lock

## Flow

- **1.** The client sends a JSApiStreamPurgeRequest (optional subject / sequence / keep filter) to $JS.API.STREAM.PURGE.\<stream> in [stream.go/Purge](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go); [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) dispatches it off the shared $JS.API worker pool to the stream leader, which rejects it if the stream is sealed or DenyPurge is set.
- **2.** At R>1 [jetstream\_cluster.go/jsClusteredStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes a streamPurge entry (capturing LastSeq) to the stream's Raft group; the purge is applied only after a quorum commits it.
- **3.** On apply each replica runs the purge under [stream.go/purgeLocked](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) — the store does fast whole-block truncation (or block compaction for a sequence purge) for an unfiltered purge, while [filestore.go/PurgeEx](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) walks the per-subject index for a filtered / keep purge — holding the stream WRITE lock for the ENTIRE operation, including the per-consumer purge loop, blocking every publish, ack, and consumer op on the stream.
- **4.** The leader returns a JSApiStreamPurgeResponse carrying the purged message count — at R>1 [jetstream\_cluster.go/applyStreamEntries](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) sends it from the Raft apply loop once the purge is committed and durable across the group, and at R=1 [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) replies synchronously after the local purge.

## In practice

- **Filtered purges cost the most** — An unfiltered purge is a fast whole-block truncation, but a subject, sequence, or keep-filtered purge walks the per-subject index and does work proportional to the messages it matches. Prefer whole-stream purges where the semantics allow, and expect filtered purges on wide streams to run long.
- **Schedule purges off the hot path** — A purge holds the stream write lock for its entire duration, so a large or filtered purge issued during peak traffic becomes a latency spike for every publisher and consumer on that stream. Trigger purges from maintenance windows or size-based retention rather than synchronously in a request path.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/stream.go — (\*stream).Purge](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/filestore.go — (\*fileStore).PurgeEx](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
