# jetstream.PublishAsync

Pipelined persisted publish: send without waiting; PubAcks return on a shared inbox.

- **Tier: high** — score 7 (invocation 7 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Publisher.PublishAsync` — `func(subject string, payload []byte, opts ...PublishOpt) (PubAckFuture, error)`
- Symbol: `jetstream.Publisher.PublishMsgAsync` — `func(msg *nats.Msg, opts ...PublishOpt) (PubAckFuture, error)`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 2 per publish (message + async PubAck)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** The client publishes the message to the stream leader in [publish.go/PublishMsgAsync](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/publish.go) and returns a PubAckFuture instead of blocking; the send registers on the context's single shared reply-inbox subscription and is bounded by PublishAsyncMaxPending (default 4000, then a 200ms stall before ErrTooManyStalledMsgs), while [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) matches interest on the leader and enters the stream's message path.
- **2a.** At R>1 [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes the message to the stream's Raft group; followers replicate the entry and a quorum must commit.
- **2b.** Concurrently, each replica appends the message to its store and updates the per-subject index in [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) (fsync batched on SyncInterval, default 2m).
- **3.** [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) publishes the PubAck to the shared inbox once the write is durable across the quorum, and [publish.go/PublishMsgAsync](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/publish.go) resolves the matching future; per-message server cost is identical to synchronous publish, only the client stopped waiting.

## In practice

- **Throughput, not cheaper writes** — Pipelining keeps many publishes in flight so throughput climbs, but each message still pays the full synchronous Publish cost: proposal, replication, store append, and index update. Reach for async to saturate a stream, not to make an individual write cheaper.
- **Size the max-pending window** — In-flight publishes are capped by PublishAsyncMaxPending (default 4000); once the window fills, sends stall for 200ms and then fail with ErrTooManyStalledMsgs. Tune the window against the stream's real ingest rate, since too small throttles throughput and too large hides backpressure until failures batch up.
- **Failures surface asynchronously** — Because the client never waits, errors including NoResponders retries arrive later on the future or error handler rather than at the call site. Any path that must confirm durability has to drain and check those futures before treating a batch as persisted.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/publish.go — (\*jetStream).PublishMsgAsync](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/publish.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [server/filestore.go — (\*fileStore).StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
