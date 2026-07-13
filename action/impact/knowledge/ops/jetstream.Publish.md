# jetstream.Publish

Synchronous persisted publish: store append, replication, then PubAck.

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Publisher.Publish` — `func(ctx context.Context, subject string, payload []byte, opts ...PublishOpt) (*PubAck, error)`
- Symbol: `jetstream.Publisher.PublishMsg` — `func(ctx context.Context, msg *nats.Msg, opts ...PublishOpt) (*PubAck, error)`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (message + PubAck)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** The client sends the message to the stream leader in [publish.go/PublishMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/publish.go) and awaits a PubAck; [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) receives it on the leader and enters the stream's message path.
- **2a.** At R>1 [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes the message to the stream's Raft group; followers replicate the entry and a quorum must commit.
- **2b.** Concurrently, each replica appends the message to its store and updates the per-subject index in [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) (fsync batched on SyncInterval, default 2m).
- **3.** [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) returns the PubAck once the write is durable across the quorum.

## In practice

- **Scales with replica count** — At more than one replica each publish waits on a Raft proposal and quorum commit before the PubAck, so latency tracks the slowest replica in the quorum. Durability comes from quorum replication plus a batched fsync on SyncInterval (default two minutes), not a per-message fsync, so replica count sets the floor, not disk speed.
- **Message size, index, and header checks** — Cost grows with message size and with per-subject index maintenance on every append, so streams with many distinct subjects pay more per write. A large dedup window adds a Nats-Msg-Id lookup, and Expected-Last headers add sequence and subject checks.
- **One writer per stream** — A stream serializes all appends, interest and workqueue acks, purges, and consumer registrations on a single writer lock, so a high publish rate contends with everything else happening on that stream. Shard hot subjects across separate streams to buy write parallelism.
- **Interest streams can skip the store** — On an interest or workqueue stream, a publish with no interested consumer skips the store entirely while the PubAck is still sent. Retention policy therefore changes the real disk cost of a publish independently of message rate.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/publish.go — (\*jetStream).PublishMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/publish.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [server/jetstream\_cluster.go — (\*stream).processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), [server/filestore.go — (\*fileStore).StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
