# kv.Put

Write a key: a JetStream publish to $KV.\<bucket>.\<key>, optionally CAS-guarded.

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Put` — `func(ctx context.Context, key string, value []byte) (uint64, error)`
- Symbol: `jetstream.KeyValue.PutString` — `func(ctx context.Context, key string, value string) (uint64, error)`
- Symbol: `jetstream.KeyValue.Create` — `func(ctx context.Context, key string, value []byte, opts ...KVCreateOpt) (uint64, error)`
- Symbol: `jetstream.KeyValue.Update` — `func(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (publish + PubAck)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** KV.Put serializes the value in [kv.go/Put](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) as a JetStream publish to $KV.\<bucket>.\<key> and awaits a PubAck; Update/Create go through [kv.go/updateRevision](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) to attach the Nats-Expected-Last-Subject-Sequence header so the server's [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) runs a compare-and-set on the key's per-subject sequence with no extra round trip.
- **2a.** At R>1 the stream leader validates any expected-last-subject-sequence header in [jetstream\_batching.go/checkMsgHeadersPreClusteredProposal](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_batching.go), then [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes the message to the stream's Raft group; a quorum of replicas must commit the entry before it is applied.
- **2b.** Concurrently each replica appends the value to its message store in [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go), updates the per-subject index, and prunes history beyond the bucket's MaxMsgsPer depth (fsync batched on SyncInterval, default 2m).
- **3.** [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) returns the PubAck carrying the key's new revision (stream sequence) once the write is durable across the quorum; a failed CAS instead yields a wrong-last-sequence error and no write.

## In practice

- **Scales with value size and replicas** — Every write is a full JetStream persisted publish, so per-write cost climbs with the value size and with the replication factor — more replicas mean more bytes fsynced and a wider quorum to satisfy. Treat both as the primary knobs when a bucket is write-heavy.
- **Conditional writes and deleted keys** — Create and Update carry an expected-last-subject-sequence header, so the compare-and-set runs on the server as a per-subject sequence check with no extra round trip. The exception is Create on an existing-but-deleted key, which can cost up to three round trips: publish, a get to find the tombstone, then a retry at the deleted revision.
- **History pruning per subject** — Beyond the compare-and-set, each write also updates the per-subject index and prunes history past the bucket's configured depth, both proportional to how many revisions a key already holds. Buckets with deep history or many keys pay this on every Put, so keep history depth no larger than you actually read back.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Put](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go), [jetstream/kv.go — (\*kvStore).Update](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [server/filestore.go — (\*fileStore).StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
