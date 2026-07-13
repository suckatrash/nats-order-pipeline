# jetstream.DeleteMsg

Erase one message from a stream; the secure variant overwrites its bytes.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Stream.DeleteMsg` — `func(ctx context.Context, seq uint64) error`
- Symbol: `jetstream.Stream.SecureDeleteMsg` — `func(ctx context.Context, seq uint64) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: stream-propose; server state: none; scan: none
- Choke points: shared-api-pool, asset-lock

## Flow

- **1.** The client sends a delete request in [stream.go/DeleteMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) on $JS.API.STREAM.MSG.DELETE.\<stream> carrying the target sequence (NoErase for DeleteMsg, erase for SecureDeleteMsg); a shared $JS.API pool worker routes it to [jetstream\_api.go/jsMsgDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) on the stream leader.
- **2.** At R>1 the leader proposes a deleteMsgOp entry to the stream's Raft group in [jetstream\_cluster.go/jsClusteredMsgDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go); followers replicate it and a quorum must commit before the delete is applied.
- **3.** Once committed, each replica applies the entry in [jetstream\_cluster.go/applyStreamEntries](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go): [filestore.go/RemoveMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) holds the file store's write lock across the target block load and writes a tombstone for interior deletes in [filestore.go/removeMsgFromBlock](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go), while SecureDeleteMsg rewrites the block's record with random bytes under that same lock.
- **4.** After the delete is durably applied in [jetstream\_cluster.go/applyStreamEntries](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), the leader returns the success ack; interior deletes leave a tombstone the file store reconciles on later compaction.

## In practice

- **Interior deletes are bookkeeping-heavy** — Deleting a message in the middle of a block is far more expensive than head or tail expiry: it writes a tombstone and grows tombstone accounting the file store must reconcile on later compaction. Prefer age or size limits to age messages out; reserve explicit DeleteMsg for the odd record you must remove out of order.
- **SecureDeleteMsg rewrites whole blocks** — The secure variant overwrites the record's bytes with random data across the whole block, all under the store write lock, so it costs in proportion to block size and holds the lock long enough to stall concurrent publishers. Reserve it for compliance erasure, not routine deletes.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/stream.go — (\*stream).DeleteMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsMsgDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/filestore.go — (\*fileStore).RemoveMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
