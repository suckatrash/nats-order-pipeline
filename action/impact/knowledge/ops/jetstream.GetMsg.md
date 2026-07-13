# jetstream.GetMsg

Read a single stored message by sequence or last-per-subject.

- **Tier: moderate** — score 4 (invocation 4 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Stream.GetMsg` — `func(ctx context.Context, seq uint64, opts ...GetMsgOpt) (*RawStreamMsg, error)`
- Symbol: `jetstream.Stream.GetLastMsgForSubject` — `func(ctx context.Context, subject string) (*RawStreamMsg, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: read; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client requests a stored message by sequence in [stream.go/GetMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go), or by last-per-subject in [stream.go/GetLastMsgForSubject](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go).
- **2.** With AllowDirect the request is served as a DIRECT.GET by any up-to-date replica in [stream.go/processDirectGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), bypassing the $JS.API pool; without it the leader answers STREAM.MSG.GET through the pool in [jetstream\_api.go/jsMsgGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **3.** The store loads the message in [stream.go/processDirectGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) to satisfy the read; a cold block faults its whole 1–8 MB extent into cache (~10s TTL).
- **4.** [jetstream\_api.go/jsMsgGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) returns the message to the client.

## In practice

- **Enable AllowDirect to spread reads** — With AllowDirect a get is served by any up-to-date replica and bypasses the shared $JS.API worker pool, spreading read load instead of funneling it through the leader. Without it every get queues through the pool on the leader. Last-per-subject lookups are index-assisted via the per-subject state, so they stay cheap either way.
- **Cold reads fault a whole block** — A get that misses the block cache faults the message's entire storage block (up to several MB) into memory, and the load holds the stream and store read locks, so cold or batched direct gets briefly stall the publish write lock. The cache expires after about ten seconds idle, so sporadic gets pay the fault repeatedly and larger messages cost more per read.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `kv.Get`, `kv.History`, `obj.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.

## Evidence

- Client: [jetstream/stream.go — (\*stream).GetMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go), [jetstream/stream.go — (\*stream).GetLastMsgForSubject](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsMsgGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/stream.go — (\*stream).processDirectGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go)
