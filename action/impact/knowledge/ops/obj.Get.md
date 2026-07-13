# obj.Get

Retrieve an object: meta lookup, then an ordered consumer streams the chunks.

- **Tier: high** — score 6 (invocation 6 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.Get` — `func(ctx context.Context, name string, opts ...GetObjectOpt) (ObjectResult, error)`
- Symbol: `jetstream.ObjectStore.GetBytes` — `func(ctx context.Context, name string, opts ...GetObjectOpt) ([]byte, error)`
- Symbol: `jetstream.ObjectStore.GetString` — `func(ctx context.Context, name string, opts ...GetObjectOpt) (string, error)`
- Symbol: `jetstream.ObjectStore.GetFile` — `func(ctx context.Context, name, file string, opts ...GetObjectOpt) error`
- Pattern: multi-request; round trips: variable
- Wire messages: meta get + consumer create + ceil(size / chunk) deliveries
- Disk I/O: read; Raft: none; server state: ephemeral; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** [object.go/GetInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) first grabs the object's metadata message ($O.\<bucket>.M.\<name>) with a direct get ($JS.DIRECT.GET.\<stream>), which the stream leader answers in [stream.go/processDirectGetLastBySubjectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) straight from its store with no consumer, returning the NUID, size, chunk subject and expected SHA-256.
- **2.** Client's [object.go/Get](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) then creates an ephemeral ordered push consumer bound to the chunk subject ($O.\<bucket>.C.\<nuid>) via $JS.API.CONSUMER.CREATE, a request serviced by the shared $JS.API worker pool.
- **3.** The consumer's delivery loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) loads each chunk sequentially from the file store via [filestore.go/LoadMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) — cache-friendly sequential reads with no Raft consensus on the read path.
- **4.** Each chunk is pushed in order by [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) to the client's deliver subject; the client streams them through a net.Pipe reader in [object.go/Get](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) and verifies the running SHA-256, unsubscribing once a delivery's NumPending reaches zero.

## In practice

- **Scales with object size** — A Get delivers ceil(size / chunk) chunk messages through a temporary ordered push consumer, so both latency and server work grow with the object's byte count, plus a per-Get consumer create on the shared API pool. For hot small values, a KV bucket or an application-side cache avoids re-streaming.
- **Every Get re-streams and re-verifies** — There is no partial or cached read path: each Get spins up a consumer, streams the whole object through a pipe reader, and recomputes the SHA-256 client-side, so fetching the same object repeatedly repeats all of that work. Cache retrieved objects in the application rather than calling Get in a loop.
- **Links add a lookup hop** — Reading through a link first resolves the link's own meta and then does a second meta lookup into the target bucket before any chunk streams, so link reads cost an extra round trip. Point hot-path reads at the object directly when you can.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `jetstream.GetMsg`, `kv.Get`, `kv.History`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.

## Evidence

- Client: [jetstream/object.go — (\*obs).Get](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), [server/stream.go — (\*stream).processDirectGetLastBySubjectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go)
