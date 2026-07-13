# jetstream.StreamInfo

Fetch stream state and configuration ($JS.API.STREAM.INFO); also how a Stream handle is bound.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamManager.Stream` — `func(ctx context.Context, stream string) (Stream, error)`
- Symbol: `jetstream.Stream.Info` — `func(ctx context.Context, opts ...StreamInfoOpt) (*StreamInfo, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client sends an INFO request on $JS.API.STREAM.INFO.\<stream> via [stream.go/Info](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) (optionally carrying SubjectsFilter/DeletedDetails/Offset); it lands on the shared $JS.API queue-worker pool where [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) handles it on the stream leader, and [jetstream.go/Stream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) issues exactly one such INFO to validate and bind a stream handle.
- **2.** [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) answers from in-memory stream state on the leader: a plain request returns an O(1) FastState snapshot, but SubjectsFilter or DeletedDetails make [stream.go/stateWithDetail](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) walk the per-subject index and deleted maps under the store read lock for data-proportional time, tallying subjects through [filestore.go/SubjectsTotals](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) and paging details at JSMaxSubjectDetails.

## In practice

- **Plain info is cheap; details are not** — A plain STREAM.INFO is an O(1) FastState read answered from memory, but requesting SubjectsFilter or DeletedDetails walks the per-subject index and deleted maps under the store read lock, taking time proportional to the data and stalling ingest on that stream. Ask for subject or deleted detail only when you need it, and expect wide streams to page results at JSMaxSubjectDetails.
- **Bind the handle once** — jetstream.Stream() performs exactly one INFO to validate and bind the handle, so cache and reuse the returned handle instead of re-binding per operation. Re-fetching stream info in a hot path adds a round trip through the shared JS API pool for state that rarely changes.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).Stream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), [jetstream/stream.go — (\*stream).Info](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
