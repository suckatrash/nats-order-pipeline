# kv.Get

Read a key's latest (or a specific) revision via direct get last-per-subject.

- **Tier: low** — score 3 (invocation 3 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Get` — `func(ctx context.Context, key string) (KeyValueEntry, error)`
- Symbol: `jetstream.KeyValue.GetRevision` — `func(ctx context.Context, key string, revision uint64) (KeyValueEntry, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: read; Raft: none; server state: none; scan: none

## Flow

- **1.** With AllowDirect set (the KV default), the client's [kv.go/Get](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) skips the stream-leader STREAM.MSG.GET and in [stream.go/getMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) issues a DIRECT.GET request keyed by last-for subject $KV.\<bucket>.\<key> to $JS.API.DIRECT.GET.\<stream>.\<subject>, awaiting the value on its reply inbox.
- **2.** The per-stream direct-get queue group (dgetGroup) established by [stream.go/subscribeToDirect](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) fields the request in [stream.go/processDirectGetLastBySubjectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) on any up-to-date replica, bypassing the stream leader, the Raft group, and the shared $JS.API handler pool, so read load spreads across replicas with no quorum or leader forwarding.
- **3.** Holding the stream/store read lock in [stream.go/getDirectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [filestore.go/LoadLastMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) does an index-assisted (psim) last-per-subject lookup and reads the value from the message block, faulting a cold block in from disk on a cache miss (which briefly stalls the publish write lock).
- **4.** Through [stream.go/processDirectGetLastBySubjectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), the serving replica returns the stored value plus Nats-Stream/Nats-Subject/Nats-Sequence headers from [stream.go/getDirectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) directly to the client's reply inbox, or a 404 status header when no message matches the subject.

## In practice

- **Cost tracks value size and cache locality** — A hit is an index-assisted last-per-subject lookup, so the dominant work is copying the value out — larger values cost more to read. On a cache miss the server must fault a cold message block in from disk, so buckets whose working set falls out of the block cache pay disk-read latency.
- **Cold reads stall concurrent writes** — The lookup holds the stream and store read locks; when a cold block faults in from disk, that fault briefly stalls the publish write lock on the same stream. A read-heavy bucket with poor cache locality can therefore add tail latency to writes on the same replica.
- **Reads scale out across replicas** — Because a Get is servable by any up-to-date replica rather than the leader alone, read throughput grows with replica count; add replicas to absorb hot read paths and prefer Get over a leader-routed STREAM.MSG.GET.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `jetstream.GetMsg`, `kv.History`, `obj.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Get](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/stream.go — (\*stream).processDirectGetLastBySubjectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go)
