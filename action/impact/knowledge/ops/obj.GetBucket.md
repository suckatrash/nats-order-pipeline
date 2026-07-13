# obj.GetBucket

Bind an object store handle by validating the backing stream.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStoreManager.ObjectStore` — `func(ctx context.Context, bucket string) (ObjectStore, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** [object.go/ObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) resolves the bucket's backing stream name OBJ\_\<bucket> and, via [jetstream.go/Stream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), sends a single STREAM.INFO request on $JS.API.STREAM.INFO.\<stream>, which [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) answers from the shared $JS.API worker pool with no raft or disk work.
- **2.** The server replies in [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) with the stream's StreamInfo, which [object.go/ObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) uses to bind the object store handle; a missing stream is mapped to ErrBucketNotFound.

## In practice

- **Cache the bucket handle** — Binding a bucket costs one STREAM.INFO round trip that only validates the backing OBJ\_ stream exists. Resolve the handle once at startup and reuse it; re-binding before every Get or Put doubles your round trips for no benefit.
- **Cost is independent of bucket size** — Binding reads only stream metadata, so the price is a single round trip regardless of how many objects the bucket holds or how large they are. Unlike Get or List, it never scales with the data in the bucket.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/object.go — (\*jetStream).ObjectStore](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
