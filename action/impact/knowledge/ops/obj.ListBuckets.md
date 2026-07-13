# obj.ListBuckets

Enumerate object store buckets by paging stream names/infos with the OBJ\_ prefix.

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStoreManager.ObjectStoreNames` — `func(ctx context.Context) ObjectStoreNamesLister`
- Symbol: `jetstream.ObjectStoreManager.ObjectStores` — `func(ctx context.Context) ObjectStoresLister`
- Pattern: multi-request; round trips: variable
- Wire messages: 1 request per page
- Disk I/O: none; Raft: none; server state: none; scan: full
- Choke points: shared-api-pool

## Flow

- **1.** [object.go/ObjectStoreNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) publishes a STREAM.NAMES request to $JS.API.STREAM.NAMES carrying a $O.\*.C.> subject filter and a page Offset via [jetstream.go/streamNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), queued through the account's shared $JS.API worker pool.
- **2.** In [jetstream\_api.go/jsStreamNamesRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) the handler scans every stream in the account, uses [jetstream.go/filteredStreams](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream.go) to retain those whose subjects collide with the $O.\*.C.> filter, sorts and slices the page from Offset up to JSApiNamesLimit (1024), and replies; the client keeps only OBJ\_-prefixed names and re-requests until Total is drained.

## In practice

- **Scales with total streams, not buckets** — The server scans every stream in the account and filters by the $O subject, so the cost tracks the account's total stream count — including KV buckets and plain streams — not just how many object-store buckets exist. A few buckets in a stream-heavy account still pay for the full scan.
- **Pages add round trips** — Results come back in pages of up to 1024 names, so a large account is drained over many sequential request-reply round trips. Enumerate buckets rarely and cache the list rather than calling it on any hot path.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/object.go — (\*jetStream).ObjectStoreNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamNamesRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
