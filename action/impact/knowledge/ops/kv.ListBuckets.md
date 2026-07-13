# kv.ListBuckets

Enumerate KV buckets by paging stream names/infos filtered to the KV\_ prefix.

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValueManager.KeyValueStoreNames` — `func(ctx context.Context) KeyValueNamesLister`
- Symbol: `jetstream.KeyValueManager.KeyValueStores` — `func(ctx context.Context) KeyValueLister`
- Pattern: multi-request; round trips: variable
- Wire messages: 1 request per page
- Disk I/O: none; Raft: none; server state: none; scan: full
- Choke points: shared-api-pool

## Flow

- **1.** [kv.go/KeyValueStoreNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) pages the enumeration through [jetstream.go/streamNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), each $JS.API.STREAM.NAMES request carrying a $KV.\*.> subject filter and a growing offset, landing in the shared $JS.API worker pool and routed to the meta leader.
- **2.** In [jetstream\_api.go/jsStreamNamesRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) the meta leader scans every stream assignment in the account under a metadata read lock, keeping those whose subjects [sublist.go/SubjectsCollide](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go) matches against $KV.\*.>, then sorts and slices the page (capped at JSApiNamesLimit=1024) and replies with the names; the client strips the KV\_ prefix to yield bucket names, and no disk or Raft is touched.

## In practice

- **Scales with total stream count** — The meta leader scans every stream assignment in the account, not just KV buckets, and then filters to the KV\_ prefix. Cost grows with the total number of streams you run, so an account with many non-KV streams makes bucket enumeration more expensive even when few buckets exist.
- **Metadata-only but paged** — Enumeration reads in-memory stream assignments under a read lock and touches no disk or Raft, but it returns at most 1024 names per request, so a very large account needs several round trips. It is the same paged scan as listing all streams, just filtered to the KV\_ prefix.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/kv.go — (\*jetStream).KeyValueStoreNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamNamesRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
