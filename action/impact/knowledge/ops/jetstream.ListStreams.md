# jetstream.ListStreams

Enumerate streams (full info or names), paged through the JS API.

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamManager.ListStreams` — `func(context.Context, ...StreamListOpt) StreamInfoLister`
- Symbol: `jetstream.StreamManager.StreamNames` — `func(context.Context, ...StreamListOpt) StreamNameLister`
- Symbol: `jetstream.StreamManager.StreamNameBySubject` — `func(ctx context.Context, subject string) (string, error)`
- Pattern: multi-request; round trips: variable
- Wire messages: 1 request per page (1024 names / 256 infos per page)
- Disk I/O: none; Raft: none; server state: none; scan: full
- Choke points: shared-api-pool

## Flow

- **1.** The client's [jetstream.go/ListStreams](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) issues a paged request to $JS.API.STREAM.LIST, delivered to the meta leader and dispatched off its shared $JS.API worker pool into [jetstream\_api.go/jsStreamListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go); for LIST the leader full-scans the account's stream assignments and scatter-gathers current StreamInfo from every stream leader via [jetstream\_cluster.go/jsClusteredStreamListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), whereas StreamNames answers directly from the in-memory assignment map.
- **2.** The meta leader in [jetstream\_api.go/jsStreamListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) returns one page (up to 256 StreamInfos for LIST, 1024 names for NAMES) carrying Total/Offset/Limit, and the client's [jetstream.go/StreamNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) re-requests with a bumped offset until every page is drained.

## In practice

- **Prefer NAMES over LIST** — StreamNames answers from the meta leader's in-memory assignment map, but ListStreams additionally scatter-gathers live StreamInfo from every stream leader — far more work and network fan-out. Ask for names when you only need to know which streams exist, use StreamNameBySubject to resolve a single subject, and reach for full info only when you need per-stream state.
- **Scales with the account's stream count** — Both variants full-scan the account's stream set and page it (256 infos or 1024 names per page), so the scan cost and the number of requests both grow with the total stream count. An account with thousands of streams turns a listing into many round trips.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).ListStreams](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), [jetstream/jetstream.go — (\*jetStream).StreamNames](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/jetstream\_api.go — (\*Server).jsStreamNamesRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
