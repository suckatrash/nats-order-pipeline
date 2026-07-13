# kv.Status

Fetch bucket status from the backing stream's info.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Status` — `func(ctx context.Context) (KeyValueStatus, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** [kv.go/Status](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) calls stream.Info, sending a STREAM.INFO request on $JS.API.STREAM.INFO.\<bucket-stream> to the stream leader, where it queues on the shared $JS.API worker pool (bounded at min(GOMAXPROCS,16)) before [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) answers it, with info requests explicitly deprioritized behind other API calls.
- **2.** The leader's handler [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) builds StreamInfo from the stream's in-memory config and cached state (no Raft, no disk), and [kv.go/Status](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) re-labels it client-side as KV bucket status: Values()=State.Msgs, History()=MaxMsgsPerSubject, TTL()=MaxAge.

## In practice

- **Just STREAM.INFO in disguise** — Status is a STREAM.INFO request relabeled with KV semantics, served from the leader's cached in-memory state with no Raft and no disk. The work itself is cheap; treat it as a metadata read, not a data operation.
- **Avoid polling in hot paths** — Because every call shares the bounded JetStream API worker pool and info requests are deprioritized there, calling Status on a request path couples your latency to how busy the server's API is. Read it periodically and cache the result rather than fetching it per operation.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Status](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
