# obj.Status

Fetch bucket status from the backing stream's info.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.Status` — `func(ctx context.Context) (ObjectStoreStatus, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** Status in [object.go/Status](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) fetches the backing OBJ\_\<bucket> stream's info via $JS.API.STREAM.INFO.\<stream>, which [jetstream\_api.go/apiDispatch](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) queues onto the shared JetStream API pool's deprioritized info queue (jsAPIRoutedInfoReqs) where only the stream leader answers in [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The stream leader answers in [jetstream\_api.go/jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) from in-memory stream state (mset.stateWithDetail) with no disk read or Raft round-trip, returning a STREAM.INFO that [object.go/Status](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) re-labels with object-store semantics like bucket size, backing store, and sealed state.

## In practice

- **Cheap read, still a round trip** — Status is answered from the stream leader's in-memory state with no disk read or Raft consensus, so an individual call is inexpensive. It is still a full network request-reply, so read it on demand rather than polling in a tight loop, and cache the result if you need it repeatedly.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/object.go — (\*obs).Status](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
