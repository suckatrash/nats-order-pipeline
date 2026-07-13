# obj.List

List objects by draining a meta watcher to the end.

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.List` — `func(ctx context.Context, opts ...ListObjectsOpt) ([]*ObjectInfo, error)`
- Pattern: multi-request; round trips: variable
- Wire messages: consumer create + one meta delivery per object + teardown
- Disk I/O: read; Raft: none; server state: ephemeral; scan: full
- Choke points: shared-api-pool

## Flow

- **1.** [object.go/List](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) opens an ephemeral ordered push consumer through [object.go/Watch](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) via $JS.API.CONSUMER.CREATE on the OBJ\_\<bucket> stream, filtered to the meta subject $O.\<bucket>.M.> with DeliverLastPerSubject so only each object's latest metadata entry replays; [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) handles the create on the shared $JS.API handler pool.
- **2.** The ordered consumer's delivery loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) walks the stream and loads each object's latest meta message from the store via [filestore.go/LoadMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) — a full read scan whose cost grows with the object count in the bucket.
- **3.** [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) pushes each meta entry to the client as a consumer delivery (one message per object); [object.go/List](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) drains updates until NumPending hits zero, then stops the watcher and tears the temporary consumer down.

## In practice

- **Scales with object count** — List drains a temporary consumer that replays every object's latest meta entry, so it is a full read scan costing on the order of one delivery plus one disk read per object. A bucket with a million objects means a million of each; the price grows without bound as the bucket fills.
- **Keep it off hot paths** — Because the cost grows with bucket size, calling List on a request path turns a large bucket into a latency and load spike. Cache the enumeration or track changes with a long-lived watcher, and refresh on a schedule rather than per request.
- **Cost tracks count, not size** — The sweep reads only meta entries, never object payloads, so listing is unaffected by how large individual objects are. A bucket of many tiny objects is just as expensive to enumerate as the count implies — control object count, not just total bytes, if you list regularly.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/object.go — (\*obs).List](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
