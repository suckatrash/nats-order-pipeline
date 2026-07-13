# obj.Watch

Live-watch object metadata changes via an ordered consumer on the meta subjects.

- **Tier: high** — score 11 (invocation 3 + 2 × steady 4)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.Watch` — `func(ctx context.Context, opts ...WatchOpt) (ObjectWatcher, error)`
- Pattern: multi-request; round trips: 1
- Wire messages: ordered consumer create + deliver-subject SUB
- Disk I/O: none; Raft: none; server state: ephemeral; scan: none
- Choke points: shared-api-pool

## Steady state

- Client traffic: flow-control and heartbeat responses
- Server work per message: load meta entry; push to watcher
- Interval work: idle heartbeats; flow control; gap-triggered consumer recreation
- Disk I/O: read; Raft: none

## Flow

- **1.** Watch in [object.go/Watch](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) subscribes to a private deliver subject and sends a CONSUMER.CREATE for an ordered ephemeral push consumer bound to the OBJ\_\<bucket> stream on the meta subjects $O.\<bucket>.M.>; riding the shared $JS.API worker pool, [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) creates the consumer directly on the stream leader with no meta-layer Raft proposal.
- **2.** With DeliverLastPerSubject the leader's delivery loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) replays the latest meta entry for every object (one message per subject, scaling with object count) and [consumer.go/deliverMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) pushes each to the watcher's deliver subject, delivering only meta entries and not chunks so per-message volume stays low.
- **3.** Steady state: once the initial replay drains, the ordered consumer in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) idles, and the client keeps answering the server's periodic idle heartbeats and flow-control markers via [js.go/checkForFlowControlResponse](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/js.go), while each later meta write is loaded from the store and pushed to the watcher.

## In practice

- **Skip the replay with UpdatesOnly** — By default a watcher first replays the latest metadata entry for every object, so startup cost grows linearly with bucket size and dominates on large buckets. If you only care about changes from now on, pass UpdatesOnly to skip that initial replay entirely.
- **Each watcher is a live consumer** — A watcher is a long-lived ordered push consumer that keeps costing while open: periodic heartbeats, flow-control exchanges, and occasional gap-triggered consumer recreation. Close watchers you no longer need rather than leaving idle ones running, since each carries the same machinery as a KV watcher.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/object.go — (\*obs).Watch](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
