# kv.Watch

Live-watch keys: ordered ephemeral push consumer delivering current values then updates.

- **Tier: high** — score 11 (invocation 3 + 2 × steady 4)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.Watch` — `func(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error)`
- Symbol: `jetstream.KeyValue.WatchAll` — `func(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error)`
- Symbol: `jetstream.KeyValue.WatchFiltered` — `func(ctx context.Context, keys []string, opts ...WatchOpt) (KeyWatcher, error)`
- Pattern: multi-request; round trips: 1
- Wire messages: ordered consumer create + deliver-subject SUB
- Disk I/O: none; Raft: none; server state: ephemeral; scan: none
- Choke points: shared-api-pool

## Steady state

- Client traffic: flow-control and heartbeat responses
- Server work per message: load revision from store; push to watcher
- Interval work: idle heartbeats; flow control; gap-triggered consumer recreation
- Disk I/O: read; Raft: none

## Flow

- **1.** [kv.go/WatchFiltered](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) sends a $JS.API.CONSUMER.CREATE request through the shared $JS.API worker pool, where [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) builds an ordered, ephemeral (R1 memory-storage), ack-none, flow-controlled push consumer created 'direct' on the stream leader with no meta proposal, starting deliver-last-per-subject by default, deliver-all under IncludeHistory, or deliver-new under UpdatesOnly, with MetaOnly requesting headers-only delivery.
- **2.** The server pushes the current value of each matching key as the initial replay (scaled by matching key count) and then live updates to the watcher's deliver subject over the ordered consumer's delivery loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go).
- **3.** Steady state: [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) reads each new revision from the store and pushes it to the watcher, interleaved with the client's idle heartbeats and flow-control responses; a detected sequence gap makes [js.go/resetOrderedConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/js.go) tear down and transparently recreate the ordered consumer client-side.

## In practice

- **Each watcher is a live consumer** — A watch is a real server-side ordered push consumer, not a cheap subscription, so N watchers on a bucket mean N delivery machines the leader must feed. Share one watcher across consumers where you can, and tear watchers down when idle rather than leaving them open.
- **Initial replay scales with keys** — On creation the watcher replays the current value of every matching key before going live, so start-up cost grows with how many keys match the filter. Watching a whole bucket replays the entire keyspace; filter to the keys you need, or use UpdatesOnly to skip the replay entirely.
- **Gaps trigger silent recreation** — The ordered consumer is ephemeral, ack-none and flow-controlled, and the client transparently tears it down and recreates it on any detected sequence gap. Recreation restarts delivery and, depending on start mode, can replay again, so a flaky link or a slow consumer that keeps triggering gaps quietly multiplies the cost.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).Watch](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go), [jetstream/kv.go — (\*kvStore).WatchFiltered](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
