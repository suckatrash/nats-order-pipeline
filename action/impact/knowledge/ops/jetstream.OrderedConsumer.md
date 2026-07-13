# jetstream.OrderedConsumer

Create an ordered consumer handle: ephemeral, ack-none, R1, recreated on any gap.

- **Tier: low** — score 3 (invocation 3 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamConsumerManager.OrderedConsumer` — `func(ctx context.Context, stream string, cfg OrderedConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.ConsumerManager.OrderedConsumer` — `func(ctx context.Context, cfg OrderedConsumerConfig) (Consumer, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: ephemeral; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client's [jetstream.go/OrderedConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) eagerly issues CreateOrUpdateConsumer through [consumer.go/upsertConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go), publishing a $JS.API.CONSUMER.CREATE request dispatched through the shared $JS.API handler pool to [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), for an ephemeral pull consumer that is ack-none, R1, memory-storage, with a 5m inactive threshold.
- **2.** The stream leader's create handler in [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) instantiates the ephemeral, R1 (no consumer Raft group) memory-storage consumer via [consumer.go/addConsumer](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) and replies with the consumer info, which the client wraps as its current pullConsumer; any later sequence gap or heartbeat miss triggers [ordered.go/reset](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/ordered.go), an async CONSUMER.DELETE plus a fresh CONSUMER.CREATE.

## In practice

- **Gaps recreate the consumer** — Any detected sequence gap or missed heartbeat tears the consumer down and recreates it — one async CONSUMER.DELETE plus a fresh CONSUMER.CREATE. On a flaky network this quiet recreation becomes a steady stream of CONSUMER.CREATE churn against the API pool, so an unstable link costs far more than the initial handle.
- **Direct create only on limits streams** — On limits-based streams the ordered consumer is created directly on the stream leader with no meta-Raft proposal, which keeps setup cheap. On workqueue, interest, or sourced streams the create still routes through the meta leader, so the same call pays a consensus round there.
- **Delivery cost is counted elsewhere** — Creating the handle is cheap; the recurring expense of pulling and acking messages is accounted under Consume and Fetch, not here. Size those operations for your throughput and treat this create as one-time setup that only repeats when a gap resets it.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).OrderedConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), [jetstream/ordered.go — (\*orderedConsumer).reset](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/ordered.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
