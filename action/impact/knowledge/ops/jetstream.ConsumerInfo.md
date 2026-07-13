# jetstream.ConsumerInfo

Fetch consumer state ($JS.API.CONSUMER.INFO); also how a Consumer handle is bound.

- **Tier: moderate** — score 4 (invocation 4 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamConsumerManager.Consumer` — `func(ctx context.Context, stream string, consumer string) (Consumer, error)`
- Symbol: `jetstream.ConsumerManager.Consumer` — `func(ctx context.Context, consumer string) (Consumer, error)`
- Symbol: `jetstream.StreamConsumerManager.PushConsumer` — `func(ctx context.Context, stream string, consumer string) (PushConsumer, error)`
- Symbol: `jetstream.ConsumerManager.PushConsumer` — `func(ctx context.Context, consumer string) (PushConsumer, error)`
- Symbol: `jetstream.Consumer.Info` — `func(context.Context) (*ConsumerInfo, error)`
- Symbol: `jetstream.PushConsumer.Info` — `func(context.Context) (*ConsumerInfo, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool, asset-lock

## Flow

- **1.** The client sends a CONSUMER.INFO request on $JS.API.CONSUMER.INFO.\<stream>.\<consumer> via [consumer.go/Info](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go), answered only by the consumer leader; binding a handle through Consumer()/PushConsumer() in [consumer.go/getConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go) issues exactly this one INFO to hydrate it.
- **2.** The request rides the deprioritized $JS.API info queue, which [jetstream\_api.go/apiDispatch](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) drains with the bounded shared API worker pool (16 goroutines served only when the main queue is empty); once that info queue passes its limit (default 10k) the server drains every request pending on it and emits a limit-reached advisory rather than block.
- **3.** The leader's worker in [jetstream\_api.go/jsConsumerInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) takes the consumer's full write lock o.mu in [consumer.go/infoWithSnapAndReply](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) — the same mutex the delivery loop holds, so info polls serialize against delivery — computes num-pending (O(pending)) and serves ConsumerInfo entirely from in-memory state, then publishes it to the reply subject.

## In practice

- **Monitoring fleets are the real cost** — A single info call is cheap, but cost scales with poll rate times consumer count. Dashboards polling every consumer funnel through one bounded API worker pool and each call contends with that consumer's delivery loop, so widen the poll interval and share results rather than polling per subscriber.
- **The API queue drops under overload** — Info rides a deprioritized queue drained only when the main API queue is idle; past its 10k limit the server discards every pending JS API request and emits an advisory rather than blocking. Bursty polling can thus lose unrelated API calls, so cap concurrency and back off when you see the advisory.
- **Binding a handle costs one info** — Consumer() and PushConsumer() each perform exactly one INFO round trip to hydrate the handle, so repeatedly rebinding multiplies these calls. Bind once and reuse the handle for the life of the consumer instead of rebinding per operation.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.

## Evidence

- Client: [jetstream/consumer.go — getConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go), [jetstream/consumer.go — (\*pullConsumer).Info](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/consumer.go — (\*consumer).infoWithSnapAndReply](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), [server/jetstream\_api.go — (\*jetStream).apiDispatch](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
