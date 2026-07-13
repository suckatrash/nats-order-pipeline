# jetstream.CreateConsumer

Create (or idempotently re-create) a consumer via meta-leader proposal.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamConsumerManager.CreateConsumer` — `func(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.ConsumerManager.CreateConsumer` — `func(ctx context.Context, cfg ConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.StreamConsumerManager.CreateOrUpdateConsumer` — `func(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.ConsumerManager.CreateOrUpdateConsumer` — `func(ctx context.Context, cfg ConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.StreamConsumerManager.CreatePushConsumer` — `func(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)`
- Symbol: `jetstream.ConsumerManager.CreatePushConsumer` — `func(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)`
- Symbol: `jetstream.StreamConsumerManager.CreateOrUpdatePushConsumer` — `func(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)`
- Symbol: `jetstream.ConsumerManager.CreateOrUpdatePushConsumer` — `func(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client publishes the ConsumerConfig to $JS.API.CONSUMER.CREATE.\<stream>\[.\<consumer>] in [jetstream.go/CreateOrUpdateConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go); the request lands on a shared $JS.API worker-pool goroutine at the meta leader in [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), where durable/replicated consumers route (unlike ephemeral direct consumers, which the stream leader creates in memory with no meta proposal).
- **2.** The meta leader builds a consumerAssignment in [jetstream\_cluster.go/jsClusteredConsumerRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) and proposes it to the JetStream meta Raft group; a quorum of meta peers must commit it through [jetstream\_cluster.go/processConsumerAssignment](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) before the consumer is materialized on any node.
- **3.** On commit each replica allocates the delivery machine in [consumer.go/addConsumer](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) (pending/redelivery tracking, ack floor, and its own Raft group when replicated) and writes the consumer meta/state file to disk in [filestore.go/writeConsumerMeta](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go), briefly holding the stream write lock.
- **4.** Once the assignment is committed and the consumer exists, [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) returns its ConsumerInfo (config plus delivery/ack state) to the client.

## In practice

- **Ephemeral consumers skip consensus** — A durable or replicated consumer routes through the meta leader and inherits the stream's replication, while an ephemeral direct consumer on a limits stream is created in memory by the stream leader with no meta proposal and reaped after its inactive threshold. For short-lived or throwaway reads, ephemeral is far cheaper.
- **Don't churn consumers** — Registration briefly holds the stream write lock and, when durable, hits the same meta leader every stream create uses, so rapid create and delete cycles steal ingest throughput and load the control plane. Create consumers once and reuse them rather than per request or per batch.
- **Replicas and start position add cost** — Cost grows with replica count (a Raft group per replica), with the number of filter subjects mapped, and with starting position — DeliverAll on a deep stream sets up a long tail the new consumer must walk. Pick the narrowest filters and the latest acceptable start to keep creation and first delivery cheap.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).CreateOrUpdateConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go), [jetstream/consumer.go — upsertConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
