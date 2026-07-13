# jetstream.UpdateConsumer

Update an existing consumer's configuration.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamConsumerManager.UpdateConsumer` — `func(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.ConsumerManager.UpdateConsumer` — `func(ctx context.Context, cfg ConsumerConfig) (Consumer, error)`
- Symbol: `jetstream.StreamConsumerManager.UpdatePushConsumer` — `func(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)`
- Symbol: `jetstream.ConsumerManager.UpdatePushConsumer` — `func(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client publishes the update request (action=update) via [jetstream.go/UpdateConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) to $JS.API.CONSUMER.CREATE, one of the shared $JS.API worker-pool subjects; the meta leader unmarshals the ConsumerConfig in [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) and routes it to the clustered consumer path in [jetstream\_cluster.go/jsClusteredConsumerRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go).
- **2.** The meta leader confirms in [jetstream\_cluster.go/jsClusteredConsumerRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) that the consumer already exists and that only updatable fields changed, then proposes the updated consumer assignment to the JetStream metacontroller Raft group, where a quorum must commit the entry.
- **3.** Each replica applies the committed assignment in [jetstream\_cluster.go/processConsumerAssignment](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), updates the live consumer's config in place via [consumer.go/updateConfig](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) (re-evaluating interest when the filter subject changed), and rewrites the durable consumer meta file to disk in [filestore.go/writeConsumerMeta](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go).
- **4.** Once the assignment commits and applies, [jetstream\_cluster.go/processClusterCreateConsumer](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) returns the updated ConsumerInfo on the reply subject.

## In practice

- **As expensive as creating one** — Updating a consumer takes the same control-plane path as creating one — a metacontroller Raft proposal, quorum commit, and a durable meta-file rewrite on every replica — with no shortcut for touching a single field. Treat config changes as heavyweight and avoid clients that re-assert consumer config on every reconnect.
- **Update will not create** — The call fails when the consumer does not exist and only a subset of fields are mutable, so it cannot stand in for create-or-update. Decide up front which settings you can change live versus those that force deleting and recreating the consumer, and handle the missing-consumer case explicitly.
- **Filter-subject changes cost more** — Changing the filter subject makes the server re-evaluate interest across the stream, which is heavier than adjusting numeric or timing fields. Batch such changes and avoid remapping filters on a hot consumer.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).UpdateConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
