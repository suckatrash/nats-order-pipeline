# jetstream.PauseConsumer

Pause or resume a consumer's delivery until a deadline.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamConsumerManager.PauseConsumer` — `func(ctx context.Context, stream string, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error)`
- Symbol: `jetstream.ConsumerManager.PauseConsumer` — `func(ctx context.Context, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error)`
- Symbol: `jetstream.StreamConsumerManager.ResumeConsumer` — `func(ctx context.Context, stream string, consumer string) (*ConsumerPauseResponse, error)`
- Symbol: `jetstream.ConsumerManager.ResumeConsumer` — `func(ctx context.Context, consumer string) (*ConsumerPauseResponse, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client's [jetstream.go/PauseConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) publishes the pause request to $JS.API.CONSUMER.PAUSE.\<stream>.\<consumer> carrying the PauseUntil deadline; a $JS.API pool worker on the meta leader (the only node that services consumer config changes) picks it up in [jetstream\_api.go/jsConsumerPauseRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader in [jetstream\_api.go/jsConsumerPauseRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) clones the consumer assignment, stamps PauseUntil onto its config, and Propose()s the updated assignment to the JetStream meta Raft group, where a quorum must commit it through [jetstream\_cluster.go/applyMetaEntries](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go).
- **3.** As the committed meta entry applies on each replica, [consumer.go/updateConfig](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) rewrites the consumer's on-disk meta file via [filestore.go/writeConsumerMeta](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) so the pause deadline is durable across restarts (this is a config change, not a data-path toggle).
- **4.** Back in [jetstream\_api.go/jsConsumerPauseRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), the meta leader returns the pause ack (PauseUntil, Paused, PauseRemaining) via [jetstream\_api.go/sendAPIResponse](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) as soon as the proposal is accepted, without blocking on quorum commit.

## In practice

- **It's a meta-layer config write** — Pausing or resuming is not a runtime toggle — it clones the consumer assignment, commits a new PauseUntil through the JetStream meta Raft group, and rewrites the on-disk consumer meta on every replica. That is a full config-change round trip, so it costs far more than a data-path signal like ack or nak.
- **Prefer a deadline over toggling** — Every pause and resume pays the full meta-Raft config write, so using pause as a frequent flow-control lever is expensive. Set a PauseUntil deadline and let it expire on its own rather than repeatedly pausing and resuming — the consumer resumes automatically when the deadline passes.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).PauseConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerPauseRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
