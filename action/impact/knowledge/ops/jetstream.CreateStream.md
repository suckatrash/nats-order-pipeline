# jetstream.CreateStream

Create a stream: meta-leader Raft proposal, store directories, raft group at R>1.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamManager.CreateStream` — `func(ctx context.Context, cfg StreamConfig) (Stream, error)`
- Symbol: `jetstream.StreamManager.CreateOrUpdateStream` — `func(ctx context.Context, cfg StreamConfig) (Stream, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client sends the stream config in [jetstream.go/CreateStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) to the cluster's meta leader, where a $JS.API pool goroutine handles it in [jetstream\_api.go/jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **2.** The meta leader in [jetstream\_cluster.go/jsClusteredStreamRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes the create to the metagroup Raft, and a quorum of peers must commit it.
- **3.** On commit each replica materializes the stream through [jetstream\_cluster.go/jsClusteredStreamRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) — store directories and Raft group state written to disk.
- **4.** The meta leader replies to the client with the created stream's info via [jetstream\_api.go/jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).

## In practice

- **Provision streams, don't create per request** — Creating a stream is a control-plane operation — meta-leader routing, a meta-Raft commit, and directory and Raft state written on every replica. Provision streams at setup or deploy time and reuse them; never create one on the data path or per request.
- **Cost scales with replicas and subjects** — Each added replica means another Raft group to create and maintain, each subject in the config adds to the interest mapping the cluster tracks, and every stream enlarges the meta-state snapshot shipped to new or lagging peers. Size replica count and subject sets to what the workload actually needs.
- **CreateOrUpdate can cost two requests** — CreateOrUpdateStream issues a second update request when the stream already exists, so using it in an idempotent setup path can double the control-plane traffic. If you only need create-once semantics, prefer the plain create and handle the already-exists case.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).CreateStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/jetstream\_cluster.go — (\*Server).jsClusteredStreamRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go)
