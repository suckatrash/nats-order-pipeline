# jetstream.UpdateStream

Update stream configuration via meta-leader proposal.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamManager.UpdateStream` — `func(ctx context.Context, cfg StreamConfig) (Stream, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client marshals the new StreamConfig and sends the request via [jetstream.go/UpdateStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) on the stream's $JS.API.STREAM.UPDATE subject; a shared $JS.API pool worker on the meta leader validates the change in [jetstream\_api.go/jsStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) against the existing assignment (subject-overlap, mirror-immutability, replica and consumer-limit checks) and routes it to the clustered update path in [jetstream\_cluster.go/jsClusteredStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go).
- **2.** The meta leader proposes the updated stream assignment in [jetstream\_cluster.go/jsClusteredStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) to the metacontroller Raft group, where a quorum of meta peers must replicate and commit the config-change entry.
- **3.** On commit each group member applies the entry via [jetstream\_cluster.go/processClusterUpdateStream](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) and rewrites its live stream's durable config on disk in [stream.go/updateWithAdvisory](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go); a replica or placement change additionally rescales the stream's Raft group and catches up newly added peers by copying the whole stream.
- **4.** Once the assignment is applied, the stream leader replies in [jetstream\_cluster.go/processClusterUpdateStream](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) with the updated StreamInfo (new config plus current state).

## In practice

- **Config-only updates are cheap-ish** — When an update touches only retention, limits, or similar fields, it is the same control-plane path as create without initial provisioning: a metacontroller Raft commit and a durable config rewrite per replica. That is bounded work, so ordinary config tweaks are safe to apply on a live stream.
- **Replica or placement changes move data** — Changing the replica count or placement turns a metadata update into a data migration — the server rescales the stream's Raft group and new peers catch up by copying the entire stream. Cost scales with stored data volume, so schedule these during low-traffic windows and change replication factor one step at a time.
- **Subject remapping re-routes interest** — Editing a stream's subjects remaps which published subjects it captures and forces interest to be re-evaluated across the account. Get the subject set right at creation where you can, since remapping a busy stream is more disruptive than changing numeric limits.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.DeleteConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).UpdateStream](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamUpdateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
