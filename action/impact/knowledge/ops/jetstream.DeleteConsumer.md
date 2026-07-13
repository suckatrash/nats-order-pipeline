# jetstream.DeleteConsumer

Delete a consumer and its tracked state.

- **Tier: high** — score 7 (invocation 7 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.StreamConsumerManager.DeleteConsumer` — `func(ctx context.Context, stream string, consumer string) error`
- Symbol: `jetstream.ConsumerManager.DeleteConsumer` — `func(ctx context.Context, consumer string) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: write; Raft: meta-propose; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client sends an empty request in [jetstream.go/DeleteConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go) to $JS.API.CONSUMER.DELETE.\<stream>.\<consumer>; the message rides the shared $JS.API worker pool to the meta leader, where [jetstream\_api.go/jsConsumerDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) validates it and dispatches the clustered delete.
- **2.** The meta leader proposes a delete-consumer assignment to the metagroup Raft in [jetstream\_cluster.go/jsClusteredConsumerDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go); a quorum must commit before the consumer is removed from cluster state.
- **3.** On commit each replica stops the consumer in [consumer.go/stopWithFlags](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), deregisters it from the stream under a brief write lock, and deletes its consumer store (pending/redelivery state); on interest-policy streams [consumer.go/cleanupNoInterestMessages](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) runs a data-proportional cleanup of now-uninteresting messages outside the lock.
- **4.** After the delete assignment commits across the metagroup quorum and is applied, the consumer leader returns the success ack to the client's reply inbox in [jetstream\_cluster.go/processClusterDeleteConsumer](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go).

## In practice

- **Interest streams turn delete into cleanup** — On a limits-policy stream deleting a consumer is pure control-plane bookkeeping, but on an interest or work-queue stream removing the last interested consumer triggers a cleanup of now-uninteresting messages that is proportional to the data those messages occupy. Deleting consumers on a busy interest stream therefore does real disk work, not just a quick deregister.
- **Discarding pending state costs more** — The delete discards the consumer's pending and redelivery state on every replica, so a consumer carrying a large in-flight backlog costs more to remove than an idle one whose tracked state is small.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **js.mu + meta raft leader** (scope: cluster) with `jetstream.CreateStream`, `jetstream.UpdateStream`, `jetstream.DeleteStream`, `jetstream.CreateConsumer`, `jetstream.UpdateConsumer`, `jetstream.PauseConsumer`, `kv.CreateBucket`, `kv.DeleteBucket`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Seal`: Every metadata mutation across every account serializes on the server's js.mu write lock and then a single meta-raft proposal stream on the one cluster meta leader. Bulk asset creation, deletion, or churn bottlenecks cluster-wide regardless of which streams are involved.

## Evidence

- Client: [jetstream/jetstream.go — (\*jetStream).DeleteConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/jetstream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerDeleteRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
