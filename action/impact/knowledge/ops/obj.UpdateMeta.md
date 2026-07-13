# obj.UpdateMeta

Rename or re-describe an object: publish new meta, purge the old meta subject.

- **Tier: high** — score 10 (invocation 10 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.UpdateMeta` — `func(ctx context.Context, name string, meta ObjectMeta) error`
- Pattern: multi-request; round trips: 2
- Wire messages: meta publish + old-meta purge
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client first writes the new metadata in [object.go/publishMeta](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go), a JetStream publish of the rollup meta message to $O.\<bucket>.M.\<name> carrying a Rollup:sub header, which the stream leader ingests in [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) before returning the PubAck folded here.
- **2a.** At R>1 the stream leader proposes the meta write in [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) to the stream's Raft group, where followers replicate the entry and a quorum must commit before it is applied.
- **2b.** Concurrently each replica runs the rollup-aware write in [stream.go/processJetStreamMsgWithRollup](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) and appends the record via [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go), where the Rollup:sub collapses the meta subject to just this one message and fsync is batched on SyncInterval (default 2m).
- **3.** On a rename the client then issues a $JS.API.STREAM.PURGE via [stream.go/Purge](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go), which the server's shared JS API worker pool dispatches to [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) targeting the old-name meta subject $O.\<bucket>.M.\<old>, leaving the object's chunks untouched.
- **4.** The leader runs the subject-scoped purge in [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), applying it to the file store via [filestore.go/PurgeEx](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) as a second Raft-committed, durably-stored control write before returning the purge ack.

## In practice

- **Cost is fixed, not size-dependent** — UpdateMeta rewrites only the object's metadata entry, so it costs the same two small control writes whether the object is a kilobyte or a terabyte. Its high cost comes from durably committing those writes through consensus, not from any data volume.
- **Rename doubles the writes** — Updating a description is a single meta write, but a rename adds a second Raft-committed purge of the old-name subject, roughly doubling the cost. Keep renames off hot paths and re-describe in place when the name does not actually need to change.
- **Rename fails on name collision** — Renaming to a name that already exists is rejected rather than silently overwriting the existing object, so treat the collision as an expected error your code must handle.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.

## Evidence

- Client: [jetstream/object.go — (\*obs).UpdateMeta](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [server/jetstream\_api.go — (\*Server).jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
