# obj.Delete

Delete an object: purge its chunks, keep a deleted-marker meta entry.

- **Tier: high** — score 12 (invocation 12 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.Delete` — `func(ctx context.Context, name string) error`
- Pattern: multi-request; round trips: 3
- Wire messages: meta lookup + rollup meta publish (deleted=true) + chunk-subject purge
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none
- Choke points: shared-api-pool, asset-lock

## Flow

- **1.** [object.go/Delete](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) fetches the object meta (GetInfo show-deleted), then publishes a rollup delete-marker to $O.\<bucket>.M.\<name> via [object.go/publishMeta](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) carrying Nats-Rollup:sub; the stream leader durably writes this single tombstone (deleted=true, size/chunks/digest zeroed) in [stream.go/processJetStreamMsgWithRollup](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) in place of every prior revision on that meta subject.
- **2.** Client then issues $JS.API.STREAM.PURGE from [object.go/Delete](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) filtered to the object's chunk subject $O.\<bucket>.C.\<nuid>; the request rides the shared $JS.API worker pool and only the stream leader answers it in [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go).
- **3.** At R>1 the leader proposes a purgeStreamOp entry (carrying the chunk-subject filter) to the stream's Raft group in [jetstream\_cluster.go/jsClusteredStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), which a quorum must commit before it applies.
- **4.** On commit each replica's [stream.go/purgeLocked](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) runs store.PurgeEx in [filestore.go/PurgeEx](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) to delete every chunk under $O.\<bucket>.C.\<nuid> while holding the stream write lock, so the stall scales with the object's chunk count.
- **5.** Once the purge applies in [jetstream\_cluster.go/applyStreamEntries](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go), the leader returns the JSApiStreamPurgeResponse with the purged count; chunk storage is reclaimed immediately while the tombstoned meta entry survives for watchers and show-deleted listings.

## In practice

- **Large deletes stall the stream** — The chunk purge holds the stream write lock for time proportional to the object's chunk count, so deleting a large object blocks every other writer on that bucket until it finishes. Delete big objects off-peak and keep chunk sizes sane so no single delete monopolizes the lock.
- **Storage frees, tombstones linger** — Delete reclaims the object's chunk bytes immediately but leaves a small deleted-marker meta entry so watchers and show-deleted listings still see the name; heavy delete churn keeps metadata growing even as data storage is freed. Recreate or compact the bucket if stale tombstones pile up.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/object.go — (\*obs).Delete](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go)
