# kv.PurgeDeletes

Compact the bucket: find delete/purge tombstones via a watcher, then purge each key's subject.

- **Tier: high** — score 11 (invocation 11 + 2 × steady 0)
- Group: Key-Value
- Symbol: `jetstream.KeyValue.PurgeDeletes` — `func(ctx context.Context, opts ...KVPurgeOpt) error`
- Pattern: multi-request; round trips: variable
- Wire messages: watcher drain + one STREAM.PURGE per tombstoned key
- Disk I/O: write; Raft: stream-propose; server state: ephemeral; scan: none
- Choke points: shared-api-pool, asset-lock

## Flow

- **1.** PurgeDeletes first opens a WatchAll in [kv.go/WatchAll](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go), asking the stream leader to create an ephemeral ordered push consumer with DeliverLastPerSubject through [jetstream\_api.go/jsConsumerCreateRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) so the client can observe the current head of every key's subject in the KV stream.
- **2.** [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) pushes each subject's last message back while [kv.go/PurgeDeletes](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) drains the watcher, keeps only entries whose Operation is KeyValueDelete or KeyValuePurge to build the tombstone list, then stops the watcher so numPending stops updating.
- **3.** For each tombstoned key [stream.go/Purge](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) issues a filtered $JS.API.STREAM.PURGE (subject $KV.\<bucket>.\<key>, with Keep=1 when the marker is newer than the 30m delete-markers threshold) through the shared $JS.API worker pool to [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) on the stream leader.
- **4.** At R>1 [jetstream\_cluster.go/jsClusteredStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes each per-key purge op to the stream's Raft group and a quorum must commit it before the purge takes effect.
- **5.** On commit every replica applies the purge in [stream.go/purgeLocked](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), calling [filestore.go/PurgeEx](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) to drop that key's older revisions from its message store while holding the stream write lock for the operation's duration, serializing it against other writers.
- **6.** [jetstream\_api.go/jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) returns a STREAM.PURGE ack for that key and [kv.go/PurgeDeletes](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go) loops to the next tombstone, so the sweep costs one propose-commit-purge round trip per deleted key.

## In practice

- **Two-phase maintenance sweep** — PurgeDeletes is a full watch pass over every key's head to find delete and purge tombstones, followed by one STREAM.PURGE proposal per tombstoned key. Cost is the whole-bucket scan plus a propose-commit round trip for each deletion, so it grows with both bucket size and the number of deleted keys.
- **Serializes on the write lock** — Each per-key purge holds the stream write lock for its duration, so a sweep over many deleted keys blocks other writers to the bucket for that whole run. Schedule it as off-peak maintenance rather than during write-heavy periods.
- **Recent tombstones are kept** — Deletes and purges newer than the delete-markers threshold, 30 minutes by default, are retained rather than compacted, so running PurgeDeletes right after deleting keys reclaims nothing. Let tombstones age past the threshold before sweeping.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/kv.go — (\*kvStore).PurgeDeletes](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/kv.go)
- Server: [server/jetstream\_api.go — (\*Server).jsStreamPurgeRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
