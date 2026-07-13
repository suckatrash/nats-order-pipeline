# obj.Put

Store an object: N chunked publishes plus a rollup meta publish; replaces any prior version.

- **Tier: high** — score 9 (invocation 9 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.Put` — `func(ctx context.Context, obj ObjectMeta, reader io.Reader) (*ObjectInfo, error)`
- Symbol: `jetstream.ObjectStore.PutBytes` — `func(ctx context.Context, name string, data []byte) (*ObjectInfo, error)`
- Symbol: `jetstream.ObjectStore.PutString` — `func(ctx context.Context, name string, data string) (*ObjectInfo, error)`
- Symbol: `jetstream.ObjectStore.PutFile` — `func(ctx context.Context, file string) (*ObjectInfo, error)`
- Pattern: multi-request; round trips: variable
- Wire messages: 1 meta lookup + ceil(size / 128KiB) pipelined async chunk publishes + 1 rollup meta publish (+ purge of the old version's chunks on overwrite)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** [object.go/Put](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) reads the source into \<=128KiB frames (objDefaultChunkSize), folds each into a rolling SHA-256, and pipelines the N chunks as async PublishMsgAsync publishes to $O.\<bucket>.C.\<nuid> that the leader ingests in [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go); a fresh nuid means a re-Put lands on a brand-new chunk subject.
- **2.** After EOF [object.go/Put](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) publishes one rollup meta message to $O.\<bucket>.M.\<name> carrying size, chunk count, and digest, tagged Nats-Rollup:sub so the leader in [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) purges any prior meta on that subject under the stream write lock while it stores the new one.
- **3a.** At R>1 [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) proposes every chunk plus the meta entry into the stream's Raft group, where each entry must reach quorum before it can be acknowledged.
- **3b.** Concurrently each replica appends every chunk and the meta message to its filestore via [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) and updates the per-subject index (fsync batched on SyncInterval, default 2m), so a 1GiB object costs ~8200 persisted writes per replica.
- **4.** The N+1 PubAcks stream back asynchronously as [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) persists each message; [object.go/Put](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) blocks on PublishAsyncComplete and, on an overwrite, then issues a stream Purge of the previous object's old chunk subject.

## In practice

- **Cost is size times replicas** — Each 128 KiB chunk is a full persisted, replicated publish, so a Put costs roughly the object size divided by 128 KiB, multiplied by the replica count — a 1 GiB object at three replicas is on the order of 25000 writes across the cluster. Size stream placement and replica count for the largest objects you expect, not the average.
- **Digest is client-side CPU** — The client folds the entire payload into a rolling SHA-256 as it publishes, so every Put spends CPU proportional to object size on the publishing process. For large objects on a busy client this hashing competes with your application work, independent of any server or network cost.
- **Overwrites cost extra** — Replacing an existing object writes the new version to a fresh chunk subject and then purges the previous version's chunks under the stream write lock, so an overwrite is strictly heavier than a first write and briefly serializes on that lock. Frequent re-Puts of the same key add purge load and can hold the write lock against concurrent writers.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.

## Evidence

- Client: [jetstream/object.go — (\*obs).Put](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), [server/filestore.go — (\*fileStore).StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
