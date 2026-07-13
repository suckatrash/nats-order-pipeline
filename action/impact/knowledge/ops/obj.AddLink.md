# obj.AddLink

Create a link to an object or bucket: a meta-only publish, no data copied.

- **Tier: high** — score 8 (invocation 8 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.AddLink` — `func(ctx context.Context, name string, obj *ObjectInfo) (*ObjectInfo, error)`
- Symbol: `jetstream.ObjectStore.AddBucketLink` — `func(ctx context.Context, name string, bucket ObjectStore) (*ObjectInfo, error)`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (meta publish + PubAck)
- Disk I/O: write; Raft: stream-propose; server state: persistent; scan: none

## Flow

- **1.** [object.go/AddLink](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) marshals the link's ObjectInfo to JSON and publishes it via [object.go/publishMeta](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) to $O.\<bucket>.M.\<name> (base64-encoded name) carrying the Nats-Rollup: sub header, then awaits a PubAck from the stream leader that ingests the record in [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go); no object bytes are copied, only the small meta record.
- **2a.** At R>1 the leader proposes the meta message to the stream's Raft group in [jetstream\_cluster.go/processClusteredInboundMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go); followers replicate the entry and a quorum must commit before it is applied.
- **2b.** Concurrently, on apply each replica appends the new link record to its store via [filestore.go/StoreMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) and then runs the subject-scoped Nats-Rollup: sub purge (keep=1) in [stream.go/processJetStreamMsgWithRollup](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), removing any earlier meta on $O.\<bucket>.M.\<name> so only this latest link record remains (fsync batched on SyncInterval, default 2m).
- **3.** Once the write is durable across the quorum the leader returns the PubAck with the assigned stream sequence in [stream.go/processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go), and AddLink hands back the ObjectInfo describing the link.

## In practice

- **Fixed cost, not data-proportional** — A link is one small persisted meta record written through the full replicated publish path, so its cost is fixed and independent of the linked object's size — it never grows with data volume.
- **Prefer links over copies** — To expose the same object under another name or bucket, add a link instead of re-uploading the bytes; you pay a single small persisted write rather than the whole chunked-upload path.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.

## Evidence

- Client: [jetstream/object.go — (\*obs).AddLink](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/stream.go — (\*stream).processJetStreamMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go)
