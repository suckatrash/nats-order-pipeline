# jetstream.Fetch

One-shot pull request for up to N messages / bytes from a consumer.

- **Tier: high** — score 6 (invocation 6 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Consumer.Fetch` — `func(batch int, opts ...FetchOpt) (MessageBatch, error)`
- Symbol: `jetstream.Consumer.FetchBytes` — `func(maxBytes int, opts ...FetchOpt) (MessageBatch, error)`
- Symbol: `jetstream.Consumer.FetchNoWait` — `func(batch int) (MessageBatch, error)`
- Symbol: `jetstream.Consumer.Next` — `func(opts ...FetchOpt) (Msg, error)`
- Pattern: request-reply; round trips: 1
- Wire messages: 1 MSG.NEXT request + up to batch-size deliveries
- Disk I/O: read; Raft: consumer-propose; server state: ephemeral; scan: none

## Flow

- **1.** The client publishes a MSG.NEXT pull request (batch, max-bytes, expires) via [pull.go/Fetch](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/pull.go) to $JS.API.CONSUMER.MSG.NEXT on the consumer leader, where [consumer.go/processNextMsgReq](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) validates the limits and parks it in the waiting queue (pooled, capped by MaxWaiting -> 409 when full).
- **2.** In the delivery loop, [consumer.go/getNextMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) pulls the next matching sequence from the store via [filestore.go/LoadNextMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go); a cold read faults an entire 1-8MB block into cache under the store read lock, which can stall concurrent stream ingest.
- **3.** On a replicated consumer the leader proposes the delivered-state update (updateDeliveredOp) to the consumer's Raft group in [consumer.go/updateDelivered](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) and parks the message in pendingDeliveries via [consumer.go/addReplicatedQueuedMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) until that entry commits at quorum: delivered state must reach quorum before the batch is sent.
- **4.** Once delivery is durable across the quorum, the leader streams the batch message-by-message to the request's reply inbox via [consumer.go/deliverMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go); each stays ack-pending (capped by MaxAckPending) until acknowledged or ack-wait lapses.

## In practice

- **Scales with batch and message size** — Cost grows with batch size and per-message size, since every delivered message is loaded from the store and streamed individually. Larger batches amortize the request round trip but move more data at once and hold more state pending ack, so size the batch to what the consumer can process and ack promptly.
- **Cold reads fault whole blocks** — A delivery that misses the block cache faults an entire storage block (up to several MB) into memory under the store read lock, which stalls concurrent ingest on the same stream. Sequential consumers with warm caches read cheaply; random or lagging consumers that keep faulting cold blocks are the expensive case.
- **Ack-pending and quorum cap throughput** — Delivered messages stay ack-pending until acknowledged, capped by MaxAckPending; slow or missing acks stall further delivery. On a replicated consumer each delivery's state update is Raft-proposed and must reach quorum before the batch is sent, so replication latency is on the delivery path, not just the ack path.

## Contention

- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `jetstream.GetMsg`, `kv.Get`, `kv.History`, `obj.Get`, `jetstream.Consume`, `jetstream.PushConsume`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.InProgress`, `jetstream.Term`, `jetstream.Consume`, `jetstream.PushConsume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/pull.go — (\*pullConsumer).Fetch](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/pull.go)
- Server: [server/consumer.go — (\*consumer).processNextMsgReq](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), [server/filestore.go — (\*fileStore).LoadNextMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go)
