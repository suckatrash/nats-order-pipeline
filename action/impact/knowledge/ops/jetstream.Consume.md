# jetstream.Consume

Continuous pull consumption: refilled batch requests, heartbeats, callback or iterator delivery.

- **Tier: high** — score 16 (invocation 4 + 2 × steady 6)
- Group: JetStream
- Symbol: `jetstream.Consumer.Consume` — `func(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error)`
- Symbol: `jetstream.Consumer.Messages` — `func(opts ...PullMessagesOpt) (MessagesContext, error)`
- Pattern: multi-request; round trips: 1
- Wire messages: initial MSG.NEXT batch request
- Disk I/O: none; Raft: consumer-propose; server state: ephemeral; scan: none

## Steady state

- Client traffic: MSG.NEXT refill when pending drops below half the batch (default 500/250); idle-heartbeat monitoring
- Server work per message: load message from store (sequential, cache-friendly); pending/ack-floor tracking; delivery to the waiting request
- Interval work: idle heartbeats; pull-request expiry sweeps; ack-wait redelivery timers
- Disk I/O: read; Raft: consumer-propose

## Flow

- **1.** The client sends an initial MSG.NEXT pull request for a batch of messages in [pull.go/Consume](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/pull.go), received by the consumer leader in [consumer.go/processNextMsgReq](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go).
- **2.** On a replicated consumer [consumer.go/processNextMsgReq](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) Raft-proposes each pull so a new leader knows the outstanding request, and delivered and ack-floor state is consensus-tracked.
- **3.** The consumer leader's delivery loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) sends matching messages to the client as they become available.
- **4.** Steady state dominates: the client auto-refills MSG.NEXT when pending drops below half the batch (default 500/250) in [pull.go/Messages](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/pull.go) and monitors idle heartbeats, while the leader's loop in [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) serves each message with a store read plus ack/pending bookkeeping, alongside redelivery timers.

## In practice

- **Steady-state cost dominates** — The expense is not the initial pull but the continuous loop: every delivered message is a store read plus pending and ack-floor bookkeeping, running for as long as the consumer is active. Size the batch and expiry so refills stay infrequent, and note that even an idle Consume keeps paying heartbeat and timer overhead.
- **Cold reads stall stream ingest** — When messages are not in cache, delivery reads from the filestore under its read lock, which momentarily blocks writes on the same stream. Consumers replaying deep history therefore steal throughput from publishers; keep hot consumers current and isolate large replays where you can.
- **Scale by consumers, not poll rate** — Each active consumer runs its own delivery loop, store reads, and timers on the leader, so aggregate cost tracks the number of live consumers far more than any single one's throughput. Prefer a few well-batched consumers over many thin ones sharing a stream.

## Contention

- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Fetch`, `jetstream.PushConsume`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `jetstream.GetMsg`, `kv.Get`, `kv.History`, `obj.Get`, `jetstream.Fetch`, `jetstream.PushConsume`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.InProgress`, `jetstream.Term`, `jetstream.Fetch`, `jetstream.PushConsume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/pull.go — (\*pullConsumer).Consume](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/pull.go), [jetstream/pull.go — (\*pullConsumer).Messages](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/pull.go)
- Server: [server/consumer.go — (\*consumer).processNextMsgReq](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
