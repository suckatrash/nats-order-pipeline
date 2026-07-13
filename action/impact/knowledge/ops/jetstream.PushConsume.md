# jetstream.PushConsume

Push consumption: subscribe to the deliver subject; server streams messages with flow control.

- **Tier: high** — score 14 (invocation 2 + 2 × steady 6)
- Group: JetStream
- Symbol: `jetstream.PushConsumer.Consume` — `func(handler MessageHandler, opts ...PushConsumeOpt) (ConsumeContext, error)`
- Pattern: multi-request; round trips: 1
- Wire messages: SUB on the deliver subject (+ consumer info validation)
- Disk I/O: none; Raft: none; server state: ephemeral; scan: none

## Steady state

- Client traffic: flow-control replies and heartbeat liveness responses
- Server work per message: load message from store; pending/ack tracking (unless ack-none); push to deliver-subject interest
- Interval work: idle heartbeats; flow-control stanzas; ack-wait redelivery timers
- Disk I/O: read; Raft: consumer-propose

## Flow

- **1.** The client sends a $JS.API.CONSUMER.INFO.\<stream>.\<name> request in [consumer.go/fetchConsumerInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/consumer.go) to validate the pre-existing consumer and learn its DeliverSubject; [jetstream\_api.go/jsConsumerInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) answers it off the shared $JS.API queue on the consumer leader.
- **2.** The leader replies via [jetstream\_api.go/jsConsumerInfoRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) with the ConsumerInfo that [consumer.go/info](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) builds — config plus delivery state — and a push consumer is only valid if that config carries a non-empty DeliverSubject.
- **3.** The client issues a plain core-NATS SUB on the deliver subject in [push.go/Consume](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/push.go) (QueueSubscribe when a DeliverGroup is set), and [client.go/processSub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) records that ordinary interest in the account sublist — no further JetStream API call is involved.
- **4.** The consumer leader drives the pace: [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) loops the stream and reads each eligible message from the store, then [consumer.go/deliverMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) pushes it to the deliver subject, throttling with flow-control stanzas so outstanding bytes stay within the pending window.
- **5.** Steady state: [consumer.go/loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) keeps delivering each message with pending/ack tracking (unless ack-none) while [consumer.go/processFlowControl](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) exchanges flow-control replies and idle heartbeats for liveness, plus ack-wait redelivery timers — matching pull-consumer cost, including a raft proposal per delivery on replicated consumers.

## In practice

- **Server-paced, but not cheaper** — Push delivery has the server drive the pace under flow control, yet the per-message cost matches pull consumption: a store read, pending and ack tracking, and on a replicated consumer a Raft proposal per delivery. Choosing push over pull changes who initiates, not what each delivered message costs.
- **Idle consumers still cost** — Even with no messages flowing, a push consumer keeps exchanging idle heartbeats and flow-control stanzas and arming ack-wait redelivery timers. Many mostly-idle push consumers carry a standing liveness cost on the consumer leader, so consolidate or tear down consumers you are not actively draining.

## Contention

- Serializes on **fs.mu — per-stream store lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `jetstream.DeleteMsg`, `jetstream.PurgeStream`, `kv.PurgeDeletes`, `obj.Delete`, `jetstream.StreamInfo`, `jetstream.GetMsg`, `kv.Get`, `jetstream.Consume`, `jetstream.Fetch`, `obj.Get`: Store appends need the write lock; purges and secure erases hold it for data-proportional time; delivery's LoadNextMsg holds the read lock across cold 1–8 MB block faults (blocking appends); subject-filtered or deleted-details StreamInfo holds the read lock for data-proportional walks. Mixing large purges or cold reads with ingest stalls both.
- Serializes on **message-block cache (per-block mb.mu, ~10s idle expiry)** (scope: stream) with `jetstream.GetMsg`, `kv.Get`, `kv.History`, `obj.Get`, `jetstream.Consume`, `jetstream.Fetch`: Random point reads force-load and force-expire 1–8 MB blocks that sequential consumers need warm; interleaving them causes repeated cold faults plus per-block mutex serialization. Mixed random-read / sequential-consume workloads on one stream thrash the cache.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.InProgress`, `jetstream.Term`, `jetstream.Fetch`, `jetstream.Consume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/push.go — (\*pushConsumer).Consume](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/push.go)
- Server: [server/consumer.go — (\*consumer).loopAndGatherMsgs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), [server/consumer.go — (\*consumer).processFlowControl](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
