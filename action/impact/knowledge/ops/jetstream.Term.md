# jetstream.Term

Fire-and-forget terminate: settle the message without redelivery, emitting an advisory.

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Msg.Term` — `func() error`
- Symbol: `jetstream.Msg.TermWithReason` — `func(reason string) error`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1
- Disk I/O: write; Raft: consumer-propose; server state: none; scan: none

## Flow

- **1.** The client fires the ack subject with a +TERM token ($JS.ACK.\<consumer>) in [message.go/Term](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go) and returns immediately; [consumer.go/processTerm](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) settles the message on the consumer leader like a positive ack to suppress redelivery, then [consumer.go/sendAdvisory](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) publishes a $JS.EVENT.ADVISORY...MSG\_TERMINATED advisory.
- **2.** On an R>1 consumer, [consumer.go/updateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) proposes an updateAcksOp entry through [consumer.go/propose](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) to the consumer's Raft group so followers replicate the settle and a quorum commits it.
- **3.** Each replica records the advanced ack floor to its consumer store via [filestore.go/UpdateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) (coalesced flush, ~10 writes/s), and on workqueue/interest streams [stream.go/ackMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) removes the now-uninterested message from the stream.

## In practice

- **Costs like an ack plus an advisory** — Term settles the message and advances consumer state exactly like a positive ack, then adds one MSG\_TERMINATED advisory publish. Budget it as an ack plus a small fan-out publish, not as a free discard.
- **Retention decides the extra work** — On workqueue or interest streams a terminated message is also removed from the stream, so the settle triggers stream-side deletion on top of the consumer-state update. On limits-based streams the message stays until normal retention reclaims it, making Term cheaper there.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.InProgress`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/message.go — (\*jetStreamMsg).Term](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go)
- Server: [server/consumer.go — (\*consumer).processTerm](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
