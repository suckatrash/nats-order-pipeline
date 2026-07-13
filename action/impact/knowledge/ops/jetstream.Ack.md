# jetstream.Ack

Positive acknowledgement (fire-and-forget +ACK).

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Msg.Ack` — `func() error`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1
- Disk I/O: write; Raft: consumer-propose; server state: none; scan: none

## Flow

- **1.** The client publishes a bare +ACK to $JS.ACK.\<consumer> in [message.go/Ack](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go) and returns immediately with no PubAck awaited; the consumer leader deletes the message's pending entry and advances the ack floor in [consumer.go/processAck](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go).
- **2.** On a replicated consumer each ack becomes an updateAcksOp proposal in [consumer.go/updateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), batched via ProposeMulti in [consumer.go/loopAndForwardProposals](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) into the consumer's Raft group and committed by a quorum before the floor is applied; at R1 this consensus round is skipped entirely.
- **3.** The consumer store persists the new ack floor in [filestore.go/UpdateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) — at R1 UpdateAcks coalesces writes to ~10 flushes/s per consumer — and on workqueue/interest streams the ack additionally takes the stream write lock in [stream.go/ackMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) to delete the acked message from the stream store.

## In practice

- **Retention policy multiplies ack cost** — On workqueue and interest streams an ack does more than advance the floor: it takes the full stream write lock, scans cross-consumer interest, and deletes the message from the stream store, so acks compete with publishers for stream ingest. Limits-retention streams skip all of that, making their acks dramatically cheaper.
- **Replica count adds a consensus round** — Because a replicated consumer commits every ack through its Raft group, ack cost grows with replica count and is bounded by quorum latency rather than the wire. Where per-ack durability is not required, an R1 consumer skips consensus and coalesces store writes to roughly ten per second.
- **Out-of-order acks fragment the floor** — The ack floor advances contiguously, so acknowledging messages out of order leaves gaps the consumer must track individually until the lower sequences are acked. Acking in delivery order, or collapsing ranges with AckAll, keeps that pending set small.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.DoubleAck`, `jetstream.InProgress`, `jetstream.Term`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/message.go — (\*jetStreamMsg).Ack](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go)
- Server: [server/consumer.go — (\*consumer).processAck](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), [server/consumer.go — (\*consumer).updateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
