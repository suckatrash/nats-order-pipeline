# jetstream.InProgress

Fire-and-forget working indicator: reset the message's ack-wait timer.

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Msg.InProgress` — `func() error`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1
- Disk I/O: write; Raft: consumer-propose; server state: none; scan: none

## Flow

- **1.** The client publishes a +WPI body via [message.go/InProgress](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go) to the message's ack inbox ($JS.ACK.\<consumer>) fire-and-forget; the consumer leader parses the ack in [consumer.go/progressUpdate](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) and, if that sequence is still pending, bumps its pending-record timestamp to reset the ack-wait/redelivery timer.
- **2.** Not just a timer poke: it rides the delivered-state path, so on a replicated (R>1) consumer [consumer.go/updateDelivered](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) proposes an updateDeliveredOp entry via [consumer.go/propose](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) to the consumer's Raft group, making every InProgress call its own consensus round that must reach quorum.
- **3.** The bumped delivered state is persisted to the consumer store via [filestore.go/UpdateDelivered](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) — a coalesced, kick-flushed write (~10/s max per consumer) applied directly at R1, or applied on each replica through [jetstream\_cluster.go/applyConsumerEntries](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) after the Raft entry commits at R>1.

## In practice

- **Replicas turn a poke into quorum** — On a single-replica consumer, InProgress is a cheap coalesced store flush capped near ten writes per second. On a replicated consumer each call rides the delivered-state Raft path and becomes a full consensus round, so raising the replica count turns a timer reset into quorum traffic.
- **Pace it against AckWait** — InProgress extends ack-wait during long-running handlers, but every call repeats the persisted delivered-state write. Send it on an interval sized to your AckWait rather than in a tight loop, or the keepalive cost eclipses the work it protects.

## Contention

- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Term`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/message.go — (\*jetStreamMsg).InProgress](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go)
- Server: [server/consumer.go — (\*consumer).progressUpdate](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
