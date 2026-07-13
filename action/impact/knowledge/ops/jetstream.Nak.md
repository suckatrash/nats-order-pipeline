# jetstream.Nak

Fire-and-forget negative ack: schedule redelivery, optionally after a delay.

- **Tier: minimal** — score 0 (invocation 0 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Msg.Nak` — `func() error`
- Symbol: `jetstream.Msg.NakWithDelay` — `func(delay time.Duration) error`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1
- Disk I/O: none; Raft: none; server state: none; scan: none

## Flow

- **1.** The client's [message.go/Nak](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go) publishes a fire-and-forget -NAK to $JS.ACK.\<consumer> (optionally carrying a delay) and returns without awaiting a reply; the consumer leader in [consumer.go/processNak](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) confirms the delivery is still pending, emits a nak advisory to $JS.EVENT.ADVISORY via [consumer.go/sendAdvisory](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), and then either re-adds the sequence to the in-memory redelivery queue and signals a new delivery, or, when a delay is given, back-dates the pending timestamp by that delay to defer the retry.

## In practice

- **The redelivery is the real cost** — The NAK message itself is one cheap fire-and-forget signal that only mutates in-memory scheduling on the consumer leader. What it triggers is a full redelivery, and every retry repeats the entire delivery cost — so the expense lives in how often you NAK, not in the call.
- **Guard against NAK loops** — A handler that chronically NAKs the same messages multiplies steady-state consumer work as those sequences are delivered again and again. Use NakWithDelay to back off, and cap retries with a max-deliver or dead-letter path so a poison message cannot loop forever.

## Contention

- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.

## Evidence

- Client: [jetstream/message.go — (\*jetStreamMsg).Nak](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go)
- Server: [server/consumer.go — (\*consumer).processNak](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
