# jetstream.DoubleAck

Synchronous acknowledgement: +ACK with a reply, awaited until the server confirms.

- **Tier: high** — score 6 (invocation 6 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.Msg.DoubleAck` — `func(context.Context) error`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (ack + confirmation)
- Disk I/O: write; Raft: consumer-propose; server state: none; scan: none

## Flow

- **1.** [message.go/DoubleAck](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go) sends the +ACK body to the message's $JS.ACK.\<consumer> reply subject as a synchronous Request and blocks on the confirmation inbox; the consumer leader parses the ack in [consumer.go/processAck](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), drops the sequence from its pending map, and advances the ack floor.
- **2.** At R>1 the consumer leader records the ack in [consumer.go/updateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) and proposes an updateAcksOp entry to the consumer's Raft group via [consumer.go/propose](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), stashing the confirmation reply by sequence in [consumer.go/addAckReply](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) until a quorum commits the ack-floor advance.
- **3.** Each replica applies the committed ack in [filestore.go/UpdateAcks](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go) and persists the updated consumer state (pending map and ack floor) to its consumer store index file via [filestore.go/writeState](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go); at R=1 the leader writes it directly.
- **4.** Once the ack is durable, [jetstream\_cluster.go/processReplicatedAck](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_cluster.go) releases the stashed reply and [consumer.go/sendAckReply](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) publishes the empty confirmation to the request's reply inbox, closing the at-least-once window and unblocking the caller's consume loop.

## In practice

- **Synchronous confirmation serializes the consume loop** — The caller blocks on the confirmation inbox for every message, so throughput is capped by the ack round-trip latency instead of overlapping with processing. Reserve DoubleAck for handoffs that genuinely must not be lost; use plain Ack, which returns immediately, for ordinary consumption.
- **Same server work as Ack** — DoubleAck does the same Raft-committed ack-floor advance as a normal Ack; the extra cost is the confirmation reply and the waiting, paid synchronously on every message rather than in the background. What you buy is the closed at-least-once window, not cheaper server work.

## Contention

- Serializes on **mset.mu — per-stream write lock** (scope: stream) with `jetstream.Publish`, `jetstream.PublishAsync`, `kv.Put`, `kv.Delete`, `kv.Purge`, `obj.Put`, `obj.AddLink`, `obj.UpdateMeta`, `jetstream.PurgeStream`, `jetstream.CreateConsumer`, `jetstream.DeleteConsumer`, `jetstream.OrderedConsumer`, `jetstream.Ack`, `jetstream.Term`, `jetstream.GetMsg`, `kv.Get`: One RWMutex serializes a stream's entire mutation pipeline: every append holds it for the store write; workqueue/interest acks take it exclusively ('only 1 at a time to gauge interest', with an O(#consumers) interest scan); purge holds it for the whole operation; consumer create/delete register under it; gets hold the read side, throttling the write lock. Draining a workqueue while publishing to it, or purging while ingesting, does not scale.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.
- Serializes on **consumer raft group (R>1)** (scope: consumer) with `jetstream.Ack`, `jetstream.InProgress`, `jetstream.Term`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`: On replicated consumers, every delivery, every ack/term/in-progress, and every pull request becomes a proposal on the same consumer raft group (batched via ProposeMulti ≤256 KB), and delivered state must reach quorum before send. Ack storms and delivery throughput share one consensus pipeline; DoubleAck adds synchronous rounds on top.

## Evidence

- Client: [jetstream/message.go — (\*jetStreamMsg).DoubleAck](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/message.go)
- Server: [server/consumer.go — (\*consumer).processAck](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go)
