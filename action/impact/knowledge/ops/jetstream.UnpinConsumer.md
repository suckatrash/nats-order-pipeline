# jetstream.UnpinConsumer

Unpin the currently pinned client of a priority-group consumer.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.ConsumerManager.UnpinConsumer` — `func(ctx context.Context, consumer string, group string) error`
- Pattern: request-reply; round trips: 1
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: shared-api-pool

## Flow

- **1.** The client sends an UnpinConsumer request in [stream.go/UnpinConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) on the consumer's $JS.API.CONSUMER.UNPIN subject; a shared $JS.API pool worker on the consumer leader validates in [jetstream\_api.go/jsConsumerUnpinRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) that the stream and consumer exist, that it is the leader, and that the named priority group is valid.
- **2.** The leader briefly takes the consumer's delivery lock (o.mu) to clear the pinned client id in [consumer.go/unassignPinId](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go), emits a CONSUMER.UNPINNED admin advisory via [consumer.go/sendUnpinnedAdvisoryLocked](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/consumer.go) and signals waiting fetches, then returns the ack from [jetstream\_api.go/jsConsumerUnpinRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) — pure in-memory state, so the next fetch re-pins.

## In practice

- **Only a transient nudge** — The unpin lives entirely in the consumer leader's memory and lasts only until the next fetch re-pins a client, so it is a one-shot rebalancing nudge, not durable configuration. If you need a specific client pinned, control which fetch arrives next rather than repeating the unpin.
- **Keep unpins occasional** — Clearing the pin briefly serializes on the same consumer lock the fetch and delivery path uses, so a single admin call is free but scripting unpins in a loop against a busy priority-group consumer can momentarily stall delivery. Reserve it for deliberate rebalancing.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListConsumers`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.ListConsumers`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.

## Evidence

- Client: [jetstream/stream.go — (\*stream).UnpinConsumer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go)
- Server: [server/jetstream\_api.go — jsConsumerUnpinRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
