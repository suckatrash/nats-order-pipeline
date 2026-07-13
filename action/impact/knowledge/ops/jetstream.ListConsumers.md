# jetstream.ListConsumers

Enumerate a stream's consumers (info or names), paged.

- **Tier: moderate** — score 5 (invocation 5 + 2 × steady 0)
- Group: JetStream
- Symbol: `jetstream.ConsumerManager.ListConsumers` — `func(context.Context) ConsumerInfoLister`
- Symbol: `jetstream.ConsumerManager.ConsumerNames` — `func(context.Context) ConsumerNameLister`
- Pattern: multi-request; round trips: variable
- Wire messages: 1 request per page
- Disk I/O: none; Raft: none; server state: none; scan: full
- Choke points: shared-api-pool

## Flow

- **1.** [stream.go/ListConsumers](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) publishes a request on $JS.API.CONSUMER.LIST.\<stream> (one request per page, carrying an advancing Offset) and awaits a page; the request is dispatched through the shared $JS.API worker pool to the stream leader's handler in [jetstream\_api.go/jsConsumerListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), which looks up the stream and gathers its public consumers in a full scan sorted by name via [stream.go/getPublicConsumers](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go).
- **2.** The leader in [jetstream\_api.go/jsConsumerListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go) returns one page of up to JSApiListLimit (256) ConsumerInfo records from the sorted set starting at Offset plus the Total count; the client's [stream.go/consumerInfos](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go) keeps issuing requests with an advancing Offset until Total consumers have been drained.

## In practice

- **Scales with consumer count** — Each call full-scans the stream's consumer set and pages it at 256 ConsumerInfos per request, so a stream with thousands of consumers costs several round trips and a scan proportional to that count. Budget for the consumer count, not a flat lookup.
- **Don't enumerate in a hot path** — Because the cost grows with consumer count and spans multiple requests, listing consumers on every operation is an anti-pattern. Cache the result, refresh on a slow cadence, and ask for names only when you don't need the full ConsumerInfo.

## Contention

- Serializes on **$JS.API worker pool + routed-request queue** (scope: server) with `jetstream.AccountInfo`, `jetstream.ConsumerInfo`, `jetstream.CreateConsumer`, `jetstream.CreateStream`, `jetstream.DeleteConsumer`, `jetstream.DeleteMsg`, `jetstream.DeleteStream`, `jetstream.GetMsg`, `jetstream.ListStreams`, `jetstream.OrderedConsumer`, `jetstream.PauseConsumer`, `jetstream.PurgeStream`, `jetstream.StreamInfo`, `jetstream.UnpinConsumer`, `jetstream.UpdateConsumer`, `jetstream.UpdateStream`, `kv.CreateBucket`, `kv.DeleteBucket`, `kv.GetBucket`, `kv.History`, `kv.Keys`, `kv.ListBuckets`, `kv.PurgeDeletes`, `kv.Status`, `kv.Watch`, `obj.CreateBucket`, `obj.Delete`, `obj.DeleteBucket`, `obj.Get`, `obj.GetBucket`, `obj.List`, `obj.ListBuckets`, `obj.Seal`, `obj.Status`, `obj.UpdateMeta`, `obj.Watch`: All non-direct JS API traffic shares one bounded worker pool (min(GOMAXPROCS,16)) with info requests explicitly deprioritized. Slow calls (subject-filtered StreamInfo, big lists) starve the rest, and past a 10k backlog the server drops every pending request and emits JSAdvisoryAPILimitReached. Watcher/ordered-consumer reset storms multiply entries. Direct gets are exempt by design.
- Serializes on **o.mu — per-consumer lock** (scope: consumer) with `jetstream.ConsumerInfo`, `jetstream.UnpinConsumer`, `jetstream.Fetch`, `jetstream.Consume`, `jetstream.PushConsume`, `jetstream.Ack`, `jetstream.DoubleAck`, `jetstream.Nak`, `jetstream.InProgress`, `jetstream.Term`: Delivery, pull-request handling, every ack variant, redelivery timer scans (O(pending)), info snapshots (O(pending) num-pending computation), and unpin all serialize on one per-consumer mutex. Frequent info polling of an actively delivering consumer directly stalls its delivery; ListConsumers touches every consumer's lock in turn.

## Evidence

- Client: [jetstream/stream.go — (\*stream).ListConsumers](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go)
- Server: [server/jetstream\_api.go — (\*Server).jsConsumerListRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go), [server/jetstream\_api.go — (\*Server).jsConsumerNamesRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
