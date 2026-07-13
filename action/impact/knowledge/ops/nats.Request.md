# nats.Request

Request-reply over core NATS using the connection's muxed inbox subscription.

- **Tier: minimal** — score 1 (invocation 1 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Conn.Request` — `func(subj string, data []byte, timeout time.Duration) (*Msg, error)`
- Symbol: `nats.Conn.RequestWithContext` — `func(ctx context.Context, subj string, data []byte) (*Msg, error)`
- Symbol: `nats.Conn.RequestMsg` — `func(msg *Msg, timeout time.Duration) (*Msg, error)`
- Symbol: `nats.Conn.RequestMsgWithContext` — `func(ctx context.Context, msg *Msg) (*Msg, error)`
- Pattern: request-reply; round trips: 1
- Wire messages: 2 (request + reply)
- Disk I/O: none; Raft: none; server state: none; scan: none

## Flow

- **1.** The client publishes the request in [nats.go/createNewRequestAndSend](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) to \<subject> carrying a unique \_INBOX.\<nuid>.\<token> reply subject; the muxed \_INBOX.\<nuid>.\* subscription is created once per connection on first request, and the server matches interest against the account sublist in [sublist.go/Match](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go) and delivers the message to the responder in [client.go/processInboundClientMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) with no per-request server state.
- **2.** The responder publishes to the \_INBOX reply subject; the server routes it back in [client.go/processInboundClientMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) through the client's single mux subscription as one MSG, which the client demuxes by token in [nats.go/respHandler](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) to the waiting request channel; in a supercluster, a reply crossing a gateway allocates a transient reply-mapping entry with a ~2s TTL, tracked in [gateway.go/trackGWReply](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/gateway.go).

## In practice

- **Reuse the muxed inbox** — The default muxed \_INBOX subscription is created once per connection and reused for every request. UseOldRequestStyle instead creates and removes a subscription on each request, taking the account sublist write lock both times and stealing throughput from same-account publishing. Prefer the default on any hot request path.
- **Scales like publish, doubled** — To the server a request is just two routed publishes — the request out and the reply back — so its fanout scales with matching subscriber interest exactly as publish does, paid twice. There is no per-request server state on a single server or cluster; responder service time is client-side latency, not server cost.
- **Cross-gateway replies take the account lock** — In a supercluster, service-import replies crossing a gateway take the account lock once per reply and allocate a short-lived reply-mapping entry with a roughly 2s TTL. A single server or cluster pays neither, so heavy cross-gateway request traffic is where request-reply stops being effectively free.

## Contention

- Serializes on **account sublist write lock + genid cache generation** (scope: account) with `nats.Subscribe`, `nats.Unsubscribe`, `nats.Drain`, `nats.Close`, `nats.Publish`: Subscribe/unsubscribe/teardown hold the account's sublist write lock (wildcards walk the ≤1024-entry result cache under it; connection teardown is O(subscriptions)), blocking every same-account publish match. Each mutation bumps genid, invalidating all clients' L1 caches, so the next publish per client per subject becomes an exclusive-write-lock cache repopulation — churn converts cheap read-lock hits into a thundering herd of write locks.

## Evidence

- Client: [nats.go — (\*Conn).Request](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go), [nats.go — (\*Conn).createNewRequestAndSend](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).processInboundClientMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/gateway.go — (\*Server).trackGWReply](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/gateway.go)
