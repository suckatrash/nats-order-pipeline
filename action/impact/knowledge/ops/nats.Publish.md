# nats.Publish

Fire-and-forget publish of one message to a subject (PUB/HPUB).

- **Tier: minimal** — score 0 (invocation 0 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Conn.Publish` — `func(subj string, data []byte) error`
- Symbol: `nats.Conn.PublishMsg` — `func(m *Msg) error`
- Symbol: `nats.Conn.PublishRequest` — `func(subj, reply string, data []byte) error`
- Symbol: `nats.Msg.Respond` — `func(data []byte) error`
- Symbol: `nats.Msg.RespondMsg` — `func(msg *Msg) error`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1
- Disk I/O: none; Raft: none; server state: none; scan: none

## Flow

- **1.** The client serializes PUB into its outbound buffer in [nats.go/publish](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) and returns immediately; the server's [client.go/processInboundClientMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) parses it, matches interest against the account sublist via [sublist.go/Match](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go), and fans the message out to subscribers, with no reply awaited.

## In practice

- **Scales with subscriber fanout and payload size** — The server does one parse and one interest lookup, then one write per matching subscriber, so a publish to a heavily-subscribed subject costs far more than a lightly-subscribed one. Message size adds to every one of those writes, so large payloads multiply with the fanout.
- **Subject stability keeps the cache warm** — Interest matches are served from the account sublist cache, but a cache miss walks the subject tree and briefly takes the sublist write lock to store the result. No-interest results are never cached, so publishing to constantly changing subjects, or to subjects nobody is listening on, re-walks the tree on every message.
- **Remote interest adds cross-cluster cost** — When matching interest exists on another server, the message is also propagated over routes, gateways, or leafnodes, so a publish that looks local can fan out across the cluster. Account for that whenever subscribers live in other clusters or on leaf nodes.
- **Fast producers can stall on backpressure** — If a destination client's outbound buffer fills past roughly 75 percent of its max-pending, the producer stalls until the slow consumer drains. A fast publisher feeding a slow subscriber pays this as latency rather than dropped messages, so watch for it on hot subjects.

## Contention

- Serializes on **account sublist write lock + genid cache generation** (scope: account) with `nats.Subscribe`, `nats.Unsubscribe`, `nats.Drain`, `nats.Close`, `nats.Request`: Subscribe/unsubscribe/teardown hold the account's sublist write lock (wildcards walk the ≤1024-entry result cache under it; connection teardown is O(subscriptions)), blocking every same-account publish match. Each mutation bumps genid, invalidating all clients' L1 caches, so the next publish per client per subject becomes an exclusive-write-lock cache repopulation — churn converts cheap read-lock hits into a thundering herd of write locks.

## Evidence

- Client: [nats.go — (\*Conn).publish](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).processInboundClientMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/client.go — (\*client).processMsgResults](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/sublist.go — (\*Sublist).Match](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go)
