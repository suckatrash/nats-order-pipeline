# nats.Subscribe

Register subject interest (SUB), optionally in a queue group; messages are then pushed.

- **Tier: moderate** — score 5 (invocation 3 + 2 × steady 1)
- Group: Core NATS
- Symbol: `nats.Conn.Subscribe` — `func(subj string, cb MsgHandler) (*Subscription, error)`
- Symbol: `nats.Conn.SubscribeSync` — `func(subj string) (*Subscription, error)`
- Symbol: `nats.Conn.QueueSubscribe` — `func(subj, queue string, cb MsgHandler) (*Subscription, error)`
- Symbol: `nats.Conn.QueueSubscribeSync` — `func(subj, queue string) (*Subscription, error)`
- Symbol: `nats.Conn.QueueSubscribeSyncWithChan` — `func(subj, queue string, ch chan *Msg) (*Subscription, error)`
- Symbol: `nats.Conn.ChanSubscribe` — `func(subj string, ch chan *Msg) (*Subscription, error)`
- Symbol: `nats.Conn.ChanQueueSubscribe` — `func(subj, group string, ch chan *Msg) (*Subscription, error)`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1 (SUB)
- Disk I/O: none; Raft: none; server state: ephemeral; scan: none
- Choke points: asset-lock

## Steady state

- Client traffic: none (server pushes MSG frames)
- Server work per message: sublist match (cached); write to client outbound buffer; slow-consumer accounting
- Disk I/O: none; Raft: none

## Flow

- **1.** The client sends one SUB frame in [nats.go/subscribe](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) registering subject (and optional queue-group) interest; [client.go/processSub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) inserts it into the account sublist via [sublist.go/Insert](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go) under the write lock, bumping genid to invalidate every same-account client's L1 match cache (a wildcard insert additionally walks the ≤1024-entry result cache under that lock), after which ref-counted route/gateway propagation and unconditional per-leafnode updates fan the interest out.
- **2.** Steady state: no further client-to-server traffic flows — the server pushes every future matching message via a cached sublist match in [sublist.go/Match](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go), then [client.go/deliverMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) writes it into this client's outbound buffer and runs slow-consumer pending-byte accounting.

## In practice

- **Subscribe churn steals publish throughput** — Registration takes the account sublist write lock, so it blocks every same-account publish match while held and forces all same-account clients to repopulate their L1 caches under an exclusive lock. Frequent subscribe and unsubscribe therefore competes directly with the account's publish throughput — keep subscriptions long-lived rather than creating them per request.
- **Wildcard subjects cost more to register** — A wildcard subscription pays maintenance proportional to cache size: on insert it walks the entire result cache, up to 1024 entries, while holding the write lock that publishers are waiting on. Prefer specific subjects where you can, and be cautious about churning wildcard subscriptions.
- **Interest fanout scales with leafnodes** — Route and gateway propagation is ref-counted, so only the first and last subscriber for a subject send interest updates across those links, though queue-group weight changes always do. Leafnode updates are unconditional and proportional to the number of connected leafnodes, so in a wide leafnode topology every subscribe pays that per-leafnode fanout.
- **Slow consumers pin server memory** — Once registered, the subscription receives every future matching message, and the server buffers pending bytes per connection until the client drains them, so a slow consumer grows server-side memory and can be force-closed. Total subscription count per server also raises match and memory cost — size buffers for the busiest matching subject and prune idle subscriptions.

## Contention

- Serializes on **account sublist write lock + genid cache generation** (scope: account) with `nats.Unsubscribe`, `nats.Drain`, `nats.Close`, `nats.Publish`, `nats.Request`: Subscribe/unsubscribe/teardown hold the account's sublist write lock (wildcards walk the ≤1024-entry result cache under it; connection teardown is O(subscriptions)), blocking every same-account publish match. Each mutation bumps genid, invalidating all clients' L1 caches, so the next publish per client per subject becomes an exclusive-write-lock cache repopulation — churn converts cheap read-lock hits into a thundering herd of write locks.

## Evidence

- Client: [nats.go — (\*Conn).subscribe](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).processSub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/sublist.go — (\*Sublist).Insert](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go)
