# nats.Unsubscribe

Remove subject interest (UNSUB), immediately or after N more messages.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Subscription.Unsubscribe` — `func() error`
- Symbol: `nats.Subscription.AutoUnsubscribe` — `func(max int) error`
- Symbol: `nats.Subscription.Drain` — `func() error`
- Pattern: fire-and-forget; round trips: 0
- Wire messages: 1 (UNSUB \[max])
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: asset-lock

## Flow

- **1.** The client sends UNSUB \<sid> \[max] in [nats.go/Unsubscribe](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) and returns without awaiting any reply; the server in [client.go/processUnsub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) removes the subscription from the account sublist via [sublist.go/Remove](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go) under the write lock — bumping genid and evicting matching entries from the L1 result cache (a wildcard removal scans the whole cache) — then retracts interest to routes, gateways, and leafnodes, or, when a max is given, records a delivery cap (sub.max) on the server-side subscription instead of removing it.

## In practice

- **Unsubscribe pays the write lock too** — Removal takes the same account sublist write lock and genid bump as subscribe — blocking same-account publish matches, forcing L1 cache repopulation, and propagating interest retraction to routes, gateways, and leafnodes. Subscribe-then-unsubscribe per request hits that exclusive lock twice, so keep subscriptions long-lived instead of tearing them down and recreating them.
- **Deferred teardown with AutoUnsubscribe and Drain** — AutoUnsubscribe records a max-delivery count on the server-side subscription rather than removing it, so the sublist teardown is deferred until the cap is reached. Drain instead retracts interest server-side first and then processes already-buffered messages locally, letting you shut a consumer down without dropping in-flight messages.

## Contention

- Serializes on **account sublist write lock + genid cache generation** (scope: account) with `nats.Subscribe`, `nats.Drain`, `nats.Close`, `nats.Publish`, `nats.Request`: Subscribe/unsubscribe/teardown hold the account's sublist write lock (wildcards walk the ≤1024-entry result cache under it; connection teardown is O(subscriptions)), blocking every same-account publish match. Each mutation bumps genid, invalidating all clients' L1 caches, so the next publish per client per subject becomes an exclusive-write-lock cache repopulation — churn converts cheap read-lock hits into a thundering herd of write locks.

## Evidence

- Client: [nats.go — (\*Subscription).Unsubscribe](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).processUnsub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/sublist.go — (\*Sublist).Remove](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go)
