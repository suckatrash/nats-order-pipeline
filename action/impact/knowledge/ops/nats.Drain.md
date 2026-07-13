# nats.Drain

Gracefully drain the connection: unsubscribe everything, flush, wait for handlers, close.

- **Tier: moderate** — score 4 (invocation 4 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Conn.Drain` — `func() error`
- Pattern: multi-request; round trips: 2
- Wire messages: UNSUB per subscription + flush PING/PONGs
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: asset-lock

## Flow

- **1.** The client's background drain goroutine in [nats.go/drainConnection](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) puts every subscription into drain mode and streams one UNSUB \<sid> per subscription; the server's [client.go/processUnsub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) removes each sub from the account sublist via [sublist.go/Remove](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/sublist.go) under the write lock, so this beat's cost scales with the connection's subscription count (the asset-lock hold).
- **2.** Once each subscription's buffered backlog and handlers have drained locally, [nats.go/drainConnection](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) flips the connection to DRAINING\_PUBS and sends a PING via [nats.go/FlushTimeout](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) to push all still-buffered publishes to the server.
- **3.** The server's [client.go/processPing](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) replies PONG to confirm the flush completed; the client then closes the socket, which the server observes in [client.go/closeConnection](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) to release any remaining subscription accounting.
- **4.** On the final close in [client.go/closeConnection](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), when a system account is configured [events.go/accountDisconnectEvent](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/events.go) publishes a $SYS.ACCOUNT.\<id>.DISCONNECT advisory for the ended session.

## In practice

- **The wait is client-side, not server cost** — On the server, Drain is just the per-subscription unsubscribes plus a normal connection close; the time spent waiting for in-flight handlers to finish and buffered publishes to flush is borne entirely by the draining client. Size the drain timeout on the client and do not expect the server to bound it.
- **Drain time scales with subscriptions and backlog** — The client blocks until every subscription's pending backlog has been dispatched and every UNSUB acknowledged, so a connection with many subscriptions or a deep pending queue drains slowly. Prefer Drain over Close for a clean shutdown, but budget the timeout to the expected backlog rather than a fixed value.

## Contention

- Serializes on **account sublist write lock + genid cache generation** (scope: account) with `nats.Subscribe`, `nats.Unsubscribe`, `nats.Close`, `nats.Publish`, `nats.Request`: Subscribe/unsubscribe/teardown hold the account's sublist write lock (wildcards walk the ≤1024-entry result cache under it; connection teardown is O(subscriptions)), blocking every same-account publish match. Each mutation bumps genid, invalidating all clients' L1 caches, so the next publish per client per subject becomes an exclusive-write-lock cache repopulation — churn converts cheap read-lock hits into a thundering herd of write locks.
- Serializes on **server-global mutex (s.mu) + $SYS event queue** (scope: server) with `nats.Connect`, `nats.Close`: Every connect/disconnect takes the server-global mutex several times (client registry, event id) and enqueues $SYS events onto a single drain goroutine. Connection churn storms serialize server-wide and back up internal event delivery.

## Evidence

- Client: [nats.go — (\*Conn).Drain](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).processUnsub](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/client.go — (\*client).closeConnection](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go)
