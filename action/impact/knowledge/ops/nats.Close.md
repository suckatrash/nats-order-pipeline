# nats.Close

Terminate the connection; server tears down all its subscriptions and state.

- **Tier: low** — score 2 (invocation 2 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Conn.Close` — `func()`
- Pattern: fire-and-forget; round trips: 0
- Disk I/O: none; Raft: none; server state: none; scan: none
- Choke points: asset-lock

## Flow

- **1a.** The client flushes its outbound write buffer and closes the TCP connection in [nats.go/Close](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) without draining, so unlike Drain it does not wait for in-flight inbound messages to be dispatched; the server's teardown in [client.go/closeConnection](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) retracts the connection's interest through [client.go/clearAccountSubs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), which batch-removes its subscriptions from the account sublist under the write lock (RemoveBatch, O(subscriptions)) and resets the account match cache once.
- **1b.** Concurrently, when events are enabled (a system account is configured), the teardown in [server.go/saveClosedClient](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/server.go) spawns a goroutine, and [events.go/accountDisconnectEvent](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/events.go) publishes a $SYS.ACCOUNT.\<acct>.DISCONNECT billing event recording the closed session and its sent/received byte counts.

## In practice

- **Drain before closing to avoid data loss** — Close does not wait for in-flight inbound messages to be dispatched to your handlers, so anything still queued is dropped and buffered writes that were not flushed are discarded. When losing messages on shutdown is unacceptable, call Drain first and reserve Close for abrupt teardown.
- **Teardown cost grows with the connection** — Cleanup is proportional to how many subscriptions the connection accumulated, so a connection that fanned out into many subscriptions is markedly more expensive to close than a lean one. With a system account configured, each close also publishes a disconnect event that monitoring subscribers consume, so reconnect-heavy fleets pay both costs repeatedly.

## Contention

- Serializes on **account sublist write lock + genid cache generation** (scope: account) with `nats.Subscribe`, `nats.Unsubscribe`, `nats.Drain`, `nats.Publish`, `nats.Request`: Subscribe/unsubscribe/teardown hold the account's sublist write lock (wildcards walk the ≤1024-entry result cache under it; connection teardown is O(subscriptions)), blocking every same-account publish match. Each mutation bumps genid, invalidating all clients' L1 caches, so the next publish per client per subject becomes an exclusive-write-lock cache repopulation — churn converts cheap read-lock hits into a thundering herd of write locks.
- Serializes on **server-global mutex (s.mu) + $SYS event queue** (scope: server) with `nats.Connect`, `nats.Drain`: Every connect/disconnect takes the server-global mutex several times (client registry, event id) and enqueues $SYS events onto a single drain goroutine. Connection churn storms serialize server-wide and back up internal event delivery.

## Evidence

- Client: [nats.go — (\*Conn).Close](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).closeConnection](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/client.go — (\*client).clearAccountSubs](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go)
