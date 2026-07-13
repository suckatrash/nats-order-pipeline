# nats.Flush

Round-trip barrier: flush buffered writes and await a PONG.

- **Tier: minimal** — score 1 (invocation 1 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Conn.Flush` — `func() error`
- Symbol: `nats.Conn.FlushTimeout` — `func(timeout time.Duration) (err error)`
- Symbol: `nats.Conn.FlushWithContext` — `func(ctx context.Context) error`
- Symbol: `nats.Conn.RTT` — `func() (time.Duration, error)`
- Pattern: request-reply; round trips: 1
- Wire messages: PING → PONG
- Disk I/O: none; Raft: none; server state: none; scan: none

## Flow

- **1.** Flush in [nats.go/FlushTimeout](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) appends a PING to the outbound buffer through [nats.go/sendPing](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go), writes it to the socket, and registers a pong-wait entry before it blocks; the server's [client.go/processPing](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) parses the PING frame with no interest match or state change.
- **2.** The server immediately enqueues a PONG via [client.go/sendPong](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go); the client's [nats.go/processPong](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) signals the registered flush channel, unblocking the caller and yielding the round-trip time.

## In practice

- **The cost is the round trip, not the work** — A flush is one wakeup and two protocol frames, so what the caller actually pays is a full round-trip of latency while it blocks, not server work. Flush once after a batch of publishes rather than after every message, and keep it out of tight loops.
- **RTT reuses the flush round trip** — Conn.RTT is measured with the same PING/PONG exchange a flush performs, so sampling round-trip time costs no more than a flush and needs no separate probe.

## Evidence

- Client: [nats.go — (\*Conn).FlushTimeout](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/client.go — (\*client).processPing](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go)
