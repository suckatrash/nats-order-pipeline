# nats.Connect

Establish a client connection: TCP (+TLS), INFO/CONNECT exchange, authentication.

- **Tier: moderate** — score 5 (invocation 3 + 2 × steady 1)
- Group: Core NATS
- Symbol: `nats.Connect` — `func(url string, options ...Option) (*Conn, error)`
- Symbol: `nats.Options.Connect` — `func() (*Conn, error)`
- Symbol: `nats.Conn.ForceReconnect` — `func() error`
- Pattern: request-reply; round trips: 2
- Wire messages: INFO ← , CONNECT+PING → , PONG ← (plus TCP/TLS handshakes)
- Disk I/O: none; Raft: none; server state: ephemeral; scan: none

## Steady state

- Client traffic: PING every PingInterval (client default 2m); server enforces its own ping\_interval
- Interval work: keepalive PING/PONG both directions; connz/statsz accounting
- Disk I/O: none; Raft: none

## Flow

- **1.** The client dials in [nats.go/connect](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go); the server accepts the TCP (and, if configured, TLS) connection, allocates per-connection state in [server.go/createClient](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/server.go), and sends its INFO.
- **2.** The client answers in [nats.go/Connect](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) with CONNECT (credentials and options) plus a PING to flush the handshake.
- **3.** The server authenticates the client in [client.go/processConnect](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go) and replies PONG; the connection is now live.
- **4.** When a system account is configured, [events.go/accountConnectEvent](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/events.go) publishes a $SYS connect event for the new session.
- **5.** Steady state: keepalive PING/PONG in both directions via [nats.go/processPingTimer](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) (client default every 2m), plus connz/statsz accounting.

## In practice

- **Reuse connections, don't churn them** — Every connect and disconnect allocates and tears down server-side state — a client struct, a subscriptions map, outbound buffers, and two dedicated goroutines — and grabs the server-global mutex several times. Open one long-lived connection per process and multiplex subjects over it; connecting per operation is the classic hidden cost.
- **TLS and auth multiply the cost** — TLS adds handshake round trips and CPU, and authentication cost depends on the mechanism: JWT signature verification and bcrypt for hashed passwords are markedly heavier than nkey or token auth. Because both land on every (re)connect, they compound sharply under connection churn.
- **Connect fanout on $SYS** — When a system account is configured, each connect and disconnect publishes a $SYS.ACCOUNT.\*.CONNECT / DISCONNECT event. A fleet that reconnects frequently turns this into steady system-account traffic that every monitoring subscriber then has to consume.
- **ForceReconnect repeats the full handshake** — ForceReconnect tears down the session and repeats the entire TCP/TLS/auth handshake against another (or the same) server, paying the whole invocation cost again. Use it deliberately for rebalancing, not as a routine recovery step.

## Contention

- Serializes on **server-global mutex (s.mu) + $SYS event queue** (scope: server) with `nats.Close`, `nats.Drain`: Every connect/disconnect takes the server-global mutex several times (client registry, event id) and enqueues $SYS events onto a single drain goroutine. Connection churn storms serialize server-wide and back up internal event delivery.

## Evidence

- Client: [nats.go — Connect](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go), [nats.go — (\*Conn).connect](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go)
- Server: [server/server.go — (\*Server).createClient](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/server.go), [server/client.go — (\*client).processConnect](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/client.go), [server/events.go — (\*Server).accountConnectEvent](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/events.go)
