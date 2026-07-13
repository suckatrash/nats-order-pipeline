# nats.NextMsg

Dequeue an already-delivered message from a sync subscription's local buffer.

- **Tier: minimal** — score 0 (invocation 0 + 2 × steady 0)
- Group: Core NATS
- Symbol: `nats.Subscription.NextMsg` — `func(timeout time.Duration) (*Msg, error)`
- Symbol: `nats.Subscription.NextMsgWithContext` — `func(ctx context.Context) (*Msg, error)`
- Symbol: `nats.Subscription.Msgs` — `func() iter.Seq2[*Msg, error]`
- Symbol: `nats.Subscription.MsgsTimeout` — `func(timeout time.Duration) iter.Seq2[*Msg, error]`
- Pattern: local-only; round trips: 0
- Disk I/O: none; Raft: none; server state: none; scan: none

## Flow

- **1.** Purely client-side: [nats.go/NextMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go) receives the already-delivered message from the subscription's pending channel (s.mch, filled earlier by the connection's reader goroutine) and processes the delivery in [nats.go/processNextMsgDelivered](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go), returning immediately if one is buffered or blocking up to the timeout — no bytes cross the wire.

## In practice

- **The cost was paid before NextMsg** — NextMsg is a local pop from a channel the server already filled, so the real expense was the earlier publish and delivery, not the dequeue. Budget message cost at the subscribe and publish that populate the pending queue, and treat NextMsg itself as effectively free.

## Evidence

- Client: [nats.go — (\*Subscription).NextMsg](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats.go), [nats\_iter.go — (\*Subscription).Msgs](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/nats_iter.go)
