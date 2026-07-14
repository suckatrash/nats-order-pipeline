// analytics consumes all ORDERS.* subjects and periodically publishes
// aggregated summaries to the ANALYTICS stream.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/nats-order-pipeline/internal/natsutil"
)

type windowState struct {
	mu        sync.Mutex
	start     time.Time
	total     int
	processed int
	rejected  int
	revenue   float64
}

func (w *windowState) record(order natsutil.Order) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total++
	switch order.Status {
	case "processed":
		w.processed++
		w.revenue += order.Price * float64(order.Quantity)
	case "rejected":
		w.rejected++
	}
}

func (w *windowState) flush() natsutil.AnalyticsSummary {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	summary := natsutil.AnalyticsSummary{
		WindowStart:     w.start,
		WindowEnd:       now,
		TotalOrders:     w.total,
		ProcessedOrders: w.processed,
		RejectedOrders:  w.rejected,
		TotalRevenue:    w.revenue,
	}
	// Reset for next window.
	w.start = now
	w.total = 0
	w.processed = 0
	w.rejected = 0
	w.revenue = 0
	return summary
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	nc, err := natsutil.Connect("order-analytics")
	if err != nil {
		slog.Error("failed to connect", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("failed to create jetstream context", "error", err)
		os.Exit(1)
	}

	if _, err := natsutil.EnsureOrdersStream(ctx, js); err != nil {
		slog.Error("failed to ensure orders stream", "error", err)
		os.Exit(1)
	}
	if _, err := natsutil.EnsureAnalyticsStream(ctx, js); err != nil {
		slog.Error("failed to ensure analytics stream", "error", err)
		os.Exit(1)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, natsutil.OrdersStream, jetstream.ConsumerConfig{
		Durable:       natsutil.ConsumerAnalytics,
		FilterSubject: natsutil.SubjectOrdersAll,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxAckPending: 5000,
		Description:   "Aggregates order metrics for analytics summaries",
	})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	state := &windowState{start: time.Now()}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		// Enrichment messages use a different payload; skip them.
		if msg.Subject() == natsutil.SubjectOrderEnriched {
			msg.Ack()
			return
		}

		var order natsutil.Order
		if err := json.Unmarshal(msg.Data(), &order); err != nil {
			msg.Nak()
			return
		}
		state.record(order)
		msg.Ack()
	})
	if err != nil {
		slog.Error("failed to start consuming", "error", err)
		os.Exit(1)
	}
	defer cc.Stop()

	// Periodic summary publication.
	interval := 30 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("analytics running", "summary_interval", interval)
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		case <-ticker.C:
			summary := state.flush()
			if summary.TotalOrders == 0 {
				continue
			}
			data, _ := json.Marshal(summary)
			if _, err := js.Publish(ctx, natsutil.SubjectAnalyticsSummary, data); err != nil {
				slog.Warn("publish summary failed", "error", err)
				continue
			}
			slog.Info("published analytics summary",
				"total", summary.TotalOrders,
				"processed", summary.ProcessedOrders,
				"rejected", summary.RejectedOrders,
				"revenue", summary.TotalRevenue,
			)
		}
	}
}
