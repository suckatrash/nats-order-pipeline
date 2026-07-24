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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/nats-order-pipeline/internal/natsutil"
	"github.com/synadia-io/nats-order-pipeline/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
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

// publishSummary publishes an analytics summary with a producer span, injecting
// trace context into the message headers.
func publishSummary(ctx context.Context, js jetstream.JetStream, summary natsutil.AnalyticsSummary) error {
	data, _ := json.Marshal(summary)

	ctx, span := tracing.StartPublish(ctx, natsutil.SubjectAnalyticsSummary)
	defer span.End()
	span.SetAttributes(attribute.Int("orders.total", summary.TotalOrders))

	msg := nats.NewMsg(natsutil.SubjectAnalyticsSummary)
	msg.Data = data
	tracing.Inject(ctx, msg.Header)

	_, err := js.PublishMsg(ctx, msg)
	tracing.RecordError(span, err)
	return err
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	shutdownTracing, err := tracing.Init(ctx, "order-analytics")
	if err != nil {
		slog.Error("failed to init tracing", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

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
		_, span := tracing.StartProcess(context.Background(), msg.Subject(), msg.Headers())
		defer span.End()

		var order natsutil.Order
		if err := json.Unmarshal(msg.Data(), &order); err != nil {
			tracing.RecordError(span, err)
			msg.Nak()
			return
		}
		span.SetAttributes(attribute.String("order.id", order.ID))
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
			if err := publishSummary(ctx, js, summary); err != nil {
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
