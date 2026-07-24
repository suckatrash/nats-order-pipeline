// notifier consumes ORDERS.processed and logs notifications. Occasionally
// simulates back-pressure by delaying acknowledgments.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/nats-order-pipeline/internal/natsutil"
	"github.com/synadia-io/nats-order-pipeline/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	shutdownTracing, err := tracing.Init(ctx, "order-notifier")
	if err != nil {
		slog.Error("failed to init tracing", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	nc, err := natsutil.Connect("order-notifier")
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

	cons, err := js.CreateOrUpdateConsumer(ctx, natsutil.OrdersStream, jetstream.ConsumerConfig{
		Durable:       natsutil.ConsumerNotifier,
		FilterSubject: natsutil.SubjectOrderProcessed,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 100,
		AckWait:       10 * time.Second,
		Description:   "Sends notifications for processed orders",
	})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	var notified atomic.Int64
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		_, span := tracing.StartProcess(context.Background(), msg.Subject(), msg.Headers())
		defer span.End()

		var order natsutil.Order
		if err := json.Unmarshal(msg.Data(), &order); err != nil {
			tracing.RecordError(span, err)
			slog.Warn("unmarshal failed", "error", err)
			msg.Nak()
			return
		}
		span.SetAttributes(attribute.String("order.id", order.ID))

		// Occasional back-pressure simulation (~2% of messages).
		if rand.IntN(100) < 2 {
			time.Sleep(time.Duration(500+rand.IntN(1500)) * time.Millisecond)
		}

		slog.Debug("notification sent", "order_id", order.ID, "customer", order.Customer)
		msg.Ack()

		n := notified.Add(1)
		if n%1000 == 0 {
			slog.Info("notification progress", "notified", n)
		}
	})
	if err != nil {
		slog.Error("failed to start consuming", "error", err)
		os.Exit(1)
	}
	defer cc.Stop()

	slog.Info("notifier running")
	<-ctx.Done()
	slog.Info("shutting down", "notified", notified.Load())
}
