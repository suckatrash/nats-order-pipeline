// processor consumes ORDERS.created and publishes ORDERS.processed or
// ORDERS.rejected after simulated processing latency.
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
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	nc, err := natsutil.Connect("order-processor")
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
		Durable:       natsutil.ConsumerProcessor,
		FilterSubject: natsutil.SubjectOrderCreated,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 1000,
		AckWait:       30 * time.Second,
		Description:   "Processes incoming orders: validates and routes to processed/rejected",
	})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	var processed, rejected atomic.Int64
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var order natsutil.Order
		if err := json.Unmarshal(msg.Data(), &order); err != nil {
			slog.Warn("unmarshal failed", "error", err)
			msg.Nak()
			return
		}

		// Simulated processing latency (~50ms).
		time.Sleep(time.Duration(30+rand.IntN(40)) * time.Millisecond)

		// ~10% rejection rate to stress-test downstream error handling.
		if rand.IntN(100) < 10 {
			order.Status = "rejected"
			data, _ := json.Marshal(order)
			if err := nc.Publish(natsutil.SubjectOrderRejected, data); err != nil {
				slog.Warn("publish rejected failed", "error", err)
				msg.Nak()
				return
			}
			msg.Ack()
			n := rejected.Add(1)
			if n%100 == 0 {
				slog.Info("rejection progress", "rejected", n)
			}
			return
		}

		order.Status = "processed"
		data, _ := json.Marshal(order)
		if err := nc.Publish(natsutil.SubjectOrderProcessed, data); err != nil {
			slog.Warn("publish processed failed", "error", err)
			msg.Nak()
			return
		}
		msg.Ack()
		n := processed.Add(1)
		if n%1000 == 0 {
			slog.Info("processing progress", "processed", n)
		}
	})
	if err != nil {
		slog.Error("failed to start consuming", "error", err)
		os.Exit(1)
	}
	defer cc.Stop()

	slog.Info("processor running")
	<-ctx.Done()
	slog.Info("shutting down", "processed", processed.Load(), "rejected", rejected.Load())
}
