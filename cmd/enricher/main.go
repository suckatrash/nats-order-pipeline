// enricher consumes ORDERS.processed and publishes ORDERS.enriched with
// customer context data (location, preferences, referral source, repeat buys).
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

var locations = []string{
	"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
	"Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
}

var allPreferences = []string{
	"express-shipping", "gift-wrap", "eco-packaging",
	"signature-required", "insured-delivery", "same-day",
}

var referralSources = []string{
	"organic", "social-media", "affiliate", "email-campaign",
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	nc, err := natsutil.Connect("order-enricher")
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
		Durable:       natsutil.ConsumerEnricher,
		FilterSubject: natsutil.SubjectOrderProcessed,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 500,
		AckWait:       15 * time.Second,
		Description:   "Enriches processed orders with customer context",
	})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	var enriched atomic.Int64
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var order natsutil.Order
		if err := json.Unmarshal(msg.Data(), &order); err != nil {
			slog.Warn("unmarshal failed", "error", err)
			msg.Nak()
			return
		}

		// Simulated enrichment lookup (~20-40ms).
		time.Sleep(time.Duration(20+rand.IntN(20)) * time.Millisecond)

		// Build random preference subset (1-3 items).
		numPrefs := 1 + rand.IntN(3)
		prefs := make([]string, 0, numPrefs)
		picked := make(map[int]bool)
		for len(prefs) < numPrefs {
			i := rand.IntN(len(allPreferences))
			if !picked[i] {
				picked[i] = true
				prefs = append(prefs, allPreferences[i])
			}
		}

		enrichment := natsutil.OrderEnrichment{
			OrderID:     order.ID,
			Customer:    order.Customer,
			Location:    locations[rand.IntN(len(locations))],
			Preferences: prefs,
			Referral:    referralSources[rand.IntN(len(referralSources))],
			RepeatBuys:  rand.IntN(26),
			EnrichedAt:  time.Now(),
		}

		data, _ := json.Marshal(enrichment)
		if err := nc.Publish(natsutil.SubjectOrderEnriched, data); err != nil {
			slog.Warn("publish enrichment failed", "error", err)
			msg.Nak()
			return
		}
		msg.Ack()

		n := enriched.Add(1)
		if n%1000 == 0 {
			slog.Info("enrichment progress", "enriched", n)
		}
	})
	if err != nil {
		slog.Error("failed to start consuming", "error", err)
		os.Exit(1)
	}
	defer cc.Stop()

	slog.Info("enricher running")
	<-ctx.Done()
	slog.Info("shutting down", "enriched", enriched.Load())
}
