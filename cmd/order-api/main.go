// order-api publishes orders to the ORDERS stream and exposes an HTTP API
// for external order submission. It also runs a background traffic generator
// for testing purposes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/nats-order-pipeline/internal/natsutil"
)

var orderCounter atomic.Int64

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	nc, err := natsutil.Connect("order-api")
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

	// HTTP API for external order submission.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /order", func(w http.ResponseWriter, r *http.Request) {
		var order natsutil.Order
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, "invalid order JSON", http.StatusBadRequest)
			return
		}
		order.ID = fmt.Sprintf("ord-%d", orderCounter.Add(1))
		order.Status = "created"
		order.CreatedAt = time.Now()

		data, _ := json.Marshal(order)
		if _, err := js.Publish(ctx, natsutil.SubjectOrderCreated, data); err != nil {
			http.Error(w, "publish failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(order)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		slog.Info("http server listening", "addr", ":8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()

	// Background traffic generator.
	rate := natsutil.EnvInt("ORDER_RATE", 20)
	slog.Info("starting traffic generator", "orders_per_sec", rate)
	go generateTraffic(ctx, js, rate)

	<-ctx.Done()
	slog.Info("shutting down")
	srv.Shutdown(context.Background())
}

var products = []string{"widget-a", "widget-b", "gadget-x", "gadget-y", "gizmo-1"}
var customers = []string{"acme-corp", "globex", "initech", "umbrella", "wayne-ent", "stark-ind"}

func generateTraffic(ctx context.Context, js jetstream.JetStream, rate int) {
	if rate <= 0 {
		return
	}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	var published int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			order := natsutil.Order{
				ID:        fmt.Sprintf("ord-%d", orderCounter.Add(1)),
				Customer:  customers[rand.IntN(len(customers))],
				Product:   products[rand.IntN(len(products))],
				Quantity:  rand.IntN(10) + 1,
				Price:     float64(rand.IntN(9900)+100) / 100,
				Status:    "created",
				CreatedAt: time.Now(),
			}
			data, _ := json.Marshal(order)
			if _, err := js.Publish(ctx, natsutil.SubjectOrderCreated, data); err != nil {
				slog.Warn("publish failed", "error", err)
				continue
			}
			published++
			if published%1000 == 0 {
				slog.Info("traffic generator progress", "published", published)
			}
		}
	}
}
