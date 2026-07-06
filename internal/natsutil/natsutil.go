// Package natsutil provides shared NATS connection helpers and subject constants
// for the order pipeline services.
package natsutil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream names.
const (
	OrdersStream    = "ORDERS"
	AnalyticsStream = "ANALYTICS"
)

// Subject constants.
const (
	SubjectOrderCreated   = "ORDERS.created"
	SubjectOrderProcessed = "ORDERS.processed"
	SubjectOrderRejected  = "ORDERS.rejected"
	SubjectOrdersAll      = "ORDERS.>"
	SubjectAnalyticsSummary = "ANALYTICS.summary"
)

// Consumer names.
const (
	ConsumerProcessor = "order-processor"
	ConsumerNotifier  = "order-notifier"
	ConsumerAnalytics = "order-analytics"
)

// Connect establishes a NATS connection using standard environment variables.
// NATS_URL defaults to nats://nats:4222 (in-cluster).
// NATS_CREDS optionally points to a credentials file.
func Connect(name string) (*nats.Conn, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://nats:4222"
	}

	opts := []nats.Option{
		nats.Name(name),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("nats reconnected", "url", nc.ConnectedUrl())
		}),
	}

	if creds := os.Getenv("NATS_CREDS"); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", url, err)
	}

	slog.Info("connected to nats", "url", nc.ConnectedUrl(), "name", name)
	return nc, nil
}

// EnsureOrdersStream creates the ORDERS stream if it doesn't exist.
func EnsureOrdersStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        OrdersStream,
		Subjects:    []string{SubjectOrdersAll},
		Retention:   jetstream.LimitsPolicy,
		MaxBytes:    5 * 1024 * 1024 * 1024, // 5 GiB
		Replicas:    1,
		Description: "Order lifecycle events",
	})
}

// EnsureAnalyticsStream creates the ANALYTICS stream if it doesn't exist.
func EnsureAnalyticsStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        AnalyticsStream,
		Subjects:    []string{"ANALYTICS.>"},
		Retention:   jetstream.WorkQueuePolicy,
		MaxBytes:    1 * 1024 * 1024 * 1024, // 1 GiB
		Replicas:    1,
		Description: "Analytics aggregation output",
	})
}

// EnvInt reads an integer environment variable with a default.
func EnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}
