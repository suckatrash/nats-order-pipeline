// Package tracing wires OpenTelemetry tracing into the order-pipeline services
// and carries trace context across NATS messages.
//
// Trace context travels between services as W3C traceparent/tracestate entries
// stored in NATS message headers (which JetStream preserves), so a message
// published by one service continues the same distributed trace when another
// service consumes it.
//
// Export is enabled only when OTEL_EXPORTER_OTLP_ENDPOINT is set. Without it a
// no-op tracer provider is installed, so the services run exactly as before when
// no collector is present.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/synadia-io/nats-order-pipeline"

// Init installs the global W3C propagator and, when OTEL_EXPORTER_OTLP_ENDPOINT
// is set, an OTLP/HTTP tracer provider for serviceName. The returned function
// flushes and shuts the provider down and should be deferred by the caller.
//
// The OTLP exporter honours the standard OTEL_EXPORTER_OTLP_* environment
// variables (endpoint, headers, protocol).
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	// Always install the propagator so context flows across NATS even if this
	// particular service isn't exporting.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		slog.Info("otel tracing disabled (set OTEL_EXPORTER_OTLP_ENDPOINT to enable)")
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	slog.Info("otel tracing enabled",
		"endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "service", serviceName)
	return tp.Shutdown, nil
}

// Tracer returns the pipeline's tracer.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// natsHeaderCarrier adapts nats.Header to the propagation.TextMapCarrier interface.
type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string { return nats.Header(c).Get(key) }
func (c natsHeaderCarrier) Set(key, val string)   { nats.Header(c).Set(key, val) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// Inject writes the trace context from ctx into NATS message headers.
func Inject(ctx context.Context, h nats.Header) {
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier(h))
}

// Extract returns a context carrying the trace context found in NATS headers.
func Extract(ctx context.Context, h nats.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, natsHeaderCarrier(h))
}

// StartPublish starts a producer span for a publish to subject and returns the
// span-carrying context (to inject into the message) and the span. End the span
// after publishing; on error call RecordPublishError.
func StartPublish(ctx context.Context, subject string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, subject+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", subject),
			attribute.String("messaging.operation", "publish"),
		),
	)
}

// StartProcess starts a consumer span for a message received on subject, using
// the trace context extracted from h as the (remote) parent.
func StartProcess(ctx context.Context, subject string, h nats.Header) (context.Context, trace.Span) {
	ctx = Extract(ctx, h)
	return Tracer().Start(ctx, subject+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", subject),
			attribute.String("messaging.operation", "process"),
		),
	)
}

// RecordError marks span as failed and records err. Safe with a nil err (no-op).
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
