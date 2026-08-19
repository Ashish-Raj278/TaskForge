package tracing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "taskforge"

// Config controls optional OTLP tracing. Tracing is disabled unless both
// Enabled is true and Endpoint is set.
type Config struct {
	Enabled     bool
	ServiceName string
	Endpoint    string
	Insecure    bool
}

// ConfigFromEnv reads TaskForge tracing configuration. The default service name
// should identify the calling process, for example taskforge-producer.
func ConfigFromEnv(defaultServiceName string) Config {
	enabled, _ := strconv.ParseBool(os.Getenv("OTEL_TRACING_ENABLED"))
	insecure := true
	if value := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); value != "" {
		insecure, _ = strconv.ParseBool(value)
	}
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	return Config{
		Enabled:     enabled,
		ServiceName: serviceName,
		Endpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Insecure:    insecure,
	}
}

// Init configures the process-wide OpenTelemetry provider and returns a
// shutdown function. A disabled or incomplete configuration installs a safe
// no-op provider so TaskForge never requires a tracing backend to run.
func Init(ctx context.Context, config Config) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if !config.Enabled || config.Endpoint == "" {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(config.Endpoint)}
	if config.Insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, options...)
	if err != nil {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", config.ServiceName))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// Start begins a span using the TaskForge instrumentation scope.
func Start(ctx context.Context, name string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(instrumentationName).Start(ctx, name, options...)
}

// ExtractHTTP applies W3C trace-context headers supplied by an HTTP caller.
func ExtractHTTP(ctx context.Context, header http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(header))
}

// TraceParent serializes the current W3C context for server-owned job metadata.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextWithTraceParent restores the parent trace for an asynchronously
// processed job. Invalid or empty values leave the supplied context unchanged.
func ContextWithTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceParent})
}

// JobAttributes provides the non-sensitive attributes shared by job spans.
func JobAttributes(id, jobType string, priority, attempts int, status string) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.String("job.id", id),
		attribute.String("job.type", jobType),
		attribute.Int("job.priority", priority),
		attribute.Int("job.attempt", attempts),
	}
	if status != "" {
		attributes = append(attributes, attribute.String("job.status", status))
	}
	return attributes
}

// SetError consistently marks application and transition failures on a span.
func SetError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// SpanIDs returns log-safe IDs for the span currently carried by ctx.
func SpanIDs(ctx context.Context) (traceID, spanID string) {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return "", ""
	}
	return spanContext.TraceID().String(), spanContext.SpanID().String()
}
