package tracing

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
	})
	return recorder
}

func TestDisabledTracingUsesNoopProvider(t *testing.T) {
	shutdown, err := Init(context.Background(), Config{Enabled: false, Endpoint: "localhost:4317"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	_, span := Start(context.Background(), "task.disabled")
	defer span.End()
	if span.SpanContext().IsValid() {
		t.Fatal("disabled tracing created a valid span context")
	}
}

func TestEnabledTracingWithoutEndpointUsesNoopProvider(t *testing.T) {
	shutdown, err := Init(context.Background(), Config{Enabled: true, ServiceName: "taskforge-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	_, span := Start(context.Background(), "task.unconfigured")
	defer span.End()
	if span.SpanContext().IsValid() {
		t.Fatal("tracing without an endpoint created a valid span context")
	}
}

func TestTraceParentPropagatesAcrossAsyncJobBoundary(t *testing.T) {
	recorder := installRecorder(t)
	rootContext, rootSpan := Start(context.Background(), "task.enqueue")
	traceParent := TraceParent(rootContext)
	rootSpan.End()
	if traceParent == "" {
		t.Fatal("expected W3C traceparent")
	}

	workerContext := ContextWithTraceParent(context.Background(), traceParent)
	_, workerSpan := Start(workerContext, "task.execute")
	workerSpan.End()
	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	if spans[1].Parent().TraceID() != spans[0].SpanContext().TraceID() || spans[1].Parent().SpanID() != spans[0].SpanContext().SpanID() {
		t.Fatalf("worker span parent = %s/%s, want enqueue span %s/%s", spans[1].Parent().TraceID(), spans[1].Parent().SpanID(), spans[0].SpanContext().TraceID(), spans[0].SpanContext().SpanID())
	}
}

func TestExtractHTTPPreservesW3CContext(t *testing.T) {
	installRecorder(t)
	rootContext, rootSpan := Start(context.Background(), "client")
	parent := TraceParent(rootContext)
	rootSpan.End()
	req, err := http.NewRequest(http.MethodPost, "http://taskforge/enqueue", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("traceparent", parent)
	extracted := ExtractHTTP(context.Background(), req.Header)
	if got, want := trace.SpanContextFromContext(extracted).TraceID(), trace.SpanContextFromContext(rootContext).TraceID(); got != want {
		t.Fatalf("extracted trace ID = %s, want %s", got, want)
	}
}

func TestSetErrorMarksSpanError(t *testing.T) {
	recorder := installRecorder(t)
	_, span := Start(context.Background(), "task.execute")
	SetError(span, errors.New("handler failed"))
	span.End()
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Status().Code != codes.Error {
		t.Fatalf("span status = %#v, want error", spans)
	}
}

func TestJobAttributesExcludePayload(t *testing.T) {
	for _, attribute := range JobAttributes("job-1", "generate_pdf", 5, 1, "processing") {
		if attribute.Key == "payload" || attribute.Key == "request.body" {
			t.Fatalf("unsafe attribute key %q", attribute.Key)
		}
	}
}
