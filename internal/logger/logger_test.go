package logger

import (
	"TaskForge/internal/task"
	"TaskForge/internal/tracing"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestJobLogIsJSONWithoutPayload(t *testing.T) {
	var output bytes.Buffer
	restore := SetOutput(&output)
	defer restore()
	Job("job_completed", task.Task{ID: "job-1", Type: "generate_pdf", Payload: map[string]interface{}{"secret": "must-not-log"}, Status: task.StatusCompleted}, nil)
	line := strings.TrimSpace(output.String())
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if event["event"] != "job_completed" || event["job_id"] != "job-1" || strings.Contains(line, "must-not-log") {
		t.Fatalf("unexpected event: %s", line)
	}
}

func TestJobLogIncludesTraceAndSpanIDs(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
	})
	var output bytes.Buffer
	restore := SetOutput(&output)
	defer restore()
	ctx, span := tracing.Start(context.Background(), "task.execute")
	JobContext(ctx, "job_completed", task.Task{ID: "job-1", Type: "generate_pdf", Status: task.StatusCompleted}, nil)
	span.End()
	var logged map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &logged); err != nil {
		t.Fatal(err)
	}
	if logged["trace_id"] == "" || logged["span_id"] == "" {
		t.Fatalf("trace IDs missing from log: %s", output.String())
	}
}
