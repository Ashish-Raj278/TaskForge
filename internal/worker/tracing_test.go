package worker

import (
	"TaskForge/internal/task"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func workerSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func spanByName(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, span := range spans {
		if span.Name() == name {
			return span, true
		}
	}
	return nil, false
}

func TestWorkerTracingCapturesSuccessRetryAndDLQ(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	recorder := workerSpanRecorder(t)
	queue := fmt.Sprintf("task_trace_test_queue:%d", time.Now().UnixNano())
	jobs := []task.Task{
		integrationTask(t, "success"),
		integrationTask(t, "retry"),
		integrationTask(t, "dead"),
	}
	jobs[0].MaxRetries = 1
	jobs[1].MaxRetries = 2
	jobs[2].MaxRetries = 1
	t.Cleanup(func() { cleanupStressTasks(ctx, rdb, queue, jobs, nil) })

	pool := NewPool(rdb, queue, 1, time.Hour, time.Millisecond, log.New(io.Discard, "", 0))
	pool.execute = func(job task.Task) error {
		switch job.Type {
		case "success":
			return nil
		default:
			return errors.New("deterministic failure")
		}
	}

	for _, job := range jobs {
		if _, err := task.StoreAndEnqueue(ctx, rdb, queue, job); err != nil {
			t.Fatal(err)
		}
		raw, claimed, err := task.Claim(ctx, rdb, queue, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		pool.processTask(1, raw, claimed)
	}

	spans := recorder.Ended()
	ack, ok := spanByName(spans, "task.acknowledge")
	if !ok || ack.Status().Code != codes.Ok {
		t.Fatalf("successful acknowledgement span = %#v", ack)
	}
	execution, ok := spanByName(spans, "task.execute")
	if !ok || execution.Status().Code != codes.Ok {
		t.Fatalf("successful execution span = %#v", execution)
	}
	if _, ok := spanByName(spans, "task.retry.schedule"); !ok {
		t.Fatal("retry scheduling span missing")
	}
	if _, ok := spanByName(spans, "task.dlq.transition"); !ok {
		t.Fatal("DLQ transition span missing")
	}

	var errorExecutions int
	for _, span := range spans {
		if span.Name() == "task.execute" && span.Status().Code == codes.Error {
			errorExecutions++
		}
	}
	if errorExecutions != 2 {
		t.Fatalf("error execution spans = %d, want 2", errorExecutions)
	}
	retrying, err := task.GetMetadata(ctx, rdb, jobs[1].ID)
	if err != nil || retrying.Status != task.StatusRetrying || retrying.Attempts != 1 {
		t.Fatalf("retry semantics changed: %#v, %v", retrying, err)
	}
	dead, err := task.GetMetadata(ctx, rdb, jobs[2].ID)
	if err != nil || dead.Status != task.StatusDead || dead.Attempts != 1 {
		t.Fatalf("DLQ semantics changed: %#v, %v", dead, err)
	}
}
