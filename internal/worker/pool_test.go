package worker

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

func TestWorkerCount(t *testing.T) {
	tests := []struct {
		name, input string
		want        int
	}{
		{"missing", "", DefaultWorkerCount}, {"not a number", "three", DefaultWorkerCount},
		{"zero", "0", DefaultWorkerCount}, {"negative", "-1", DefaultWorkerCount}, {"valid", "5", 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := WorkerCount(test.input); got != test.want {
				t.Fatalf("WorkerCount(%q) = %d, want %d", test.input, got, test.want)
			}
		})
	}
}

func TestPoolRunReturnsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancel()
	pool := NewPool(nil, "task_queue", 2, DefaultVisibilityTimeout, DefaultRetryBaseDelay, log.New(io.Discard, "", 0))
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not stop after context cancellation")
	}
	if active := pool.Metrics().Snapshot().ActiveWorkers; active != 0 {
		t.Fatalf("active workers after shutdown = %d, want 0", active)
	}
}

func TestRetryBaseDelay(t *testing.T) {
	if got := RetryBaseDelay("5s"); got != 5*time.Second {
		t.Fatalf("RetryBaseDelay(5s) = %s, want 5s", got)
	}
	if got := RetryBaseDelay("invalid"); got != DefaultRetryBaseDelay {
		t.Fatalf("RetryBaseDelay(invalid) = %s, want %s", got, DefaultRetryBaseDelay)
	}
}

func TestVisibilityTimeout(t *testing.T) {
	if got := VisibilityTimeout("5s"); got != 5*time.Second {
		t.Fatalf("VisibilityTimeout(5s) = %s, want 5s", got)
	}
	if got := VisibilityTimeout("invalid"); got != DefaultVisibilityTimeout {
		t.Fatalf("VisibilityTimeout(invalid) = %s, want %s", got, DefaultVisibilityTimeout)
	}
}

func TestWaitForContextStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitForContext(ctx, time.Second) {
		t.Fatal("waitForContext returned true after cancellation")
	}
}
