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
	pool := NewPool(nil, "task_queue", 2, log.New(io.Discard, "", 0))
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pool did not stop after context cancellation")
	}
}
