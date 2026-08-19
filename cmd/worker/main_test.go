package main

import "testing"

func TestWorkerPortDefault(t *testing.T) {
	t.Setenv("PORT_WORKER", "")
	if got := workerPort(); got != "8081" {
		t.Fatalf("workerPort = %s, want 8081", got)
	}
}
