package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	// Do not inspect a live queue or collide with Redis-backed integration tests
	// from other packages when Go runs package tests concurrently.
	options.DB = 13
	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Skip(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestMetricsHealthAndReadyHandlers(t *testing.T) {
	rdb := testRedis(t)
	metrics := NewMetrics()
	metrics.Enqueued()
	metrics.Completed()
	metrics.Retried()
	response := httptest.NewRecorder()
	MetricsHandler(rdb, metrics)(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	body := response.Body.String()
	for _, name := range []string{"taskforge_jobs_enqueued_total 1", "taskforge_jobs_completed_total 1", "taskforge_jobs_retried_total 1", "taskforge_queue_depth", "taskforge_processing_depth", "taskforge_retry_queue_depth", "taskforge_scheduled_queue_depth", "taskforge_dlq_depth", "taskforge_active_workers"} {
		if !strings.Contains(body, name) {
			t.Fatalf("metrics missing %q: %s", name, body)
		}
	}
	for _, handler := range []http.HandlerFunc{HealthHandler(rdb), ReadyHandler(rdb)} {
		response = httptest.NewRecorder()
		handler(response, httptest.NewRequest(http.MethodGet, "/", nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"redis":"ok"`) {
			t.Fatalf("health/readiness response = %d %s", response.Code, response.Body.String())
		}
	}
}

func TestMetricsAreConcurrencySafe(t *testing.T) {
	metrics := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); metrics.Enqueued(); metrics.WorkerStarted(); metrics.WorkerStopped() }()
	}
	wg.Wait()
	snapshot := metrics.Snapshot()
	if snapshot.Enqueued != 100 || snapshot.ActiveWorkers != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestUnavailableRedisHealthAndReady(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer rdb.Close()
	for _, handler := range []http.HandlerFunc{HealthHandler(rdb), ReadyHandler(rdb)} {
		response := httptest.NewRecorder()
		handler(response, httptest.NewRequest(http.MethodGet, "/", nil))
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("unavailable status = %d", response.Code)
		}
	}
}
