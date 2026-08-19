package observability

import (
	"TaskForge/internal/task"
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Metrics struct {
	enqueued  atomic.Uint64
	completed atomic.Uint64
	failed    atomic.Uint64
	retried   atomic.Uint64
	dead      atomic.Uint64
	recovered atomic.Uint64
	replayed  atomic.Uint64
	workers   atomic.Int64
}

type Snapshot struct {
	Enqueued, Completed, Failed, Retried, Dead, Recovered, Replayed uint64
	ActiveWorkers                                                   int64
}

func NewMetrics() *Metrics             { return &Metrics{} }
func (m *Metrics) Enqueued()           { m.enqueued.Add(1) }
func (m *Metrics) Completed()          { m.completed.Add(1) }
func (m *Metrics) Failed()             { m.failed.Add(1) }
func (m *Metrics) Retried()            { m.retried.Add(1) }
func (m *Metrics) Dead()               { m.dead.Add(1) }
func (m *Metrics) Recovered(count int) { m.recovered.Add(uint64(count)) }
func (m *Metrics) Replayed()           { m.replayed.Add(1) }
func (m *Metrics) WorkerStarted()      { m.workers.Add(1) }
func (m *Metrics) WorkerStopped()      { m.workers.Add(-1) }
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{m.enqueued.Load(), m.completed.Load(), m.failed.Load(), m.retried.Load(), m.dead.Load(), m.recovered.Load(), m.replayed.Load(), m.workers.Load()}
}

func MetricsHandler(rdb *redis.Client, metrics *Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET request allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		pipe := rdb.Pipeline()
		queue := pipe.ZCard(ctx, task.PriorityQueue)
		processing := pipe.LLen(ctx, task.ProcessingQueue)
		retry := pipe.ZCard(ctx, task.RetryQueue)
		scheduled := pipe.ZCard(ctx, task.ScheduleQueue)
		dlq := pipe.LLen(ctx, task.DeadLetterQueue)
		if _, err := pipe.Exec(ctx); err != nil {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		s := metrics.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintf(w, "# TYPE taskforge_jobs_enqueued_total counter\ntaskforge_jobs_enqueued_total %d\n", s.Enqueued)
		fmt.Fprintf(w, "# TYPE taskforge_jobs_completed_total counter\ntaskforge_jobs_completed_total %d\n", s.Completed)
		fmt.Fprintf(w, "# TYPE taskforge_jobs_failed_total counter\ntaskforge_jobs_failed_total %d\n", s.Failed)
		fmt.Fprintf(w, "# TYPE taskforge_jobs_retried_total counter\ntaskforge_jobs_retried_total %d\n", s.Retried)
		fmt.Fprintf(w, "# TYPE taskforge_jobs_dead_total counter\ntaskforge_jobs_dead_total %d\n", s.Dead)
		fmt.Fprintf(w, "# TYPE taskforge_jobs_recovered_total counter\ntaskforge_jobs_recovered_total %d\n", s.Recovered)
		fmt.Fprintf(w, "# TYPE taskforge_jobs_replayed_total counter\ntaskforge_jobs_replayed_total %d\n", s.Replayed)
		fmt.Fprintf(w, "# TYPE taskforge_queue_depth gauge\ntaskforge_queue_depth %d\n", queue.Val())
		fmt.Fprintf(w, "# TYPE taskforge_processing_depth gauge\ntaskforge_processing_depth %d\n", processing.Val())
		fmt.Fprintf(w, "# TYPE taskforge_retry_queue_depth gauge\ntaskforge_retry_queue_depth %d\n", retry.Val())
		fmt.Fprintf(w, "# TYPE taskforge_scheduled_queue_depth gauge\ntaskforge_scheduled_queue_depth %d\n", scheduled.Val())
		fmt.Fprintf(w, "# TYPE taskforge_dlq_depth gauge\ntaskforge_dlq_depth %d\n", dlq.Val())
		fmt.Fprintf(w, "# TYPE taskforge_active_workers gauge\ntaskforge_active_workers %d\n", s.ActiveWorkers)
	}
}

func HealthHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET request allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			writeStatus(w, http.StatusServiceUnavailable, "unhealthy", "unavailable")
			return
		}
		writeStatus(w, http.StatusOK, "ok", "ok")
	}
}

func ReadyHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET request allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			writeStatus(w, http.StatusServiceUnavailable, "not ready", "unavailable")
			return
		}
		writeStatus(w, http.StatusOK, "ready", "ok")
	}
}

func writeStatus(w http.ResponseWriter, statusCode int, status, redisStatus string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, `{"status":%q,"redis":%q}`+"\n", status, redisStatus)
}
