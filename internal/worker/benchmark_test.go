package worker

import (
	"TaskForge/internal/task"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const benchmarkBatchSize = 48

func benchmarkRedis(b *testing.B) *redis.Client {
	b.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		b.Fatalf("parse Redis URL: %v", err)
	}
	// Keep Redis-backed benchmarks independent from the application and from
	// integration tests that may be running in other package test binaries.
	options.DB = 14
	rdb := redis.NewClient(options)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		rdb.Close()
		b.Skipf("Redis unavailable: %v", err)
	}
	b.Cleanup(func() { rdb.Close() })
	return rdb
}

func benchmarkTask(b *testing.B, taskType string) task.Task {
	b.Helper()
	id, err := task.NewID()
	if err != nil {
		b.Fatal(err)
	}
	return task.Task{
		ID:         id,
		Type:       taskType,
		Payload:    map[string]interface{}{},
		Status:     task.StatusPending,
		MaxRetries: 2,
		CreatedAt:  time.Now().UTC(),
	}
}

func benchmarkQueue(b *testing.B) string {
	b.Helper()
	id, err := task.NewID()
	if err != nil {
		b.Fatal(err)
	}
	return "task_benchmark_queue:" + id
}

func reportBatchMetrics(b *testing.B, jobs int, elapsed time.Duration) {
	b.Helper()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(jobs)/elapsed.Seconds(), "jobs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(jobs), "avg_job_latency_ns")
}

func waitForBenchmark(b *testing.B, deadline time.Duration, condition func() (bool, error)) {
	b.Helper()
	until := time.Now().Add(deadline)
	for {
		ok, err := condition()
		if err != nil {
			b.Fatal(err)
		}
		if ok {
			return
		}
		if time.Now().After(until) {
			b.Fatal("timed out waiting for benchmark workload")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// BenchmarkConcurrentEnqueue measures a 48-client concurrent enqueue batch.
func BenchmarkConcurrentEnqueue(b *testing.B) {
	ctx := context.Background()
	rdb := benchmarkRedis(b)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		queue := benchmarkQueue(b)
		jobs := make([]task.Task, benchmarkBatchSize)
		for index := range jobs {
			jobs[index] = benchmarkTask(b, "generate_pdf")
		}
		var wg sync.WaitGroup
		errs := make(chan error, benchmarkBatchSize)
		b.StartTimer()
		started := time.Now()
		for index := range jobs {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				if _, err := task.StoreAndEnqueue(ctx, rdb, queue, jobs[index]); err != nil {
					errs <- err
				}
			}(index)
		}
		wg.Wait()
		elapsed := time.Since(started)
		b.StopTimer()
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
		ids := make(map[string]struct{}, benchmarkBatchSize)
		for _, job := range jobs {
			if _, duplicate := ids[job.ID]; duplicate {
				b.Fatalf("duplicate job ID %s", job.ID)
			}
			ids[job.ID] = struct{}{}
		}
		if got := rdb.ZCard(ctx, queue).Val(); got != benchmarkBatchSize {
			b.Fatalf("enqueued jobs = %d, want %d", got, benchmarkBatchSize)
		}
		cleanupStressTasks(ctx, rdb, queue, jobs, nil)
		reportBatchMetrics(b, benchmarkBatchSize, elapsed)
	}
}

// BenchmarkConcurrentPriorityClaims measures atomic claims from a preloaded
// 48-job priority queue. Setup and cleanup are excluded from the timed region.
func BenchmarkConcurrentPriorityClaims(b *testing.B) {
	ctx := context.Background()
	rdb := benchmarkRedis(b)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		queue := benchmarkQueue(b)
		jobs := make([]task.Task, benchmarkBatchSize)
		for index := range jobs {
			jobs[index] = benchmarkTask(b, "claim")
			jobs[index].Priority = index % 6
			if _, err := task.StoreAndEnqueue(ctx, rdb, queue, jobs[index]); err != nil {
				b.Fatal(err)
			}
		}
		start := make(chan struct{})
		ids := make(chan string, benchmarkBatchSize)
		errs := make(chan error, benchmarkBatchSize)
		var wg sync.WaitGroup
		b.StartTimer()
		started := time.Now()
		for index := 0; index < benchmarkBatchSize; index++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, claimed, err := task.Claim(ctx, rdb, queue, time.Second)
				if err != nil {
					errs <- err
					return
				}
				ids <- claimed.ID
			}()
		}
		close(start)
		wg.Wait()
		elapsed := time.Since(started)
		b.StopTimer()
		close(ids)
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
		claimedIDs := make(map[string]struct{}, benchmarkBatchSize)
		for id := range ids {
			if _, duplicate := claimedIDs[id]; duplicate {
				b.Fatalf("duplicate priority claim for %s", id)
			}
			claimedIDs[id] = struct{}{}
		}
		if len(claimedIDs) != benchmarkBatchSize {
			b.Fatalf("claimed %d jobs, want %d", len(claimedIDs), benchmarkBatchSize)
		}
		if got := rdb.ZCard(ctx, queue).Val(); got != 0 {
			b.Fatalf("priority queue depth = %d, want 0", got)
		}
		rawTasks := rdb.LRange(ctx, task.ProcessingQueue, 0, -1).Val()
		cleanupStressTasks(ctx, rdb, queue, jobs, rawTasks)
		reportBatchMetrics(b, benchmarkBatchSize, elapsed)
	}
}

// BenchmarkWorkerScaling measures end-to-end worker processing for the same
// 48-job batch at one, four, and eight workers. Use -benchtime=1x for a
// concise fixed-workload comparison.
func BenchmarkWorkerScaling(b *testing.B) {
	for _, workerCount := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workerCount), func(b *testing.B) {
			ctx := context.Background()
			rdb := benchmarkRedis(b)
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				queue := benchmarkQueue(b)
				jobs := make([]task.Task, benchmarkBatchSize)
				for index := range jobs {
					jobs[index] = benchmarkTask(b, "generate_pdf")
					if _, err := task.StoreAndEnqueue(ctx, rdb, queue, jobs[index]); err != nil {
						b.Fatal(err)
					}
				}
				pool := NewPool(rdb, queue, workerCount, time.Hour, time.Second, log.New(io.Discard, "", 0))
				pool.execute = func(task.Task) error { return nil }
				poolCtx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				b.StartTimer()
				started := time.Now()
				go func() { pool.Run(poolCtx); close(done) }()
				waitForBenchmark(b, 10*time.Second, func() (bool, error) {
					for _, job := range jobs {
						metadata, err := task.GetMetadata(ctx, rdb, job.ID)
						if err != nil {
							return false, err
						}
						if metadata.Status != task.StatusCompleted {
							return false, nil
						}
					}
					return true, nil
				})
				elapsed := time.Since(started)
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					b.Fatal("worker pool did not stop")
				}
				b.StopTimer()
				if active := pool.Metrics().Snapshot().ActiveWorkers; active != 0 {
					b.Fatalf("active workers after benchmark shutdown = %d", active)
				}
				cleanupStressTasks(ctx, rdb, queue, jobs, nil)
				reportBatchMetrics(b, benchmarkBatchSize, elapsed)
			}
		})
	}
}

// BenchmarkRetryDLQWorkflow measures the retry scheduling, due promotion, and
// terminal DLQ transition for a fixed batch without waiting for scheduler ticks.
func BenchmarkRetryDLQWorkflow(b *testing.B) {
	ctx := context.Background()
	rdb := benchmarkRedis(b)
	const jobsPerBatch = 16
	for iteration := 0; iteration < b.N; iteration++ {
		queue := benchmarkQueue(b)
		jobs := make([]task.Task, jobsPerBatch)
		b.StartTimer()
		started := time.Now()
		for index := range jobs {
			job := benchmarkTask(b, "retry")
			jobs[index] = job
			if _, err := task.StoreAndEnqueue(ctx, rdb, queue, job); err != nil {
				b.Fatal(err)
			}
			raw, claimed, err := task.Claim(ctx, rdb, queue, time.Second)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := task.StartProcessing(ctx, rdb, claimed.ID); err != nil {
				b.Fatal(err)
			}
			if _, err := task.ScheduleRetry(ctx, rdb, raw, claimed.ID, time.Now().UTC(), errors.New("benchmark failure")); err != nil {
				b.Fatal(err)
			}
			if _, err := task.MoveDueRetries(ctx, rdb, queue, time.Now().UTC()); err != nil {
				b.Fatal(err)
			}
			raw, claimed, err = task.Claim(ctx, rdb, queue, time.Second)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := task.StartProcessing(ctx, rdb, claimed.ID); err != nil {
				b.Fatal(err)
			}
			if _, err := task.MoveToDLQ(ctx, rdb, raw, claimed.ID, errors.New("benchmark failure")); err != nil {
				b.Fatal(err)
			}
		}
		elapsed := time.Since(started)
		b.StopTimer()
		for _, job := range jobs {
			metadata, err := task.GetMetadata(ctx, rdb, job.ID)
			if err != nil || metadata.Status != task.StatusDead || metadata.Attempts != 2 {
				b.Fatalf("retry/DLQ result for %s = %#v, %v", job.ID, metadata, err)
			}
		}
		cleanupStressTasks(ctx, rdb, queue, jobs, nil)
		reportBatchMetrics(b, jobsPerBatch, elapsed)
	}
}

// BenchmarkScheduledPromotion measures Redis sorted-set promotion of a due
// 32-job schedule batch. Promotion does not execute jobs directly.
func BenchmarkScheduledPromotion(b *testing.B) {
	ctx := context.Background()
	rdb := benchmarkRedis(b)
	const jobsPerBatch = 32
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		queue := benchmarkQueue(b)
		jobs := make([]task.Task, jobsPerBatch)
		for index := range jobs {
			jobs[index] = benchmarkTask(b, "scheduled")
			jobs[index].Priority = index % 5
			due := time.Now().UTC().Add(-time.Millisecond)
			jobs[index].ScheduledAt = &due
			if err := task.StoreAndSchedule(ctx, rdb, jobs[index]); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		started := time.Now()
		moved, err := task.MoveDueScheduled(ctx, rdb, queue, time.Now().UTC())
		elapsed := time.Since(started)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if moved != jobsPerBatch {
			b.Fatalf("scheduled jobs moved = %d, want %d", moved, jobsPerBatch)
		}
		if got := rdb.ZCard(ctx, queue).Val(); got != jobsPerBatch {
			b.Fatalf("promoted priority jobs = %d, want %d", got, jobsPerBatch)
		}
		cleanupStressTasks(ctx, rdb, queue, jobs, nil)
		reportBatchMetrics(b, jobsPerBatch, elapsed)
	}
}
