package worker

import (
	"TaskForge/internal/task"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	stressJobCount     = 48
	stressWorkerCount  = 6
	stressTestDeadline = 8 * time.Second
)

func stressQueue(t *testing.T) string {
	t.Helper()
	id, err := task.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return "task_stress_queue:" + id
}

func cleanupStressTasks(ctx context.Context, rdb *redis.Client, queue string, jobs []task.Task, rawTasks []string) {
	keys := []string{queue, task.PrioritySequenceKey(queue)}
	for _, job := range jobs {
		keys = append(keys, task.MetadataKey(job.ID))
		rdb.ZRem(ctx, task.RetryQueue, job.ID)
		rdb.ZRem(ctx, task.ScheduleQueue, job.ID)
		rdb.LRem(ctx, task.DeadLetterQueue, 0, job.ID)
	}
	rdb.Del(ctx, keys...)
	for _, rawTask := range rawTasks {
		rdb.LRem(ctx, task.ProcessingQueue, 0, rawTask)
	}
}

func waitForStress(t *testing.T, condition func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(stressTestDeadline)
	for {
		ok, err := condition()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for concurrent workload")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStressConcurrentEnqueueWorkersAndShutdown(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queue := stressQueue(t)
	jobs := make([]task.Task, stressJobCount)
	for index := range jobs {
		jobs[index] = integrationTask(t, "generate_pdf")
		jobs[index].MaxRetries = 1
	}
	var enqueueWG sync.WaitGroup
	errCh := make(chan error, stressJobCount)

	for index := range jobs {
		enqueueWG.Add(1)
		go func(index int) {
			defer enqueueWG.Done()
			if _, err := task.StoreAndEnqueue(ctx, rdb, queue, jobs[index]); err != nil {
				errCh <- err
			}
		}(index)
	}
	enqueueWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupStressTasks(ctx, rdb, queue, jobs, nil) })

	ids := make(map[string]struct{}, stressJobCount)
	for _, job := range jobs {
		if job.ID == "" {
			t.Fatal("concurrent enqueue produced an empty job ID")
		}
		if _, duplicate := ids[job.ID]; duplicate {
			t.Fatalf("duplicate generated job ID: %s", job.ID)
		}
		ids[job.ID] = struct{}{}
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != stressJobCount {
		t.Fatalf("pending jobs after concurrent enqueue = %d, want %d", got, stressJobCount)
	}

	pool := NewPool(rdb, queue, stressWorkerCount, time.Hour, time.Second, log.New(io.Discard, "", 0))
	var executionsMu sync.Mutex
	executions := make(map[string]int, stressJobCount)
	pool.execute = func(job task.Task) error {
		executionsMu.Lock()
		executions[job.ID]++
		executionsMu.Unlock()
		return nil
	}
	poolCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pool.Run(poolCtx); close(done) }()

	waitForStress(t, func() (bool, error) {
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
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool did not stop under load")
	}
	if active := pool.Metrics().Snapshot().ActiveWorkers; active != 0 {
		t.Fatalf("active workers after shutdown = %d, want 0", active)
	}
	executionsMu.Lock()
	defer executionsMu.Unlock()
	for _, job := range jobs {
		if got := executions[job.ID]; got != 1 {
			t.Fatalf("job %s executions = %d, want 1", job.ID, got)
		}
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != 0 {
		t.Fatalf("pending jobs after processing = %d, want 0", got)
	}
}

func TestStressAtomicClaimsAndConcurrentRecovery(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queue := stressQueue(t)
	jobs := make([]task.Task, stressJobCount)
	for index := range jobs {
		jobs[index] = integrationTask(t, fmt.Sprintf("claim-%d", index))
		jobs[index].Priority = index % 5
		if _, err := task.StoreAndEnqueue(ctx, rdb, queue, jobs[index]); err != nil {
			t.Fatal(err)
		}
	}
	rawTasks := make([]string, 0, stressJobCount)
	t.Cleanup(func() { cleanupStressTasks(ctx, rdb, queue, jobs, rawTasks) })

	type claimResult struct{ raw, id string }
	start := make(chan struct{})
	results := make(chan claimResult, stressJobCount)
	errCh := make(chan error, stressJobCount)
	var claimWG sync.WaitGroup
	for index := 0; index < stressJobCount; index++ {
		claimWG.Add(1)
		go func() {
			defer claimWG.Done()
			<-start
			raw, claimed, err := task.Claim(ctx, rdb, queue, time.Second)
			if err != nil {
				errCh <- err
				return
			}
			results <- claimResult{raw: raw, id: claimed.ID}
		}()
	}
	close(start)
	claimWG.Wait()
	close(results)
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	claimedIDs := make(map[string]struct{}, stressJobCount)
	for result := range results {
		if _, duplicate := claimedIDs[result.id]; duplicate {
			t.Fatalf("duplicate atomic claim for %s", result.id)
		}
		claimedIDs[result.id] = struct{}{}
		rawTasks = append(rawTasks, result.raw)
		if _, err := task.StartProcessing(ctx, rdb, result.id); err != nil {
			t.Fatal(err)
		}
	}
	if len(claimedIDs) != stressJobCount {
		t.Fatalf("claimed %d jobs, want %d", len(claimedIDs), stressJobCount)
	}
	pool := NewPool(rdb, queue, stressWorkerCount, time.Hour, time.Second, log.New(io.Discard, "", 0))
	var executionsMu sync.Mutex
	executions := make(map[string]int, stressJobCount)
	pool.execute = func(job task.Task) error {
		executionsMu.Lock()
		executions[job.ID]++
		executionsMu.Unlock()
		return nil
	}
	poolCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pool.Run(poolCtx); close(done) }()
	waitForStress(t, func() (bool, error) {
		return pool.Metrics().Snapshot().ActiveWorkers == stressWorkerCount, nil
	})

	var recoveryWG sync.WaitGroup
	recoveryErrs := make(chan error, stressWorkerCount)
	for index := 0; index < stressWorkerCount; index++ {
		recoveryWG.Add(1)
		go func() {
			defer recoveryWG.Done()
			if _, err := task.RecoverAbandoned(ctx, rdb, queue, time.Nanosecond); err != nil {
				recoveryErrs <- err
			}
		}()
	}
	recoveryWG.Wait()
	close(recoveryErrs)
	for err := range recoveryErrs {
		t.Fatal(err)
	}
	waitForStress(t, func() (bool, error) {
		for _, job := range jobs {
			metadata, err := task.GetMetadata(ctx, rdb, job.ID)
			if err != nil {
				return false, err
			}
			if metadata.Status != task.StatusCompleted || metadata.Attempts != 2 {
				return false, nil
			}
		}
		return true, nil
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery worker pool did not stop")
	}
	executionsMu.Lock()
	defer executionsMu.Unlock()
	for _, job := range jobs {
		if got := executions[job.ID]; got != 1 {
			t.Fatalf("recovered job %s executions = %d, want 1", job.ID, got)
		}
	}
}

func TestStressRetriesDLQAndConcurrentReplay(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queue := stressQueue(t)
	const retryJobs = 18
	jobs := make([]task.Task, retryJobs)
	for index := range jobs {
		jobs[index] = integrationTask(t, "always-fail")
		jobs[index].MaxRetries = 2
		if _, err := task.StoreAndEnqueue(ctx, rdb, queue, jobs[index]); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { cleanupStressTasks(ctx, rdb, queue, jobs, nil) })

	pool := NewPool(rdb, queue, stressWorkerCount, time.Hour, time.Millisecond, log.New(io.Discard, "", 0))
	var executionsMu sync.Mutex
	executions := make(map[string]int, retryJobs)
	pool.execute = func(job task.Task) error {
		executionsMu.Lock()
		executions[job.ID]++
		executionsMu.Unlock()
		return errors.New("deterministic failure")
	}
	poolCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pool.Run(poolCtx); close(done) }()
	waitForStress(t, func() (bool, error) {
		for _, job := range jobs {
			metadata, err := task.GetMetadata(ctx, rdb, job.ID)
			if err != nil {
				return false, err
			}
			if metadata.Status != task.StatusDead || metadata.Attempts != 2 {
				return false, nil
			}
			if err := rdb.ZScore(ctx, task.RetryQueue, job.ID).Err(); !errors.Is(err, redis.Nil) {
				return false, fmt.Errorf("job %s retained retry member: %w", job.ID, err)
			}
		}
		return true, nil
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retry worker pool did not stop")
	}
	executionsMu.Lock()
	for _, job := range jobs {
		if got := executions[job.ID]; got != 2 {
			executionsMu.Unlock()
			t.Fatalf("job %s executions = %d, want 2", job.ID, got)
		}
	}
	executionsMu.Unlock()

	job := jobs[0]
	start := make(chan struct{})
	results := make(chan error, stressWorkerCount)
	var replayWG sync.WaitGroup
	for index := 0; index < stressWorkerCount; index++ {
		replayWG.Add(1)
		go func() {
			defer replayWG.Done()
			<-start
			_, err := task.ReplayDeadTask(ctx, rdb, queue, job.ID)
			results <- err
		}()
	}
	close(start)
	replayWG.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful concurrent replays = %d, want 1", successes)
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != 1 {
		t.Fatalf("replayed priority entries = %d, want 1", got)
	}
}

func TestStressScheduledPromotion(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queue := stressQueue(t)
	const scheduledJobs = 24
	jobs := make([]task.Task, scheduledJobs)
	dueAt := time.Now().UTC().Add(50 * time.Millisecond)
	for index := range jobs {
		jobs[index] = integrationTask(t, "scheduled")
		jobs[index].Priority = index % 4
		scheduled := dueAt
		jobs[index].ScheduledAt = &scheduled
	}
	t.Cleanup(func() { cleanupStressTasks(ctx, rdb, queue, jobs, nil) })
	var scheduleWG sync.WaitGroup
	errCh := make(chan error, scheduledJobs)
	for index := range jobs {
		scheduleWG.Add(1)
		go func(index int) {
			defer scheduleWG.Done()
			if err := task.StoreAndSchedule(ctx, rdb, jobs[index]); err != nil {
				errCh <- err
			}
		}(index)
	}
	scheduleWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	pool := NewPool(rdb, queue, stressWorkerCount, time.Hour, time.Second, log.New(io.Discard, "", 0))
	var executionsMu sync.Mutex
	executions := make(map[string]int, scheduledJobs)
	pool.execute = func(job task.Task) error {
		executionsMu.Lock()
		executions[job.ID]++
		executionsMu.Unlock()
		return nil
	}
	poolCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pool.Run(poolCtx); close(done) }()
	waitForStress(t, func() (bool, error) {
		for _, job := range jobs {
			metadata, err := task.GetMetadata(ctx, rdb, job.ID)
			if err != nil {
				return false, err
			}
			if metadata.Status != task.StatusCompleted {
				return false, nil
			}
			if err := rdb.ZScore(ctx, task.ScheduleQueue, job.ID).Err(); !errors.Is(err, redis.Nil) {
				return false, fmt.Errorf("job %s retained schedule member: %w", job.ID, err)
			}
		}
		return true, nil
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduled worker pool did not stop")
	}
	executionsMu.Lock()
	defer executionsMu.Unlock()
	for _, job := range jobs {
		if got := executions[job.ID]; got != 1 {
			t.Fatalf("scheduled job %s executions = %d, want 1", job.ID, got)
		}
	}
}
