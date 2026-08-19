package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func priorityTestTask(t *testing.T, kind string, priority int) Task {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return Task{ID: id, Type: kind, Payload: map[string]interface{}{}, Status: StatusPending, Priority: priority, MaxRetries: 3, CreatedAt: time.Now().UTC()}
}

func cleanupPriorityQueue(ctx context.Context, rdb interface {
	Del(context.Context, ...string) *redis.IntCmd
	LRem(context.Context, string, int64, interface{}) *redis.IntCmd
}, queue string, tasks []Task, rawTasks []string) {
	keys := []string{queue, PrioritySequenceKey(queue)}
	for _, queuedTask := range tasks {
		keys = append(keys, MetadataKey(queuedTask.ID))
	}
	rdb.Del(ctx, keys...)
	for _, rawTask := range rawTasks {
		rdb.LRem(ctx, ProcessingQueue, 1, rawTask)
	}
}

func TestPriorityClaimOrdersByPriorityThenFIFO(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_priority_test_queue:" + seed
	queuedTasks := []Task{
		priorityTestTask(t, "a", 1),
		priorityTestTask(t, "b", 10),
		priorityTestTask(t, "c", 5),
		priorityTestTask(t, "d", 10),
		priorityTestTask(t, "e", 0),
	}
	rawTasks := make([]string, 0, len(queuedTasks))
	t.Cleanup(func() { cleanupPriorityQueue(ctx, rdb, queue, queuedTasks, rawTasks) })
	for _, queuedTask := range queuedTasks {
		if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
			t.Fatal(err)
		}
	}

	wantIDs := []string{queuedTasks[1].ID, queuedTasks[3].ID, queuedTasks[2].ID, queuedTasks[0].ID, queuedTasks[4].ID}
	for index, wantID := range wantIDs {
		rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		rawTasks = append(rawTasks, rawTask)
		if claimedTask.ID != wantID {
			t.Fatalf("claim %d = %s, want %s", index, claimedTask.ID, wantID)
		}
	}
}

func TestDefaultPriorityIsZero(t *testing.T) {
	if DefaultPriority != 0 {
		t.Fatalf("default priority = %d, want 0", DefaultPriority)
	}
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_priority_test_queue:" + seed
	queuedTask := priorityTestTask(t, "default", DefaultPriority)
	rawTasks := make([]string, 0, 1)
	t.Cleanup(func() { cleanupPriorityQueue(ctx, rdb, queue, []Task{queuedTask}, rawTasks) })
	if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.Priority != DefaultPriority {
		t.Fatalf("claimed default priority = %d, want %d", claimedTask.Priority, DefaultPriority)
	}
}

func TestPriorityClaimSupportsNegativePrioritiesAndConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_priority_test_queue:" + seed
	const taskCount = 12
	queuedTasks := make([]Task, 0, taskCount)
	for index := 0; index < taskCount; index++ {
		queuedTask := priorityTestTask(t, fmt.Sprintf("task-%d", index), -1)
		queuedTasks = append(queuedTasks, queuedTask)
		if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
			t.Fatal(err)
		}
	}
	rawTasks := make([]string, 0, taskCount)
	t.Cleanup(func() { cleanupPriorityQueue(ctx, rdb, queue, queuedTasks, rawTasks) })

	start := make(chan struct{})
	type claimed struct {
		raw string
		job Task
		err error
	}
	results := make(chan claimed, taskCount)
	var wg sync.WaitGroup
	for index := 0; index < taskCount; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
			results <- claimed{raw: rawTask, job: claimedTask, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	seen := make(map[string]struct{}, taskCount)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if _, duplicate := seen[result.job.ID]; duplicate {
			t.Fatalf("task %s was claimed more than once", result.job.ID)
		}
		seen[result.job.ID] = struct{}{}
		rawTasks = append(rawTasks, result.raw)
	}
	if len(seen) != taskCount {
		t.Fatalf("claimed %d jobs, want %d", len(seen), taskCount)
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != 0 {
		t.Fatalf("pending priority jobs = %d, want 0", got)
	}
}

func TestRetryReentersPriorityQueueAndYieldsToHigherPriorityJob(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_priority_test_queue:" + seed
	lowPriorityTask := priorityTestTask(t, "low", 1)
	highPriorityTask := priorityTestTask(t, "high", 10)
	rawTasks := make([]string, 0, 2)
	t.Cleanup(func() {
		cleanupPriorityQueue(ctx, rdb, queue, []Task{lowPriorityTask, highPriorityTask}, rawTasks)
		rdb.ZRem(ctx, RetryQueue, lowPriorityTask.ID)
	})
	if _, err := StoreAndEnqueue(ctx, rdb, queue, lowPriorityTask); err != nil {
		t.Fatal(err)
	}
	rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if _, err := StartProcessing(ctx, rdb, claimedTask.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := ScheduleRetry(ctx, rdb, rawTask, claimedTask.ID, time.Now().UTC().Add(-time.Millisecond), errors.New("temporary failure")); err != nil {
		t.Fatal(err)
	}
	if moved, err := MoveDueRetries(ctx, rdb, queue, time.Now().UTC()); err != nil || moved != 1 {
		t.Fatalf("move retry = %d, %v; want 1, nil", moved, err)
	}
	if _, err := StoreAndEnqueue(ctx, rdb, queue, highPriorityTask); err != nil {
		t.Fatal(err)
	}

	rawTask, claimedTask = "", Task{}
	rawTask, claimedTask, err = Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.ID != highPriorityTask.ID {
		t.Fatalf("first pending job = %s, want high-priority %s", claimedTask.ID, highPriorityTask.ID)
	}
	rawTask, claimedTask, err = Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.ID != lowPriorityTask.ID || claimedTask.Priority != lowPriorityTask.Priority {
		t.Fatalf("retried job = ID %s priority %d, want ID %s priority %d", claimedTask.ID, claimedTask.Priority, lowPriorityTask.ID, lowPriorityTask.Priority)
	}
}

func TestDLQReplayReturnsToPriorityQueueWithOriginalPriority(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_priority_test_queue:" + seed
	queuedTask := priorityTestTask(t, "dead", 7)
	rawTasks := make([]string, 0, 1)
	t.Cleanup(func() {
		cleanupPriorityQueue(ctx, rdb, queue, []Task{queuedTask}, rawTasks)
		rdb.LRem(ctx, DeadLetterQueue, 0, queuedTask.ID)
	})
	if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if _, err := StartProcessing(ctx, rdb, claimedTask.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := MoveToDLQ(ctx, rdb, rawTask, claimedTask.ID, errors.New("permanent failure")); err != nil {
		t.Fatal(err)
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != 0 {
		t.Fatalf("dead job remained pending: %d priority entries", got)
	}
	replayedTask, err := ReplayDeadTask(ctx, rdb, queue, queuedTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayedTask.Priority != queuedTask.Priority || replayedTask.Status != StatusPending || replayedTask.Attempts != 0 {
		t.Fatalf("replayed task = priority %d status %s attempts %d", replayedTask.Priority, replayedTask.Status, replayedTask.Attempts)
	}
	rawTask, claimedTask, err = Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.ID != queuedTask.ID || claimedTask.Priority != queuedTask.Priority {
		t.Fatalf("replayed claim = ID %s priority %d, want ID %s priority %d", claimedTask.ID, claimedTask.Priority, queuedTask.ID, queuedTask.Priority)
	}
}

func TestCrashRecoveryReentersPriorityQueueWithOriginalPriority(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_priority_test_queue:" + seed
	recoveredTask := priorityTestTask(t, "recovered", 2)
	highPriorityTask := priorityTestTask(t, "high", 9)
	rawTasks := make([]string, 0, 2)
	t.Cleanup(func() { cleanupPriorityQueue(ctx, rdb, queue, []Task{recoveredTask, highPriorityTask}, rawTasks) })
	if _, err := StoreAndEnqueue(ctx, rdb, queue, recoveredTask); err != nil {
		t.Fatal(err)
	}
	rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if _, err := StartProcessing(ctx, rdb, claimedTask.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := StoreAndEnqueue(ctx, rdb, queue, highPriorityTask); err != nil {
		t.Fatal(err)
	}
	if recovered, err := RecoverAbandoned(ctx, rdb, queue, time.Nanosecond); err != nil || recovered != 1 {
		t.Fatalf("recovered = %d, %v; want 1, nil", recovered, err)
	}
	rawTask, claimedTask, err = Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.ID != highPriorityTask.ID {
		t.Fatalf("first recovered-queue claim = %s, want high-priority %s", claimedTask.ID, highPriorityTask.ID)
	}
	rawTask, claimedTask, err = Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.ID != recoveredTask.ID || claimedTask.Priority != recoveredTask.Priority {
		t.Fatalf("recovered claim = ID %s priority %d, want ID %s priority %d", claimedTask.ID, claimedTask.Priority, recoveredTask.ID, recoveredTask.Priority)
	}
}
