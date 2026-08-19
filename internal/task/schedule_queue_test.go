package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func scheduledTestTask(t *testing.T, priority int, scheduledAt time.Time) Task {
	t.Helper()
	queuedTask := priorityTestTask(t, "scheduled", priority)
	utcScheduledAt := scheduledAt.UTC()
	queuedTask.ScheduledAt = &utcScheduledAt
	return queuedTask
}

func cleanupScheduledTask(ctx context.Context, rdb *redis.Client, queue string, queuedTasks []Task, rawTasks []string) {
	cleanupPriorityQueue(ctx, rdb, queue, queuedTasks, rawTasks)
	for _, queuedTask := range queuedTasks {
		rdb.ZRem(ctx, ScheduleQueue, queuedTask.ID)
		rdb.ZRem(ctx, RetryQueue, queuedTask.ID)
		rdb.LRem(ctx, DeadLetterQueue, 0, queuedTask.ID)
	}
}

func TestScheduledTaskWaitsThenEntersPriorityQueue(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_schedule_test_queue:" + seed
	scheduledAt := time.Now().UTC().Add(time.Second)
	queuedTask := scheduledTestTask(t, 5, scheduledAt)
	rawTasks := make([]string, 0, 1)
	t.Cleanup(func() { cleanupScheduledTask(ctx, rdb, queue, []Task{queuedTask}, rawTasks) })
	if err := StoreAndSchedule(ctx, rdb, queuedTask); err != nil {
		t.Fatal(err)
	}
	if score, err := rdb.ZScore(ctx, ScheduleQueue, queuedTask.ID).Result(); err != nil || int64(score) != scheduledAt.UnixMilli() {
		t.Fatalf("schedule score = %v, %v; want %d", score, err, scheduledAt.UnixMilli())
	}
	if _, _, err := Claim(ctx, rdb, queue, time.Second); !errors.Is(err, redis.Nil) {
		t.Fatalf("future scheduled task was claimable: %v", err)
	}
	if moved, err := MoveDueScheduled(ctx, rdb, queue, scheduledAt.Add(-time.Millisecond)); err != nil || moved != 0 {
		t.Fatalf("early move = %d, %v; want 0, nil", moved, err)
	}
	if moved, err := MoveDueScheduled(ctx, rdb, queue, scheduledAt); err != nil || moved != 1 {
		t.Fatalf("due move = %d, %v; want 1, nil", moved, err)
	}
	if got := rdb.ZScore(ctx, ScheduleQueue, queuedTask.ID).Err(); !errors.Is(got, redis.Nil) {
		t.Fatalf("scheduled task remained scheduled: %v", got)
	}
	rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if claimedTask.ID != queuedTask.ID || claimedTask.Priority != queuedTask.Priority || claimedTask.ScheduledAt == nil || !claimedTask.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("claimed scheduled task did not preserve metadata: %#v", claimedTask)
	}
}

func TestDueScheduledTasksUsePriorityAndDoNotDuplicate(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_schedule_test_queue:" + seed
	past := time.Now().UTC().Add(-time.Second)
	queuedTasks := []Task{
		scheduledTestTask(t, 1, past),
		scheduledTestTask(t, 10, past),
		scheduledTestTask(t, 5, past),
		scheduledTestTask(t, 10, past),
	}
	rawTasks := make([]string, 0, len(queuedTasks))
	t.Cleanup(func() { cleanupScheduledTask(ctx, rdb, queue, queuedTasks, rawTasks) })
	for _, queuedTask := range queuedTasks {
		if err := StoreAndSchedule(ctx, rdb, queuedTask); err != nil {
			t.Fatal(err)
		}
	}
	if moved, err := MoveDueScheduled(ctx, rdb, queue, time.Now().UTC()); err != nil || moved != len(queuedTasks) {
		t.Fatalf("due move = %d, %v; want %d, nil", moved, err, len(queuedTasks))
	}
	if moved, err := MoveDueScheduled(ctx, rdb, queue, time.Now().UTC()); err != nil || moved != 0 {
		t.Fatalf("duplicate due move = %d, %v; want 0, nil", moved, err)
	}
	firstPriorityIDs := map[string]struct{}{queuedTasks[1].ID: {}, queuedTasks[3].ID: {}}
	for index := 0; index < len(queuedTasks); index++ {
		rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		rawTasks = append(rawTasks, rawTask)
		if index < 2 {
			if _, ok := firstPriorityIDs[claimedTask.ID]; !ok {
				t.Fatalf("scheduled priority claim %d = %s, want a priority-10 task", index, claimedTask.ID)
			}
			delete(firstPriorityIDs, claimedTask.ID)
			continue
		}
		if index == 2 && claimedTask.ID != queuedTasks[2].ID {
			t.Fatalf("scheduled priority-5 claim = %s, want %s", claimedTask.ID, queuedTasks[2].ID)
		}
		if index == 3 && claimedTask.ID != queuedTasks[0].ID {
			t.Fatalf("scheduled priority-1 claim = %s, want %s", claimedTask.ID, queuedTasks[0].ID)
		}
	}
}

func TestScheduledTaskRetainsScheduleDuringRetryAndRecovery(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	seed, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := "task_schedule_test_queue:" + seed
	scheduledAt := time.Now().UTC().Add(-time.Second)
	queuedTask := scheduledTestTask(t, 4, scheduledAt)
	rawTasks := make([]string, 0, 2)
	t.Cleanup(func() { cleanupScheduledTask(ctx, rdb, queue, []Task{queuedTask}, rawTasks) })
	if err := StoreAndSchedule(ctx, rdb, queuedTask); err != nil {
		t.Fatal(err)
	}
	if _, err := MoveDueScheduled(ctx, rdb, queue, time.Now().UTC()); err != nil {
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
	if _, err := ScheduleRetry(ctx, rdb, rawTask, claimedTask.ID, time.Now().UTC().Add(-time.Millisecond), fmt.Errorf("temporary failure")); err != nil {
		t.Fatal(err)
	}
	retryingTask, err := GetMetadata(ctx, rdb, queuedTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retryingTask.ScheduledAt == nil || !retryingTask.ScheduledAt.Equal(scheduledAt) {
		t.Fatal("retry changed original scheduled_at")
	}
	if _, err := MoveDueRetries(ctx, rdb, queue, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	rawTask, claimedTask, err = Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, rawTask)
	if _, err := StartProcessing(ctx, rdb, claimedTask.ID); err != nil {
		t.Fatal(err)
	}
	if recovered, err := RecoverAbandoned(ctx, rdb, queue, time.Nanosecond); err != nil || recovered != 1 {
		t.Fatalf("scheduled recovery = %d, %v; want 1, nil", recovered, err)
	}
	recoveredRawTask, recoveredTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawTasks = append(rawTasks, recoveredRawTask)
	if recoveredTask.ID != queuedTask.ID || recoveredTask.Priority != queuedTask.Priority || recoveredTask.ScheduledAt == nil || !recoveredTask.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("recovered scheduled task did not preserve metadata: %#v", recoveredTask)
	}
}
