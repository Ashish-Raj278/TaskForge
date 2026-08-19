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

	"github.com/redis/go-redis/v9"
)

func TestSuccessfulRetryReachesCompleted(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queuedTask := integrationTask(t, "generate_pdf")
	queuedTask.MaxRetries = 2
	queue := fmt.Sprintf("task_test_queue:%s", queuedTask.ID)
	t.Cleanup(func() {
		rdb.Del(ctx, queue, task.MetadataKey(queuedTask.ID))
		rdb.ZRem(ctx, task.RetryQueue, queuedTask.ID)
	})
	if _, err := task.StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}

	pool := NewPool(rdb, queue, 1, time.Hour, time.Millisecond, log.New(io.Discard, "", 0))
	executions := 0
	pool.execute = func(task.Task) error {
		executions++
		if executions == 1 {
			return errors.New("temporary failure")
		}
		return nil
	}

	rawTask, claimedTask, err := task.Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool.processTask(1, rawTask, claimedTask)
	retryingTask, err := task.GetMetadata(ctx, rdb, queuedTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retryingTask.Status != task.StatusRetrying || retryingTask.Attempts != 1 {
		t.Fatalf("after first failure: status %s attempts %d", retryingTask.Status, retryingTask.Attempts)
	}

	if moved, err := task.MoveDueRetries(ctx, rdb, queue, time.Now().UTC().Add(time.Second)); err != nil || moved != 1 {
		t.Fatalf("move retry = %d, %v; want 1, nil", moved, err)
	}
	rawTask, claimedTask, err = task.Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool.processTask(1, rawTask, claimedTask)

	completedTask, err := task.GetMetadata(ctx, rdb, queuedTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completedTask.Status != task.StatusCompleted || completedTask.Attempts != 2 {
		t.Fatalf("completed task = status %s attempts %d", completedTask.Status, completedTask.Attempts)
	}
	if containsTask(ctx, rdb, task.ProcessingQueue, rawTask) {
		t.Fatal("completed retry remained in processing")
	}
}

func TestExhaustedRetriesReachDLQWithoutRescheduling(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queuedTask := integrationTask(t, "unsupported")
	queuedTask.MaxRetries = 2
	queue := fmt.Sprintf("task_test_queue:%s", queuedTask.ID)
	t.Cleanup(func() {
		rdb.Del(ctx, queue, task.MetadataKey(queuedTask.ID))
		rdb.ZRem(ctx, task.RetryQueue, queuedTask.ID)
		rdb.LRem(ctx, task.DeadLetterQueue, 0, queuedTask.ID)
	})
	if _, err := task.StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	pool := NewPool(rdb, queue, 1, time.Hour, time.Millisecond, log.New(io.Discard, "", 0))
	pool.execute = func(task.Task) error { return errors.New("always fails") }

	for attempt := 1; attempt <= 2; attempt++ {
		rawTask, claimedTask, err := task.Claim(ctx, rdb, queue, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		pool.processTask(1, rawTask, claimedTask)
		if attempt == 1 {
			if moved, err := task.MoveDueRetries(ctx, rdb, queue, time.Now().UTC().Add(time.Second)); err != nil || moved != 1 {
				t.Fatalf("move retry = %d, %v; want 1, nil", moved, err)
			}
		}
	}

	failedTask, err := task.GetMetadata(ctx, rdb, queuedTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failedTask.Status != task.StatusDead || failedTask.Attempts != 2 {
		t.Fatalf("dead task = status %s attempts %d", failedTask.Status, failedTask.Attempts)
	}
	if got := rdb.ZScore(ctx, task.RetryQueue, queuedTask.ID).Err(); !errors.Is(got, redis.Nil) {
		t.Fatalf("exhausted task was still scheduled: %v", got)
	}
	matches := 0
	for _, member := range rdb.LRange(ctx, task.DeadLetterQueue, 0, -1).Val() {
		if member == queuedTask.ID {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("DLQ occurrences = %d, want 1", matches)
	}
}

func TestWorkerProcessesOtherJobWhileRetryWaits(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	retryingTask := integrationTask(t, "retrying")
	retryingTask.MaxRetries = 2
	normalTask := integrationTask(t, "normal")
	queue := fmt.Sprintf("task_test_queue:%s", retryingTask.ID)
	t.Cleanup(func() {
		rdb.Del(ctx, queue, task.MetadataKey(retryingTask.ID), task.MetadataKey(normalTask.ID))
		rdb.ZRem(ctx, task.RetryQueue, retryingTask.ID)
	})
	if _, err := task.StoreAndEnqueue(ctx, rdb, queue, retryingTask); err != nil {
		t.Fatal(err)
	}
	if _, err := task.StoreAndEnqueue(ctx, rdb, queue, normalTask); err != nil {
		t.Fatal(err)
	}

	pool := NewPool(rdb, queue, 1, time.Hour, 500*time.Millisecond, log.New(io.Discard, "", 0))
	pool.execute = func(taskToExecute task.Task) error {
		if taskToExecute.ID == retryingTask.ID {
			return errors.New("temporary failure")
		}
		return nil
	}

	rawTask, claimedTask, err := task.Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool.processTask(1, rawTask, claimedTask)
	rawTask, claimedTask, err = task.Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool.processTask(1, rawTask, claimedTask)

	metadata, err := task.GetMetadata(ctx, rdb, normalTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Status != task.StatusCompleted {
		t.Fatalf("normal task status = %s, want completed", metadata.Status)
	}
	retryMetadata, err := task.GetMetadata(ctx, rdb, retryingTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retryMetadata.Status != task.StatusRetrying {
		t.Fatalf("retry task status = %s, want retrying", retryMetadata.Status)
	}
}
