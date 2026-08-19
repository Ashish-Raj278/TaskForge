package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func retryRedis(t *testing.T) *redis.Client {
	t.Helper()
	return redisClient(t)
}

func TestRetryDelay(t *testing.T) {
	base := 2 * time.Second
	for attempts, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second} {
		if got := RetryDelay(base, attempts); got != want {
			t.Fatalf("RetryDelay(%s, %d) = %s, want %s", base, attempts, got, want)
		}
	}
}

func TestScheduleRetryAndMoveDueRetry(t *testing.T) {
	ctx := context.Background()
	rdb := retryRedis(t)
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	queuedTask := Task{ID: id, Type: "generate_pdf", Payload: map[string]interface{}{}, Status: StatusPending, MaxRetries: 3, CreatedAt: time.Now().UTC()}
	queue := fmt.Sprintf("task_test_queue:%s", id)
	t.Cleanup(func() {
		rdb.Del(ctx, queue, MetadataKey(id))
		rdb.ZRem(ctx, RetryQueue, id)
	})
	if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	rawTask, _, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.LRem(ctx, ProcessingQueue, 1, rawTask) })
	if _, err := StartProcessing(ctx, rdb, id); err != nil {
		t.Fatal(err)
	}

	retryAt := time.Now().UTC().Add(time.Second)
	retryingTask, err := ScheduleRetry(ctx, rdb, rawTask, id, retryAt, errors.New("temporary failure"))
	if err != nil {
		t.Fatal(err)
	}
	if retryingTask.Status != StatusRetrying || retryingTask.Attempts != 1 {
		t.Fatalf("retry metadata = status %s, attempts %d", retryingTask.Status, retryingTask.Attempts)
	}
	if contains(ctx, rdb, ProcessingQueue, rawTask) {
		t.Fatal("scheduled retry remained in processing")
	}
	if score, err := rdb.ZScore(ctx, RetryQueue, id).Result(); err != nil || int64(score) != retryAt.UnixMilli() {
		t.Fatalf("retry score = %v, %v; want %d", score, err, retryAt.UnixMilli())
	}
	if moved, err := MoveDueRetries(ctx, rdb, queue, retryAt.Add(-time.Millisecond)); err != nil || moved != 0 {
		t.Fatalf("early move = %d, %v; want 0, nil", moved, err)
	}
	if moved, err := MoveDueRetries(ctx, rdb, queue, retryAt); err != nil || moved != 1 {
		t.Fatalf("due move = %d, %v; want 1, nil", moved, err)
	}
	metadata, err := GetMetadata(ctx, rdb, id)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Status != StatusPending || metadata.Attempts != 1 {
		t.Fatalf("moved metadata = status %s, attempts %d", metadata.Status, metadata.Attempts)
	}
	if got := rdb.ZCard(ctx, RetryQueue).Val(); got != 0 {
		t.Fatalf("retry members = %d, want 0", got)
	}
}
