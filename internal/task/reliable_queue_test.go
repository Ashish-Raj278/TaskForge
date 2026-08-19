package task

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse Redis URL: %v", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Skipf("Redis unavailable: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func testTask(t *testing.T, kind string) Task {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return Task{ID: id, Type: kind, Payload: map[string]interface{}{}, Status: StatusPending, CreatedAt: time.Now().UTC()}
}

func TestClaimMovesTaskToProcessingAndRecoveryRestoresPending(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	taskToRecover := testTask(t, "generate_pdf")
	queue := fmt.Sprintf("task_test_queue:%s", taskToRecover.ID)
	t.Cleanup(func() {
		rdb.Del(ctx, queue, MetadataKey(taskToRecover.ID))
	})

	if _, err := StoreAndEnqueue(ctx, rdb, queue, taskToRecover); err != nil {
		t.Fatal(err)
	}
	rawTask, claimedTask, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.LRem(ctx, ProcessingQueue, 1, rawTask) })
	if claimedTask.ID != taskToRecover.ID {
		t.Fatalf("claimed task ID = %s, want %s", claimedTask.ID, taskToRecover.ID)
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != 0 {
		t.Fatalf("queued tasks = %d, want 0", got)
	}
	if !contains(ctx, rdb, ProcessingQueue, rawTask) {
		t.Fatal("claimed task was not present in the processing queue")
	}

	if _, err := StartProcessing(ctx, rdb, taskToRecover.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverAbandoned(ctx, rdb, queue, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}

	recoveredTask, err := GetMetadata(ctx, rdb, taskToRecover.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredTask.Status != StatusPending {
		t.Fatalf("status = %s, want %s", recoveredTask.Status, StatusPending)
	}
	if recoveredTask.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", recoveredTask.Attempts)
	}
	if contains(ctx, rdb, ProcessingQueue, rawTask) {
		t.Fatal("recovered task remained in the processing queue")
	}
	if got := rdb.ZCard(ctx, queue).Val(); got != 1 {
		t.Fatalf("queued tasks = %d, want 1", got)
	}
}

func contains(ctx context.Context, rdb *redis.Client, queue, rawTask string) bool {
	tasks := rdb.LRange(ctx, queue, 0, -1).Val()
	for _, task := range tasks {
		if task == rawTask {
			return true
		}
	}
	return false
}
