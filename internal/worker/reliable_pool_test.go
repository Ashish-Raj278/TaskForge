package worker

import (
	"TaskForge/internal/task"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func integrationRedis(t *testing.T) *redis.Client {
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

func integrationTask(t *testing.T, kind string) task.Task {
	t.Helper()
	id, err := task.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return task.Task{ID: id, Type: kind, Payload: map[string]interface{}{}, Status: task.StatusPending, CreatedAt: time.Now().UTC()}
}

func TestProcessTaskAcknowledgesTerminalTasks(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	tests := []struct {
		name       string
		taskType   string
		wantStatus task.Status
	}{
		{name: "success", taskType: "generate_pdf", wantStatus: task.StatusCompleted},
		{name: "application failure", taskType: "unsupported", wantStatus: task.StatusFailed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queuedTask := integrationTask(t, test.taskType)
			queue := fmt.Sprintf("task_test_queue:%s", queuedTask.ID)
			t.Cleanup(func() {
				rdb.Del(ctx, queue, task.MetadataKey(queuedTask.ID))
			})
			if _, err := task.StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
				t.Fatal(err)
			}
			rawTask, claimedTask, err := task.Claim(ctx, rdb, queue, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { rdb.LRem(ctx, task.ProcessingQueue, 1, rawTask) })

			pool := NewPool(rdb, queue, 1, time.Hour, log.New(io.Discard, "", 0))
			pool.processTask(1, rawTask, claimedTask)

			metadata, err := task.GetMetadata(ctx, rdb, queuedTask.ID)
			if err != nil {
				t.Fatal(err)
			}
			if metadata.Status != test.wantStatus {
				t.Fatalf("status = %s, want %s", metadata.Status, test.wantStatus)
			}
			if metadata.Attempts != 1 {
				t.Fatalf("attempts = %d, want 1", metadata.Attempts)
			}
			if containsTask(ctx, rdb, task.ProcessingQueue, rawTask) {
				t.Fatal("terminal task was not acknowledged")
			}
		})
	}
}

func TestPoolConsumesTasksConcurrently(t *testing.T) {
	ctx := context.Background()
	rdb := integrationRedis(t)
	queue := fmt.Sprintf("task_test_queue:%d", time.Now().UnixNano())
	queuedTasks := make([]task.Task, 4)
	for index := range queuedTasks {
		queuedTasks[index] = integrationTask(t, "generate_pdf")
		if _, err := task.StoreAndEnqueue(ctx, rdb, queue, queuedTasks[index]); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		rdb.Del(ctx, queue)
		for _, queuedTask := range queuedTasks {
			rdb.Del(ctx, task.MetadataKey(queuedTask.ID))
		}
	})

	poolContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewPool(rdb, queue, 2, time.Hour, log.New(io.Discard, "", 0))
	done := make(chan struct{})
	go func() {
		pool.Run(poolContext)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		completed := 0
		for _, queuedTask := range queuedTasks {
			metadata, err := task.GetMetadata(ctx, rdb, queuedTask.ID)
			if err != nil {
				t.Fatal(err)
			}
			if metadata.Status == task.StatusCompleted {
				completed++
			}
		}
		if completed == len(queuedTasks) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed = %d, want %d", completed, len(queuedTasks))
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not stop after context cancellation")
	}
	if stats := pool.Stats(); stats.JobsDone != int64(len(queuedTasks)) {
		t.Fatalf("jobs done = %d, want %d", stats.JobsDone, len(queuedTasks))
	}
}

func containsTask(ctx context.Context, rdb *redis.Client, queue, rawTask string) bool {
	for _, task := range rdb.LRange(ctx, queue, 0, -1).Val() {
		if task == rawTask {
			return true
		}
	}
	return false
}
