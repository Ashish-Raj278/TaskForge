package task

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMoveToDLQAndReplay(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	queuedTask := testTask(t, "unsupported")
	queue := fmt.Sprintf("task_test_queue:%s", queuedTask.ID)
	t.Cleanup(func() {
		rdb.Del(ctx, queue, MetadataKey(queuedTask.ID))
		rdb.LRem(ctx, DeadLetterQueue, 0, queuedTask.ID)
	})
	if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	rawTask, _, err := Claim(ctx, rdb, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := StartProcessing(ctx, rdb, queuedTask.ID); err != nil {
		t.Fatal(err)
	}

	deadTask, err := MoveToDLQ(ctx, rdb, rawTask, queuedTask.ID, errors.New("permanent failure"))
	if err != nil {
		t.Fatal(err)
	}
	if deadTask.Status != StatusDead || deadTask.Attempts != 1 || deadTask.LastError != "permanent failure" {
		t.Fatalf("dead task = status %s attempts %d error %q", deadTask.Status, deadTask.Attempts, deadTask.LastError)
	}
	if contains(ctx, rdb, ProcessingQueue, rawTask) {
		t.Fatal("dead task remained in processing")
	}
	if matches := countDLQEntries(ctx, rdb, queuedTask.ID); matches != 1 {
		t.Fatalf("DLQ occurrences = %d, want 1", matches)
	}
	deadTasks, err := ListDeadTasks(ctx, rdb, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, listedTask := range deadTasks {
		if listedTask.ID == queuedTask.ID && listedTask.Status == StatusDead {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("dead task %s was not listed", queuedTask.ID)
	}

	replayedTask, err := ReplayDeadTask(ctx, rdb, queue, queuedTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayedTask.Status != StatusPending || replayedTask.Attempts != 0 || replayedTask.LastError != "" {
		t.Fatalf("replayed task = status %s attempts %d error %q", replayedTask.Status, replayedTask.Attempts, replayedTask.LastError)
	}
	if matches := countDLQEntries(ctx, rdb, queuedTask.ID); matches != 0 {
		t.Fatalf("DLQ occurrences after replay = %d, want 0", matches)
	}
	if got := rdb.LLen(ctx, queue).Val(); got != 1 {
		t.Fatalf("queue length = %d, want 1", got)
	}
	if _, err := ReplayDeadTask(ctx, rdb, queue, queuedTask.ID); err == nil {
		t.Fatal("duplicate replay succeeded")
	}
	if got := rdb.LLen(ctx, queue).Val(); got != 1 {
		t.Fatalf("queue length after duplicate replay = %d, want 1", got)
	}
}

func countDLQEntries(ctx context.Context, rdb *redis.Client, id string) int {
	matches := 0
	for _, member := range rdb.LRange(ctx, DeadLetterQueue, 0, -1).Val() {
		if member == id {
			matches++
		}
	}
	return matches
}

func TestReplayRejectsNonDeadTask(t *testing.T) {
	ctx := context.Background()
	rdb := redisClient(t)
	queuedTask := testTask(t, "generate_pdf")
	queue := fmt.Sprintf("task_test_queue:%s", queuedTask.ID)
	t.Cleanup(func() { rdb.Del(ctx, queue, MetadataKey(queuedTask.ID)) })
	if _, err := StoreAndEnqueue(ctx, rdb, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayDeadTask(ctx, rdb, queue, queuedTask.ID); err == nil {
		t.Fatal("pending task replay succeeded")
	}
}
