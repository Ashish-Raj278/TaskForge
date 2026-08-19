package main

import (
	"TaskForge/internal/task"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func producerRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Skipf("Redis unavailable: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestDeadJobAndDLQReplayHandlers(t *testing.T) {
	requestContext := context.Background()
	client := producerRedis(t)
	originalRDB := rdb
	rdb = client
	t.Cleanup(func() { rdb = originalRDB })

	id, err := task.NewID()
	if err != nil {
		t.Fatal(err)
	}
	queue := fmt.Sprintf("task_api_queue:%s", id)
	queuedTask := task.Task{ID: id, Type: "unsupported", Payload: map[string]interface{}{}, Status: task.StatusPending, MaxRetries: 1, CreatedAt: time.Now().UTC()}
	t.Cleanup(func() {
		client.Del(requestContext, queue, task.MetadataKey(id))
		client.LRem(requestContext, task.DeadLetterQueue, 0, id)
	})
	if _, err := task.StoreAndEnqueue(requestContext, client, queue, queuedTask); err != nil {
		t.Fatal(err)
	}
	rawTask, _, err := task.Claim(requestContext, client, queue, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.StartProcessing(requestContext, client, id); err != nil {
		t.Fatal(err)
	}
	if _, err := task.MoveToDLQ(requestContext, client, rawTask, id, fmt.Errorf("unsupported task")); err != nil {
		t.Fatal(err)
	}

	jobResponse := httptest.NewRecorder()
	getJobHandler(jobResponse, httptest.NewRequest(http.MethodGet, "/jobs/"+id, nil))
	if jobResponse.Code != http.StatusOK {
		t.Fatalf("GET /jobs status = %d, want 200", jobResponse.Code)
	}
	var returnedJob task.Task
	if err := json.NewDecoder(jobResponse.Body).Decode(&returnedJob); err != nil {
		t.Fatal(err)
	}
	if returnedJob.Status != task.StatusDead {
		t.Fatalf("GET /jobs status = %s, want dead", returnedJob.Status)
	}

	dlqResponse := httptest.NewRecorder()
	getDLQHandler(dlqResponse, httptest.NewRequest(http.MethodGet, "/dlq?limit=10", nil))
	if dlqResponse.Code != http.StatusOK {
		t.Fatalf("GET /dlq status = %d, want 200", dlqResponse.Code)
	}
	var deadTasks []task.Task
	if err := json.NewDecoder(dlqResponse.Body).Decode(&deadTasks); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, deadTask := range deadTasks {
		if deadTask.ID == id && deadTask.Status == task.StatusDead {
			found = true
		}
	}
	if !found {
		t.Fatal("GET /dlq did not return the dead job")
	}

	retryResponse := httptest.NewRecorder()
	retryDLQHandler(retryResponse, httptest.NewRequest(http.MethodPost, "/dlq/"+id+"/retry", nil))
	if retryResponse.Code != http.StatusOK {
		t.Fatalf("POST /dlq/{id}/retry status = %d, want 200", retryResponse.Code)
	}
	var replayedTask task.Task
	if err := json.NewDecoder(retryResponse.Body).Decode(&replayedTask); err != nil {
		t.Fatal(err)
	}
	if replayedTask.Status != task.StatusPending || replayedTask.Attempts != 0 {
		t.Fatalf("replayed task = status %s attempts %d", replayedTask.Status, replayedTask.Attempts)
	}
	matches := 0
	for _, member := range client.LRange(requestContext, task.DeadLetterQueue, 0, -1).Val() {
		if member == id {
			matches++
		}
	}
	if matches != 0 {
		t.Fatalf("DLQ occurrences after replay = %d, want 0", matches)
	}

	duplicateResponse := httptest.NewRecorder()
	retryDLQHandler(duplicateResponse, httptest.NewRequest(http.MethodPost, "/dlq/"+id+"/retry", nil))
	if duplicateResponse.Code != http.StatusConflict {
		t.Fatalf("duplicate replay status = %d, want 409", duplicateResponse.Code)
	}
}
