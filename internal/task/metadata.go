package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const MetadataKeyPrefix = "task:"

var ErrNotFound = errors.New("task metadata not found")

func MetadataKey(id string) string {
	return MetadataKeyPrefix + id
}

func GetMetadata(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	task, _, err := getMetadata(ctx, rdb, id)
	return task, err
}

func getMetadata(ctx context.Context, rdb *redis.Client, id string) (Task, []byte, error) {
	metadata, err := rdb.Get(ctx, MetadataKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Task{}, nil, ErrNotFound
	}
	if err != nil {
		return Task{}, nil, fmt.Errorf("get task metadata: %w", err)
	}

	var task Task
	if err := json.Unmarshal(metadata, &task); err != nil {
		return Task{}, nil, fmt.Errorf("unmarshal task metadata: %w", err)
	}

	return task, metadata, nil
}

func StartProcessing(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	return updateMetadata(ctx, rdb, id, func(task *Task) {
		task.Status = StatusProcessing
		task.Attempts++
		now := time.Now().UTC()
		task.ProcessingStartedAt = &now
		task.NextRetryAt = nil
	})
}

func MarkCompleted(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	return updateMetadata(ctx, rdb, id, func(task *Task) {
		task.Status = StatusCompleted
		task.ProcessingStartedAt = nil
		task.NextRetryAt = nil
		task.LastError = ""
	})
}

// MarkCompletedAndAcknowledge atomically records completion and removes the claimed task from processing.
func MarkCompletedAndAcknowledge(ctx context.Context, rdb *redis.Client, rawTask, id string) (Task, error) {
	return markTerminalAndAcknowledge(ctx, rdb, rawTask, id, StatusCompleted, nil)
}

// MarkFailedAndAcknowledge atomically records final failure and removes the claimed task from processing.
func MarkFailedAndAcknowledge(ctx context.Context, rdb *redis.Client, rawTask, id string, lastError error) (Task, error) {
	return markTerminalAndAcknowledge(ctx, rdb, rawTask, id, StatusFailed, lastError)
}

func markTerminalAndAcknowledge(ctx context.Context, rdb *redis.Client, rawTask, id string, status Status, lastError error) (Task, error) {
	task, metadata, err := getMetadata(ctx, rdb, id)
	if err != nil {
		return Task{}, err
	}
	if task.Status != StatusProcessing {
		return Task{}, fmt.Errorf("mark terminal task: task %s is %s, not processing", id, task.Status)
	}

	task.Status = status
	task.ProcessingStartedAt = nil
	task.NextRetryAt = nil
	if lastError != nil {
		task.LastError = lastError.Error()
	} else {
		task.LastError = ""
	}
	updatedMetadata, err := json.Marshal(task)
	if err != nil {
		return Task{}, fmt.Errorf("marshal terminal task metadata: %w", err)
	}

	result, err := rdb.Eval(ctx, markTerminalScript, []string{ProcessingQueue, MetadataKey(id)}, rawTask, string(metadata), string(updatedMetadata)).Int()
	if err != nil {
		return Task{}, fmt.Errorf("mark terminal task: %w", err)
	}
	if result == 0 {
		return Task{}, errors.New("mark terminal task: task changed before acknowledgement")
	}
	return task, nil
}

func MarkFailed(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	return updateMetadata(ctx, rdb, id, func(task *Task) {
		task.Status = StatusFailed
		task.ProcessingStartedAt = nil
		task.NextRetryAt = nil
	})
}

const markTerminalScript = `
if redis.call('GET', KEYS[2]) ~= ARGV[2] then
  return 0
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[2], ARGV[3])
return 1
`

func updateMetadata(ctx context.Context, rdb *redis.Client, id string, update func(*Task)) (Task, error) {
	task, _, err := getMetadata(ctx, rdb, id)
	if err != nil {
		return Task{}, err
	}

	update(&task)
	metadata, err := json.Marshal(task)
	if err != nil {
		return Task{}, fmt.Errorf("marshal updated task metadata: %w", err)
	}

	if err := rdb.Set(ctx, MetadataKey(id), metadata, 0).Err(); err != nil {
		return Task{}, fmt.Errorf("update task metadata: %w", err)
	}

	return task, nil
}
