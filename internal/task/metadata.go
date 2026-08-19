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

// StoreAndEnqueue saves task metadata and appends the task to the execution queue atomically.
func StoreAndEnqueue(ctx context.Context, rdb *redis.Client, queue string, task Task) (int64, error) {
	serializedTask, err := json.Marshal(task)
	if err != nil {
		return 0, fmt.Errorf("marshal task metadata: %w", err)
	}

	var queueLength *redis.IntCmd
	_, err = rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, MetadataKey(task.ID), serializedTask, 0)
		queueLength = pipe.RPush(ctx, queue, serializedTask)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("store and enqueue task: %w", err)
	}

	return queueLength.Val(), nil
}

func GetMetadata(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	metadata, err := rdb.Get(ctx, MetadataKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Task{}, ErrNotFound
	}
	if err != nil {
		return Task{}, fmt.Errorf("get task metadata: %w", err)
	}

	var task Task
	if err := json.Unmarshal(metadata, &task); err != nil {
		return Task{}, fmt.Errorf("unmarshal task metadata: %w", err)
	}

	return task, nil
}

func StartProcessing(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	return updateMetadata(ctx, rdb, id, func(task *Task) {
		task.Status = StatusProcessing
		task.Attempts++
		now := time.Now().UTC()
		task.ProcessingStartedAt = &now
	})
}

func MarkCompleted(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	return updateMetadata(ctx, rdb, id, func(task *Task) {
		task.Status = StatusCompleted
		task.ProcessingStartedAt = nil
	})
}

func MarkFailed(ctx context.Context, rdb *redis.Client, id string) (Task, error) {
	return updateMetadata(ctx, rdb, id, func(task *Task) {
		task.Status = StatusFailed
		task.ProcessingStartedAt = nil
	})
}

func updateMetadata(ctx context.Context, rdb *redis.Client, id string, update func(*Task)) (Task, error) {
	task, err := GetMetadata(ctx, rdb, id)
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
