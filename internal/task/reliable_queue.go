package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const ProcessingQueue = "task_processing"

// Claim atomically moves the highest-priority pending task to processing.
// A missing task returns redis.Nil; callers use their cancellation-aware poll interval.
func Claim(ctx context.Context, rdb *redis.Client, queue string, _ time.Duration) (string, Task, error) {
	result, err := rdb.Eval(ctx, claimPriorityScript, []string{queue, ProcessingQueue}).Result()
	if err != nil {
		return "", Task{}, err
	}
	if result == nil {
		return "", Task{}, redis.Nil
	}
	raw, ok := result.(string)
	if !ok {
		return "", Task{}, fmt.Errorf("claim task: unexpected Redis result %T", result)
	}

	var task Task
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return raw, Task{}, fmt.Errorf("unmarshal claimed task: %w", err)
	}

	return raw, task, nil
}

func Acknowledge(ctx context.Context, rdb *redis.Client, rawTask string) error {
	removed, err := rdb.LRem(ctx, ProcessingQueue, 1, rawTask).Result()
	if err != nil {
		return fmt.Errorf("acknowledge task: %w", err)
	}
	if removed == 0 {
		return errors.New("acknowledge task: task not found in processing queue")
	}
	return nil
}

// RecoverAbandoned moves expired processing tasks back to the queue without changing Attempts.
func RecoverAbandoned(ctx context.Context, rdb *redis.Client, queue string, visibilityTimeout time.Duration) (int, error) {
	return RecoverAbandonedWithCallback(ctx, rdb, queue, visibilityTimeout, nil)
}

// RecoverAbandonedWithCallback invokes recovered after an abandoned processing
// entry has been atomically restored to the priority queue. The callback is
// observational only and cannot affect queue recovery.
func RecoverAbandonedWithCallback(ctx context.Context, rdb *redis.Client, queue string, visibilityTimeout time.Duration, onRecovered func(Task)) (int, error) {
	rawTasks, err := rdb.LRange(ctx, ProcessingQueue, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("list processing tasks: %w", err)
	}

	recovered := 0
	cutoff := time.Now().UTC().Add(-visibilityTimeout)
	for _, rawTask := range rawTasks {
		var queuedTask Task
		if err := json.Unmarshal([]byte(rawTask), &queuedTask); err != nil {
			continue
		}

		metadata, err := rdb.Get(ctx, MetadataKey(queuedTask.ID)).Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return recovered, fmt.Errorf("get processing metadata: %w", err)
		}

		var task Task
		if err := json.Unmarshal(metadata, &task); err != nil {
			return recovered, fmt.Errorf("unmarshal processing metadata: %w", err)
		}
		if task.Status != StatusProcessing || task.ProcessingStartedAt == nil || task.ProcessingStartedAt.After(cutoff) {
			continue
		}

		task.Status = StatusPending
		task.ProcessingStartedAt = nil
		updatedMetadata, err := json.Marshal(task)
		if err != nil {
			return recovered, fmt.Errorf("marshal recovered metadata: %w", err)
		}

		result, err := rdb.Eval(ctx, recoverScript, []string{ProcessingQueue, queue, PrioritySequenceKey(queue), MetadataKey(task.ID)}, rawTask, string(metadata), string(updatedMetadata), task.Priority).Int()
		if err != nil {
			return recovered, fmt.Errorf("recover processing task: %w", err)
		}
		recovered += result
		if result == 1 && onRecovered != nil {
			onRecovered(task)
		}
	}

	return recovered, nil
}

const claimPriorityScript = `
local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if #popped == 0 then
  return false
end
local raw = string.sub(popped[1], 22)
redis.call('RPUSH', KEYS[2], raw)
return raw
`

const recoverScript = `
if redis.call('GET', KEYS[4]) ~= ARGV[2] then
  return 0
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
local sequence = redis.call('INCR', KEYS[3])
local member = string.format('%020d', sequence) .. '|' .. ARGV[1]
redis.call('SET', KEYS[4], ARGV[3])
redis.call('ZADD', KEYS[2], -tonumber(ARGV[4]), member)
return 1
`
