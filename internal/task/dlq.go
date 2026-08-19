package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// DeadLetterQueue indexes jobs that exhausted their application-level attempts.
// It is separate from Task 4 processing-queue crash recovery; delivery remains at-least-once.
const DeadLetterQueue = "task_dlq"

// MoveToDLQ atomically records a terminal dead state, acknowledges the claimed
// task, and indexes its ID in the dead-letter queue.
func MoveToDLQ(ctx context.Context, rdb *redis.Client, rawTask, id string, lastError error) (Task, error) {
	task, metadata, err := getMetadata(ctx, rdb, id)
	if err != nil {
		return Task{}, err
	}
	if task.Status != StatusProcessing {
		return Task{}, fmt.Errorf("move task to DLQ: task %s is %s, not processing", id, task.Status)
	}

	task.Status = StatusDead
	task.ProcessingStartedAt = nil
	task.NextRetryAt = nil
	if lastError != nil {
		task.LastError = lastError.Error()
	}
	updatedMetadata, err := json.Marshal(task)
	if err != nil {
		return Task{}, fmt.Errorf("marshal dead task metadata: %w", err)
	}

	result, err := rdb.Eval(ctx, moveToDLQScript, []string{ProcessingQueue, DeadLetterQueue, MetadataKey(id)}, rawTask, string(metadata), string(updatedMetadata), id).Int()
	if err != nil {
		return Task{}, fmt.Errorf("move task to DLQ: %w", err)
	}
	if result == 0 {
		return Task{}, errors.New("move task to DLQ: task changed before acknowledgement")
	}
	return task, nil
}

// ListDeadTasks returns up to limit current dead-job metadata records.
func ListDeadTasks(ctx context.Context, rdb *redis.Client, limit int64) ([]Task, error) {
	if limit < 1 {
		return []Task{}, nil
	}
	ids, err := rdb.LRange(ctx, DeadLetterQueue, 0, limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("list dead-letter queue: %w", err)
	}

	deadTasks := make([]Task, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}

		queuedTask, err := GetMetadata(ctx, rdb, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if queuedTask.Status == StatusDead {
			deadTasks = append(deadTasks, queuedTask)
		}
	}
	return deadTasks, nil
}

// ReplayDeadTask atomically resets a dead job and appends it to the execution queue.
func ReplayDeadTask(ctx context.Context, rdb *redis.Client, queue, id string) (Task, error) {
	task, metadata, err := getMetadata(ctx, rdb, id)
	if err != nil {
		return Task{}, err
	}
	if task.Status != StatusDead {
		return Task{}, fmt.Errorf("replay dead task: task %s is %s, not dead", id, task.Status)
	}

	task.Status = StatusPending
	task.Attempts = 0
	task.ProcessingStartedAt = nil
	task.NextRetryAt = nil
	task.LastError = ""
	updatedMetadata, err := json.Marshal(task)
	if err != nil {
		return Task{}, fmt.Errorf("marshal replay task metadata: %w", err)
	}
	queuedTask, err := json.Marshal(task)
	if err != nil {
		return Task{}, fmt.Errorf("marshal replay task: %w", err)
	}

	result, err := rdb.Eval(ctx, replayDeadTaskScript, []string{DeadLetterQueue, queue, PrioritySequenceKey(queue), MetadataKey(id)}, id, string(metadata), string(updatedMetadata), string(queuedTask), task.Priority).Int()
	if err != nil {
		return Task{}, fmt.Errorf("replay dead task: %w", err)
	}
	if result == 0 {
		return Task{}, errors.New("replay dead task: task changed before replay")
	}
	return task, nil
}

const moveToDLQScript = `
if redis.call('GET', KEYS[3]) ~= ARGV[2] then
  return 0
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[3], ARGV[3])
redis.call('LREM', KEYS[2], 0, ARGV[4])
redis.call('LPUSH', KEYS[2], ARGV[4])
return 1
`

const replayDeadTaskScript = `
if redis.call('GET', KEYS[4]) ~= ARGV[2] then
  return 0
end
if redis.call('LREM', KEYS[1], 0, ARGV[1]) == 0 then
  return 0
end
local sequence = redis.call('INCR', KEYS[3])
local member = string.format('%020d', sequence) .. '|' .. ARGV[4]
redis.call('SET', KEYS[4], ARGV[3])
redis.call('ZADD', KEYS[2], -tonumber(ARGV[5]), member)
return 1
`
