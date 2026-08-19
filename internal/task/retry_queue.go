package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

const RetryQueue = "task_retry_queue"

// RetryDelay uses the failed execution attempt number: base*2^(attempts-1).
func RetryDelay(base time.Duration, attempts int) time.Duration {
	if attempts <= 1 {
		return base
	}

	delay := base
	for attempt := 1; attempt < attempts && delay <= time.Duration(math.MaxInt64/2); attempt++ {
		delay *= 2
	}
	return delay
}

func CanRetry(task Task) bool {
	return task.Attempts < task.MaxRetries
}

// ScheduleRetry atomically acknowledges a failed in-flight task and schedules its next attempt.
func ScheduleRetry(ctx context.Context, rdb *redis.Client, rawTask, id string, retryAt time.Time, lastError error) (Task, error) {
	task, metadata, err := getMetadata(ctx, rdb, id)
	if err != nil {
		return Task{}, err
	}
	if task.Status != StatusProcessing {
		return Task{}, fmt.Errorf("schedule retry: task %s is %s, not processing", id, task.Status)
	}

	task.Status = StatusRetrying
	task.ProcessingStartedAt = nil
	utcRetryAt := retryAt.UTC()
	task.NextRetryAt = &utcRetryAt
	if lastError != nil {
		task.LastError = lastError.Error()
	}
	updatedMetadata, err := json.Marshal(task)
	if err != nil {
		return Task{}, fmt.Errorf("marshal retry metadata: %w", err)
	}

	result, err := rdb.Eval(ctx, scheduleRetryScript, []string{ProcessingQueue, RetryQueue, MetadataKey(id)}, rawTask, string(metadata), string(updatedMetadata), retryAt.UnixMilli(), id).Int()
	if err != nil {
		return Task{}, fmt.Errorf("schedule retry: %w", err)
	}
	if result == 0 {
		return Task{}, errors.New("schedule retry: task changed before it could be scheduled")
	}
	return task, nil
}

// MoveDueRetries atomically returns eligible retrying jobs to the execution queue.
func MoveDueRetries(ctx context.Context, rdb *redis.Client, queue string, now time.Time) (int, error) {
	ids, err := rdb.ZRangeByScore(ctx, RetryQueue, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now.UnixMilli())}).Result()
	if err != nil {
		return 0, fmt.Errorf("list due retries: %w", err)
	}

	moved := 0
	for _, id := range ids {
		task, metadata, err := getMetadata(ctx, rdb, id)
		if errors.Is(err, ErrNotFound) {
			rdb.ZRem(ctx, RetryQueue, id)
			continue
		}
		if err != nil {
			return moved, err
		}
		if task.Status != StatusRetrying || task.NextRetryAt == nil {
			rdb.ZRem(ctx, RetryQueue, id)
			continue
		}
		if task.NextRetryAt.After(now) {
			// The sorted-set score has millisecond precision while metadata keeps the
			// full timestamp. Preserve the retry if the scheduler observes it slightly
			// early and restore its intended score.
			if err := rdb.ZAdd(ctx, RetryQueue, redis.Z{Score: float64(task.NextRetryAt.UnixMilli()), Member: id}).Err(); err != nil {
				return moved, fmt.Errorf("reschedule early retry: %w", err)
			}
			continue
		}

		task.Status = StatusPending
		task.NextRetryAt = nil
		updatedMetadata, err := json.Marshal(task)
		if err != nil {
			return moved, fmt.Errorf("marshal eligible retry metadata: %w", err)
		}
		queuedTask, err := json.Marshal(task)
		if err != nil {
			return moved, fmt.Errorf("marshal eligible retry task: %w", err)
		}

		result, err := rdb.Eval(ctx, moveDueRetryScript, []string{RetryQueue, queue, PrioritySequenceKey(queue), MetadataKey(id)}, id, string(metadata), string(updatedMetadata), string(queuedTask), task.Priority).Int()
		if err != nil {
			return moved, fmt.Errorf("move due retry: %w", err)
		}
		moved += result
	}
	return moved, nil
}

const scheduleRetryScript = `
if redis.call('GET', KEYS[3]) ~= ARGV[2] then
  return 0
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[3], ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
return 1
`

const moveDueRetryScript = `
if redis.call('GET', KEYS[4]) ~= ARGV[2] then
  return 0
end
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
  return 0
end
local sequence = redis.call('INCR', KEYS[3])
local member = string.format('%020d', sequence) .. '|' .. ARGV[4]
redis.call('SET', KEYS[4], ARGV[3])
redis.call('ZADD', KEYS[2], -tonumber(ARGV[5]), member)
return 1
`
