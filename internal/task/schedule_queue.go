package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const ScheduleQueue = "task_schedule_queue"

const scheduledMoveBatchSize int64 = 100

// StoreAndSchedule saves pending task metadata and schedules its first eligibility time atomically.
func StoreAndSchedule(ctx context.Context, rdb *redis.Client, task Task) error {
	if task.ScheduledAt == nil {
		return errors.New("schedule task: scheduled_at is required")
	}
	serializedTask, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal scheduled task metadata: %w", err)
	}
	if err := rdb.Eval(ctx, storeAndScheduleScript, []string{ScheduleQueue, MetadataKey(task.ID)}, string(serializedTask), task.ScheduledAt.UTC().UnixMilli(), task.ID).Err(); err != nil {
		return fmt.Errorf("store and schedule task: %w", err)
	}
	return nil
}

// MoveDueScheduled atomically makes all due pending jobs available to the priority queue.
func MoveDueScheduled(ctx context.Context, rdb *redis.Client, queue string, now time.Time) (int, error) {
	ids, err := rdb.ZRangeByScore(ctx, ScheduleQueue, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now.UTC().UnixMilli()), Count: scheduledMoveBatchSize}).Result()
	if err != nil {
		return 0, fmt.Errorf("list due scheduled tasks: %w", err)
	}

	moved := 0
	for _, id := range ids {
		queuedTask, metadata, err := getMetadata(ctx, rdb, id)
		if errors.Is(err, ErrNotFound) {
			if err := rdb.ZRem(ctx, ScheduleQueue, id).Err(); err != nil {
				return moved, fmt.Errorf("remove missing scheduled task: %w", err)
			}
			continue
		}
		if err != nil {
			return moved, err
		}
		if queuedTask.Status != StatusPending || queuedTask.ScheduledAt == nil {
			if err := rdb.ZRem(ctx, ScheduleQueue, id).Err(); err != nil {
				return moved, fmt.Errorf("remove stale scheduled task: %w", err)
			}
			continue
		}
		if queuedTask.ScheduledAt.After(now) {
			if err := rdb.ZAdd(ctx, ScheduleQueue, redis.Z{Score: float64(queuedTask.ScheduledAt.UTC().UnixMilli()), Member: id}).Err(); err != nil {
				return moved, fmt.Errorf("reschedule early scheduled task: %w", err)
			}
			continue
		}

		rawTask, err := json.Marshal(queuedTask)
		if err != nil {
			return moved, fmt.Errorf("marshal due scheduled task: %w", err)
		}
		result, err := rdb.Eval(ctx, moveDueScheduledScript, []string{ScheduleQueue, queue, PrioritySequenceKey(queue), MetadataKey(id)}, id, string(metadata), string(rawTask), queuedTask.Priority).Int()
		if err != nil {
			return moved, fmt.Errorf("move due scheduled task: %w", err)
		}
		moved += result
	}
	return moved, nil
}

const storeAndScheduleScript = `
redis.call('SET', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
return 1
`

const moveDueScheduledScript = `
if redis.call('GET', KEYS[4]) ~= ARGV[2] then
  return 0
end
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
  return 0
end
local sequence = redis.call('INCR', KEYS[3])
local member = string.format('%020d', sequence) .. '|' .. ARGV[3]
redis.call('ZADD', KEYS[2], -tonumber(ARGV[4]), member)
return 1
`
