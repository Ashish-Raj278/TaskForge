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

func Claim(ctx context.Context, rdb *redis.Client, queue string, timeout time.Duration) (string, Task, error) {
	raw, err := rdb.BLMove(ctx, queue, ProcessingQueue, "LEFT", "RIGHT", timeout).Result()
	if err != nil {
		return "", Task{}, err
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

		result, err := rdb.Eval(ctx, recoverScript, []string{ProcessingQueue, queue, MetadataKey(task.ID)}, rawTask, string(metadata), string(updatedMetadata)).Int()
		if err != nil {
			return recovered, fmt.Errorf("recover processing task: %w", err)
		}
		recovered += result
	}

	return recovered, nil
}

const recoverScript = `
if redis.call('GET', KEYS[3]) ~= ARGV[2] then
  return 0
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[3], ARGV[3])
redis.call('RPUSH', KEYS[2], ARGV[1])
return 1
`
