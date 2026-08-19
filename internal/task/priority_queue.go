package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const PriorityQueue = "task_priority_queue"

// PrioritySequenceKey stores the monotonic enqueue sequence for a priority queue.
func PrioritySequenceKey(queue string) string {
	return queue + ":sequence"
}

// StoreAndEnqueue stores metadata and inserts the pending task into the priority queue atomically.
// Higher priority values are claimed first; equal-priority tasks use Redis' monotonic sequence order.
func StoreAndEnqueue(ctx context.Context, rdb *redis.Client, queue string, task Task) (int64, error) {
	serializedTask, err := json.Marshal(task)
	if err != nil {
		return 0, fmt.Errorf("marshal task metadata: %w", err)
	}

	queueLength, err := rdb.Eval(ctx, storeAndEnqueueScript, []string{queue, PrioritySequenceKey(queue), MetadataKey(task.ID)}, string(serializedTask), task.Priority).Int64()
	if err != nil {
		return 0, fmt.Errorf("store and enqueue task: %w", err)
	}
	return queueLength, nil
}

const storeAndEnqueueScript = `
local sequence = redis.call('INCR', KEYS[2])
local member = string.format('%020d', sequence) .. '|' .. ARGV[1]
redis.call('SET', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[1], -tonumber(ARGV[2]), member)
return redis.call('ZCARD', KEYS[1])
`
