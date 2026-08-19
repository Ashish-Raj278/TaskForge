package worker

import (
	"TaskForge/internal/logger"
	"TaskForge/internal/task"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultWorkerCount = 3
	defaultPollTimeout = time.Second
)

type Stats struct {
	QueueLength int64
	JobsDone    int64
	JobsFailed  int64
}

type Pool struct {
	rdb         *redis.Client
	queue       string
	workerCount int
	logger      *log.Logger
	queueLength atomic.Int64
	jobsDone    atomic.Int64
	jobsFailed  atomic.Int64
}

// WorkerCount returns the configured worker count, defaulting to three for missing or invalid values.
func WorkerCount(value string) int {
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 {
		return DefaultWorkerCount
	}
	return count
}

func NewPool(rdb *redis.Client, queue string, workerCount int, logger *log.Logger) *Pool {
	if workerCount < 1 {
		workerCount = DefaultWorkerCount
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Pool{rdb: rdb, queue: queue, workerCount: workerCount, logger: logger}
}

// Run starts the configured workers and returns after they all stop.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for workerID := 1; workerID <= p.workerCount; workerID++ {
		wg.Add(1)
		go p.runWorker(ctx, workerID, &wg)
	}
	wg.Wait()
}

func (p *Pool) Stats() Stats {
	return Stats{QueueLength: p.queueLength.Load(), JobsDone: p.jobsDone.Load(), JobsFailed: p.jobsFailed.Load()}
}

func (p *Pool) runWorker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	p.logger.Printf("worker %d started", workerID)
	defer p.logger.Printf("worker %d stopped", workerID)
	for {
		if ctx.Err() != nil {
			return
		}
		result, err := p.rdb.BLPop(ctx, defaultPollTimeout, p.queue).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			p.logger.Printf("worker %d stopped after Redis error: %v", workerID, err)
			return
		}
		p.updateQueueLength(workerID)
		var taskToExecute task.Task
		if err := json.Unmarshal([]byte(result[1]), &taskToExecute); err != nil {
			p.logger.Printf("worker %d could not decode queued task: %v", workerID, err)
			continue
		}
		p.processTask(workerID, taskToExecute)
	}
}

func (p *Pool) processTask(workerID int, taskToExecute task.Task) {
	metadataContext := context.Background()
	var err error
	taskToExecute, err = task.StartProcessing(metadataContext, p.rdb, taskToExecute.ID)
	if err != nil {
		p.logger.Printf("worker %d could not mark task %s as processing: %v", workerID, taskToExecute.ID, err)
		return
	}
	p.logger.Printf("worker %d processing task %s (%s)", workerID, taskToExecute.ID, taskToExecute.Type)
	if err := Process_Task(taskToExecute); err != nil {
		p.jobsFailed.Add(1)
		failedTask, updateErr := task.MarkFailed(metadataContext, p.rdb, taskToExecute.ID)
		if updateErr != nil {
			p.logger.Printf("worker %d could not mark task %s as failed: %v", workerID, taskToExecute.ID, updateErr)
			failedTask = taskToExecute
		}
		logger.LogFailure(failedTask, err)
		p.logger.Printf("worker %d failed task %s: %v", workerID, taskToExecute.ID, err)
		return
	}
	p.jobsDone.Add(1)
	completedTask, err := task.MarkCompleted(metadataContext, p.rdb, taskToExecute.ID)
	if err != nil {
		p.logger.Printf("worker %d could not mark task %s as completed: %v", workerID, taskToExecute.ID, err)
		completedTask = taskToExecute
	}
	logger.LogSuccess(completedTask)
	p.logger.Printf("worker %d completed task %s", workerID, taskToExecute.ID)
}

func (p *Pool) updateQueueLength(workerID int) {
	queueLength, err := p.rdb.LLen(context.Background(), p.queue).Result()
	if err != nil {
		p.logger.Printf("worker %d could not read queue length: %v", workerID, err)
		return
	}
	p.queueLength.Store(queueLength)
}
