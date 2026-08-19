package worker

import (
	"TaskForge/internal/logger"
	"TaskForge/internal/task"
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultWorkerCount       = 3
	DefaultVisibilityTimeout = 30 * time.Second
	defaultPollTimeout       = time.Second
)

type Stats struct {
	QueueLength int64
	JobsDone    int64
	JobsFailed  int64
}

type Pool struct {
	rdb               *redis.Client
	queue             string
	workerCount       int
	visibilityTimeout time.Duration
	logger            *log.Logger
	queueLength       atomic.Int64
	jobsDone          atomic.Int64
	jobsFailed        atomic.Int64
}

// WorkerCount returns the configured worker count, defaulting to three for missing or invalid values.
func WorkerCount(value string) int {
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 {
		return DefaultWorkerCount
	}
	return count
}

// VisibilityTimeout returns the configured lease duration, defaulting to 30 seconds for missing or invalid values.
func VisibilityTimeout(value string) time.Duration {
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return DefaultVisibilityTimeout
	}
	return timeout
}

func NewPool(rdb *redis.Client, queue string, workerCount int, visibilityTimeout time.Duration, logger *log.Logger) *Pool {
	if workerCount < 1 {
		workerCount = DefaultWorkerCount
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = DefaultVisibilityTimeout
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Pool{
		rdb:               rdb,
		queue:             queue,
		workerCount:       workerCount,
		visibilityTimeout: visibilityTimeout,
		logger:            logger,
	}
}

// Run starts the configured workers and recovery loop, returning after they all stop.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go p.runRecovery(ctx, &wg)

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

		rawTask, taskToExecute, err := task.Claim(ctx, p.rdb, p.queue, defaultPollTimeout)
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
		p.processTask(workerID, rawTask, taskToExecute)
	}
}

func (p *Pool) processTask(workerID int, rawTask string, taskToExecute task.Task) {
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
			return
		}
		logger.LogFailure(failedTask, err)
		if ackErr := task.Acknowledge(metadataContext, p.rdb, rawTask); ackErr != nil {
			p.logger.Printf("worker %d could not acknowledge failed task %s: %v", workerID, taskToExecute.ID, ackErr)
			return
		}
		p.logger.Printf("worker %d failed task %s", workerID, taskToExecute.ID)
		return
	}

	p.jobsDone.Add(1)
	completedTask, err := task.MarkCompleted(metadataContext, p.rdb, taskToExecute.ID)
	if err != nil {
		p.logger.Printf("worker %d could not mark task %s as completed: %v", workerID, taskToExecute.ID, err)
		return
	}
	logger.LogSuccess(completedTask)
	if err := task.Acknowledge(metadataContext, p.rdb, rawTask); err != nil {
		p.logger.Printf("worker %d could not acknowledge completed task %s: %v", workerID, taskToExecute.ID, err)
		return
	}
	p.logger.Printf("worker %d completed task %s", workerID, taskToExecute.ID)
}

func (p *Pool) runRecovery(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	interval := p.visibilityTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	p.logger.Printf("processing recovery started with visibility timeout %s", p.visibilityTimeout)
	defer p.logger.Println("processing recovery stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovered, err := task.RecoverAbandoned(context.Background(), p.rdb, p.queue, p.visibilityTimeout)
			if err != nil {
				p.logger.Printf("processing recovery error: %v", err)
				continue
			}
			if recovered > 0 {
				p.logger.Printf("recovered %d abandoned task(s)", recovered)
			}
		}
	}
}

func (p *Pool) updateQueueLength(workerID int) {
	queueLength, err := p.rdb.LLen(context.Background(), p.queue).Result()
	if err != nil {
		p.logger.Printf("worker %d could not read queue length: %v", workerID, err)
		return
	}
	p.queueLength.Store(queueLength)
}
