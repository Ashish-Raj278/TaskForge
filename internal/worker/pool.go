package worker

import (
	"TaskForge/internal/logger"
	"TaskForge/internal/observability"
	"TaskForge/internal/task"
	"TaskForge/internal/tracing"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	DefaultWorkerCount        = 3
	DefaultVisibilityTimeout  = 30 * time.Second
	DefaultRetryBaseDelay     = 2 * time.Second
	defaultPollTimeout        = time.Second
	retrySchedulerInterval    = 500 * time.Millisecond
	scheduleSchedulerInterval = 500 * time.Millisecond
	redisFailureBackoff       = time.Second
)

type Stats struct {
	JobsDone   int64
	JobsFailed int64
}

type Pool struct {
	rdb               *redis.Client
	queue             string
	workerCount       int
	visibilityTimeout time.Duration
	retryBaseDelay    time.Duration
	logger            *log.Logger
	execute           func(task.Task) error
	jobsDone          atomic.Int64
	jobsFailed        atomic.Int64
	metrics           *observability.Metrics
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

// RetryBaseDelay returns the configured delay before the first retry, defaulting to two seconds.
func RetryBaseDelay(value string) time.Duration {
	delay, err := time.ParseDuration(value)
	if err != nil || delay <= 0 {
		return DefaultRetryBaseDelay
	}
	return delay
}

func NewPool(rdb *redis.Client, queue string, workerCount int, visibilityTimeout, retryBaseDelay time.Duration, logger *log.Logger) *Pool {
	if workerCount < 1 {
		workerCount = DefaultWorkerCount
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = DefaultVisibilityTimeout
	}
	if retryBaseDelay <= 0 {
		retryBaseDelay = DefaultRetryBaseDelay
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Pool{
		rdb:               rdb,
		queue:             queue,
		workerCount:       workerCount,
		visibilityTimeout: visibilityTimeout,
		retryBaseDelay:    retryBaseDelay,
		logger:            logger,
		execute:           Process_Task,
		metrics:           observability.NewMetrics(),
	}
}

func (p *Pool) Metrics() *observability.Metrics { return p.metrics }

// Run starts the configured workers and recovery loop, returning after they all stop.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go p.runRecovery(ctx, &wg)
	wg.Add(1)
	go p.runRetryScheduler(ctx, &wg)
	wg.Add(1)
	go p.runScheduleScheduler(ctx, &wg)

	for workerID := 1; workerID <= p.workerCount; workerID++ {
		wg.Add(1)
		go p.runWorker(ctx, workerID, &wg)
	}
	wg.Wait()
}

func (p *Pool) runScheduleScheduler(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(scheduleSchedulerInterval)
	defer ticker.Stop()

	logger.Event("scheduler_started")
	defer logger.Event("scheduler_stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			moved, err := task.MoveDueScheduledWithCallback(ctx, p.rdb, p.queue, time.Now().UTC(), func(queuedTask task.Task) {
				spanContext := tracing.ContextWithTraceParent(ctx, queuedTask.TraceParent)
				spanContext, span := tracing.Start(spanContext, "task.schedule.promote", oteltrace.WithAttributes(tracing.JobAttributes(queuedTask.ID, queuedTask.Type, queuedTask.Priority, queuedTask.Attempts, string(queuedTask.Status))...))
				if queuedTask.ScheduledAt != nil {
					span.SetAttributes(attribute.String("job.scheduled_at", queuedTask.ScheduledAt.UTC().Format(time.RFC3339Nano)))
				}
				logger.JobContext(spanContext, "job_scheduled_promoted", queuedTask, nil)
				span.End()
			})
			if err != nil {
				p.logger.Printf("scheduled-job scheduler error: %v", err)
				if !waitForContext(ctx, redisFailureBackoff) {
					return
				}
				continue
			}
			if moved > 0 {
				p.logger.Printf("returned %d scheduled task(s) to the priority queue", moved)
			}
		}
	}
}

func (p *Pool) Stats() Stats {
	return Stats{JobsDone: p.jobsDone.Load(), JobsFailed: p.jobsFailed.Load()}
}

func (p *Pool) runWorker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	p.metrics.WorkerStarted()
	defer p.metrics.WorkerStopped()
	logger.Event("worker_started")
	defer logger.Event("worker_stopped")
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
				pollTimer := time.NewTimer(defaultPollTimeout)
				select {
				case <-ctx.Done():
					if !pollTimer.Stop() {
						<-pollTimer.C
					}
					return
				case <-pollTimer.C:
				}
				continue
			}
			p.logger.Printf("worker %d Redis error: %v", workerID, err)
			if !waitForContext(ctx, redisFailureBackoff) {
				return
			}
			continue
		}

		claimContext := tracing.ContextWithTraceParent(ctx, taskToExecute.TraceParent)
		claimContext, claimSpan := tracing.Start(claimContext, "task.claim", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer), oteltrace.WithAttributes(tracing.JobAttributes(taskToExecute.ID, taskToExecute.Type, taskToExecute.Priority, taskToExecute.Attempts, string(taskToExecute.Status))...))
		claimSpan.SetAttributes(attribute.Int("worker.id", workerID))
		logger.JobForWorkerContext(claimContext, "job_claimed", workerID, taskToExecute, nil, 0)
		claimSpan.End()
		p.processTaskContext(claimContext, workerID, rawTask, taskToExecute)
	}
}

func (p *Pool) processTask(workerID int, rawTask string, taskToExecute task.Task) {
	p.processTaskContext(context.Background(), workerID, rawTask, taskToExecute)
}

func (p *Pool) processTaskContext(ctx context.Context, workerID int, rawTask string, taskToExecute task.Task) {
	metadataContext := context.Background()
	var err error
	taskToExecute, err = task.StartProcessing(metadataContext, p.rdb, taskToExecute.ID)
	if err != nil {
		p.logger.Printf("worker %d could not mark task %s as processing: %v", workerID, taskToExecute.ID, err)
		return
	}
	executionContext, executionSpan := tracing.Start(ctx, "task.execute", oteltrace.WithAttributes(tracing.JobAttributes(taskToExecute.ID, taskToExecute.Type, taskToExecute.Priority, taskToExecute.Attempts, string(taskToExecute.Status))...))
	defer executionSpan.End()
	executionSpan.SetAttributes(attribute.Int("worker.id", workerID))
	logger.JobForWorkerContext(executionContext, "job_started", workerID, taskToExecute, nil, 0)
	executionStarted := time.Now()

	p.logger.Printf("worker %d processing task %s (%s)", workerID, taskToExecute.ID, taskToExecute.Type)
	if err := executeSafely(p.execute, taskToExecute); err != nil {
		p.jobsFailed.Add(1)
		p.metrics.Failed()
		tracing.SetError(executionSpan, err)
		logger.JobForWorkerContext(executionContext, "job_failed", workerID, taskToExecute, err, time.Since(executionStarted))
		if task.CanRetry(taskToExecute) {
			retryAt := time.Now().UTC().Add(task.RetryDelay(p.retryBaseDelay, taskToExecute.Attempts))
			retryContext, retrySpan := tracing.Start(executionContext, "task.retry.schedule", oteltrace.WithAttributes(tracing.JobAttributes(taskToExecute.ID, taskToExecute.Type, taskToExecute.Priority, taskToExecute.Attempts, string(task.StatusRetrying))...))
			retrySpan.SetAttributes(attribute.String("job.retry_at", retryAt.Format(time.RFC3339Nano)))
			retryingTask, scheduleErr := task.ScheduleRetry(metadataContext, p.rdb, rawTask, taskToExecute.ID, retryAt, err)
			if scheduleErr != nil {
				tracing.SetError(retrySpan, scheduleErr)
				retrySpan.End()
				p.logger.Printf("worker %d could not schedule retry for task %s: %v", workerID, taskToExecute.ID, scheduleErr)
				return
			}
			retrySpan.SetStatus(codes.Ok, "retry scheduled")
			retrySpan.End()
			executionSpan.SetAttributes(attribute.String("job.final_status", string(task.StatusRetrying)))
			p.metrics.Retried()
			logger.JobForWorkerContext(retryContext, "job_retry_scheduled", workerID, retryingTask, err, time.Since(executionStarted))
			p.logger.Printf("worker %d scheduled retry for task %s at %s", workerID, taskToExecute.ID, retryAt.Format(time.RFC3339Nano))
			return
		}

		dlqContext, dlqSpan := tracing.Start(executionContext, "task.dlq.transition", oteltrace.WithAttributes(tracing.JobAttributes(taskToExecute.ID, taskToExecute.Type, taskToExecute.Priority, taskToExecute.Attempts, string(task.StatusDead))...))
		failedTask, updateErr := task.MoveToDLQ(metadataContext, p.rdb, rawTask, taskToExecute.ID, err)
		if updateErr != nil {
			tracing.SetError(dlqSpan, updateErr)
			dlqSpan.End()
			p.logger.Printf("worker %d could not move task %s to the dead-letter queue: %v", workerID, taskToExecute.ID, updateErr)
			return
		}
		dlqSpan.SetStatus(codes.Ok, "dead-lettered")
		dlqSpan.End()
		executionSpan.SetAttributes(attribute.String("job.final_status", string(task.StatusDead)))
		p.metrics.Dead()
		logger.JobForWorkerContext(dlqContext, "job_dead", workerID, failedTask, err, time.Since(executionStarted))
		p.logger.Printf("worker %d dead-lettered task %s", workerID, taskToExecute.ID)
		return
	}

	p.jobsDone.Add(1)
	ackContext, ackSpan := tracing.Start(executionContext, "task.acknowledge", oteltrace.WithAttributes(tracing.JobAttributes(taskToExecute.ID, taskToExecute.Type, taskToExecute.Priority, taskToExecute.Attempts, string(task.StatusCompleted))...))
	completedTask, err := task.MarkCompletedAndAcknowledge(metadataContext, p.rdb, rawTask, taskToExecute.ID)
	if err != nil {
		tracing.SetError(ackSpan, err)
		ackSpan.End()
		p.logger.Printf("worker %d could not mark task %s as completed: %v", workerID, taskToExecute.ID, err)
		return
	}
	ackSpan.SetStatus(codes.Ok, "acknowledged")
	ackSpan.End()
	executionSpan.SetAttributes(attribute.String("job.final_status", string(task.StatusCompleted)), attribute.Int64("job.execution_duration_ms", time.Since(executionStarted).Milliseconds()))
	executionSpan.SetStatus(codes.Ok, "completed")
	p.metrics.Completed()
	logger.JobForWorkerContext(ackContext, "job_completed", workerID, completedTask, nil, time.Since(executionStarted))
	p.logger.Printf("worker %d completed task %s", workerID, taskToExecute.ID)
}

func (p *Pool) runRetryScheduler(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(retrySchedulerInterval)
	defer ticker.Stop()

	logger.Event("scheduler_started")
	defer logger.Event("scheduler_stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			moved, err := task.MoveDueRetriesWithCallback(ctx, p.rdb, p.queue, time.Now().UTC(), func(queuedTask task.Task) {
				spanContext := tracing.ContextWithTraceParent(ctx, queuedTask.TraceParent)
				spanContext, span := tracing.Start(spanContext, "task.retry.promote", oteltrace.WithAttributes(tracing.JobAttributes(queuedTask.ID, queuedTask.Type, queuedTask.Priority, queuedTask.Attempts, string(queuedTask.Status))...))
				logger.JobContext(spanContext, "job_retry_promoted", queuedTask, nil)
				span.End()
			})
			if err != nil {
				p.logger.Printf("retry scheduler error: %v", err)
				if !waitForContext(ctx, redisFailureBackoff) {
					return
				}
				continue
			}
			if moved > 0 {
				p.logger.Printf("returned %d retry task(s) to the queue", moved)
			}
		}
	}
}

func (p *Pool) runRecovery(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	interval := p.visibilityTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Event("scheduler_started")
	defer logger.Event("scheduler_stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovered, err := task.RecoverAbandonedWithCallback(ctx, p.rdb, p.queue, p.visibilityTimeout, func(queuedTask task.Task) {
				spanContext := tracing.ContextWithTraceParent(ctx, queuedTask.TraceParent)
				spanContext, span := tracing.Start(spanContext, "task.recover", oteltrace.WithAttributes(tracing.JobAttributes(queuedTask.ID, queuedTask.Type, queuedTask.Priority, queuedTask.Attempts, string(queuedTask.Status))...))
				span.SetAttributes(attribute.String("recovery.reason", "visibility_timeout"), attribute.String("job.final_status", string(task.StatusPending)))
				logger.JobContext(spanContext, "job_recovered", queuedTask, nil)
				span.End()
			})
			if err != nil {
				p.logger.Printf("processing recovery error: %v", err)
				if !waitForContext(ctx, redisFailureBackoff) {
					return
				}
				continue
			}
			if recovered > 0 {
				p.metrics.Recovered(recovered)
				logger.Count("job_recovered", recovered)
				p.logger.Printf("recovered %d abandoned task(s)", recovered)
			}
		}
	}
}

func executeSafely(execute func(task.Task) error, queuedTask task.Task) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("task handler panic: %v", recovered)
		}
	}()
	return execute(queuedTask)
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
