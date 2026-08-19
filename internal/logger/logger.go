package logger

import (
	"TaskForge/internal/task"
	"TaskForge/internal/tracing"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

var outputMu sync.Mutex
var output io.Writer = os.Stdout

type event struct {
	Timestamp string      `json:"timestamp"`
	Level     string      `json:"level"`
	Event     string      `json:"event"`
	JobID     string      `json:"job_id,omitempty"`
	JobType   string      `json:"job_type,omitempty"`
	WorkerID  int         `json:"worker_id,omitempty"`
	Status    task.Status `json:"status,omitempty"`
	Priority  int         `json:"priority,omitempty"`
	Attempts  int         `json:"attempts,omitempty"`
	Error     string      `json:"error,omitempty"`
	Count     int         `json:"count,omitempty"`
	Duration  int64       `json:"duration_ms"`
	TraceID   string      `json:"trace_id,omitempty"`
	SpanID    string      `json:"span_id,omitempty"`
}

func Job(name string, queuedTask task.Task, err error) {
	JobContext(context.Background(), name, queuedTask, err)
}

func JobForWorker(name string, workerID int, queuedTask task.Task, err error, duration time.Duration) {
	JobForWorkerContext(context.Background(), name, workerID, queuedTask, err, duration)
}

func JobContext(ctx context.Context, name string, queuedTask task.Task, err error) {
	JobDurationContext(ctx, name, queuedTask, err, 0)
}

func JobForWorkerContext(ctx context.Context, name string, workerID int, queuedTask task.Task, err error, duration time.Duration) {
	e := newJobEvent(ctx, name, queuedTask, err, duration)
	e.WorkerID = workerID
	write(e)
}

func JobDurationContext(ctx context.Context, name string, queuedTask task.Task, err error, duration time.Duration) {
	write(newJobEvent(ctx, name, queuedTask, err, duration))
}

func newJobEvent(ctx context.Context, name string, queuedTask task.Task, err error, duration time.Duration) event {
	traceID, spanID := tracing.SpanIDs(ctx)
	e := event{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Level: "info", Event: name, JobID: queuedTask.ID, JobType: queuedTask.Type, Status: queuedTask.Status, Priority: queuedTask.Priority, Attempts: queuedTask.Attempts, Duration: duration.Milliseconds(), TraceID: traceID, SpanID: spanID}
	if err != nil {
		e.Level = "error"
		e.Error = err.Error()
	}
	return e
}

func JobDuration(name string, queuedTask task.Task, err error, duration time.Duration) {
	JobDurationContext(context.Background(), name, queuedTask, err, duration)
}

func Count(name string, count int) {
	write(event{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Level: "info", Event: name, Count: count})
}

func Event(name string) {
	write(event{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Level: "info", Event: name})
}

// SetOutput changes the event sink and returns a restore function. It is useful for tests.
func SetOutput(w io.Writer) func() {
	outputMu.Lock()
	previous := output
	output = w
	outputMu.Unlock()
	return func() {
		outputMu.Lock()
		output = previous
		outputMu.Unlock()
	}
}

func write(e event) {
	encoded, err := json.Marshal(e)
	if err != nil {
		return
	}
	outputMu.Lock()
	defer outputMu.Unlock()
	_, _ = output.Write(append(encoded, '\n'))
}
