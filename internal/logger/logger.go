package logger

import (
	"TaskForge/internal/task"
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
}

func Job(name string, queuedTask task.Task, err error) {
	JobDuration(name, queuedTask, err, 0)
}

func JobForWorker(name string, workerID int, queuedTask task.Task, err error, duration time.Duration) {
	e := event{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Level: "info", Event: name, JobID: queuedTask.ID, JobType: queuedTask.Type, WorkerID: workerID, Status: queuedTask.Status, Priority: queuedTask.Priority, Attempts: queuedTask.Attempts, Duration: duration.Milliseconds()}
	if err != nil {
		e.Level = "error"
		e.Error = err.Error()
	}
	write(e)
}

func JobDuration(name string, queuedTask task.Task, err error, duration time.Duration) {
	e := event{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Level: "info", Event: name, JobID: queuedTask.ID, JobType: queuedTask.Type, Status: queuedTask.Status, Priority: queuedTask.Priority, Attempts: queuedTask.Attempts, Duration: duration.Milliseconds()}
	if err != nil {
		e.Level = "error"
		e.Error = err.Error()
	}
	write(e)
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
