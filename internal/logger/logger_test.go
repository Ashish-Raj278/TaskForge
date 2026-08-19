package logger

import (
	"TaskForge/internal/task"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestJobLogIsJSONWithoutPayload(t *testing.T) {
	var output bytes.Buffer
	restore := SetOutput(&output)
	defer restore()
	Job("job_completed", task.Task{ID: "job-1", Type: "generate_pdf", Payload: map[string]interface{}{"secret": "must-not-log"}, Status: task.StatusCompleted}, nil)
	line := strings.TrimSpace(output.String())
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if event["event"] != "job_completed" || event["job_id"] != "job-1" || strings.Contains(line, "must-not-log") {
		t.Fatalf("unexpected event: %s", line)
	}
}
