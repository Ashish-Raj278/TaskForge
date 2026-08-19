package task

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusRetrying   Status = "retrying"

	DefaultPriority   = 0
	DefaultMaxRetries = 3
)

type Task struct {
	ID                  string                 `json:"id"`
	Type                string                 `json:"type"`
	Payload             map[string]interface{} `json:"payload"`
	Status              Status                 `json:"status"`
	Priority            int                    `json:"priority"`
	MaxRetries          int                    `json:"max_retries"`
	Attempts            int                    `json:"attempts"`
	CreatedAt           time.Time              `json:"created_at"`
	ScheduledAt         *time.Time             `json:"scheduled_at,omitempty"`
	ProcessingStartedAt *time.Time             `json:"processing_started_at,omitempty"`
}

// NewID creates a randomly generated UUID version 4 without an external dependency.
func NewID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate task ID: %w", err)
	}

	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		hex.EncodeToString(id[0:4]),
		hex.EncodeToString(id[4:6]),
		hex.EncodeToString(id[6:8]),
		hex.EncodeToString(id[8:10]),
		hex.EncodeToString(id[10:16]),
	), nil
}

type Metrics struct {
	Total_jobs_in_queue int64 `json:"total_jobs_in_queue"`
	Jobs_done           int   `json:"jobs_done"`
	Jobs_failed         int   `json:"jobs_failed"`
}
