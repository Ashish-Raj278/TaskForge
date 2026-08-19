package main

import (
	"TaskForge/internal/logger"
	"TaskForge/internal/observability"
	"TaskForge/internal/task"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client
var metrics = observability.NewMetrics()

var ctx = context.Background()

const defaultDLQListLimit int64 = 100

type enqueueRequest struct {
	Type        string                 `json:"type"`
	Payload     map[string]interface{} `json:"payload"`
	Priority    *int                   `json:"priority"`
	MaxRetries  *int                   `json:"max_retries"`
	ScheduledAt *time.Time             `json:"scheduled_at"`
}

func connectRedis() *redis.Client {
	redisURL := os.Getenv("REDIS_URL")
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatal("Could not parse Redis URL:", err)
	}
	rdb := redis.NewClient(opt)
	return rdb
}

func main() {
	var PORT string = ":" + os.Getenv("PORT_PRODUCER")

	godotenv.Load()

	rdb = connectRedis()

	http.HandleFunc("/enqueue", post_handler)
	http.HandleFunc("/jobs/", getJobHandler)
	http.HandleFunc("/dlq", getDLQHandler)
	http.HandleFunc("/dlq/", retryDLQHandler)
	http.HandleFunc("/metrics", observability.MetricsHandler(rdb, metrics))
	http.HandleFunc("/health", observability.HealthHandler(rdb))
	http.HandleFunc("/ready", observability.ReadyHandler(rdb))

	log.Println("Starting the server on port ", PORT)

	err := http.ListenAndServe(PORT, nil)

	if err != nil {
		log.Fatal("error starting the server")
	}

}

func post_handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		fmt.Println("Only POST request accepted")
		http.Error(w, "Only POST request accepted", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain")

	// Read the request body
	var request enqueueRequest
	err := json.NewDecoder(r.Body).Decode(&request)

	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if request.Type == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if request.Type == "send_email" {
		if request.Payload["to"] == nil || request.Payload["subject"] == nil {
			http.Error(w, "Bad request,pass to and subject fields inside the payload", http.StatusBadRequest)
			return
		}
	}

	priority := task.DefaultPriority
	if request.Priority != nil {
		priority = *request.Priority
	}

	maxRetries := task.DefaultMaxRetries
	if request.MaxRetries != nil {
		if *request.MaxRetries < 0 {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		maxRetries = *request.MaxRetries
	}

	id, err := task.NewID()
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	var scheduledAt *time.Time
	if request.ScheduledAt != nil {
		utcScheduledAt := request.ScheduledAt.UTC()
		scheduledAt = &utcScheduledAt
	}

	queuedTask := task.Task{
		ID:          id,
		Type:        request.Type,
		Payload:     request.Payload,
		Status:      task.StatusPending,
		Priority:    priority,
		MaxRetries:  maxRetries,
		Attempts:    0,
		CreatedAt:   time.Now().UTC(),
		ScheduledAt: scheduledAt,
	}

	if queuedTask.ScheduledAt != nil && queuedTask.ScheduledAt.After(time.Now().UTC()) {
		if err := task.StoreAndSchedule(ctx, rdb, queuedTask); err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		metrics.Enqueued()
		logger.Job("job_enqueued", queuedTask, nil)
		fmt.Fprintf(w, "Task of type '%s' has been successfully scheduled", queuedTask.Type)
		return
	}
	queueLength, err := task.StoreAndEnqueue(ctx, rdb, task.PriorityQueue, queuedTask)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	metrics.Enqueued()
	logger.Job("job_enqueued", queuedTask, nil)
	fmt.Println("Length of queue ", queueLength)

	fmt.Fprintf(w, "Task of type '%s' has been successfully added to the queue", queuedTask.Type)

}

func getJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET request accepted", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	queuedTask, err := task.GetMetadata(ctx, rdb, id)
	if errors.Is(err, task.ErrNotFound) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queuedTask); err != nil {
		log.Println("Could not encode job metadata:", err)
	}
}

func getDLQHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET request accepted", http.StatusMethodNotAllowed)
		return
	}

	limit := defaultDLQListLimit
	if requestedLimit := r.URL.Query().Get("limit"); requestedLimit != "" {
		parsedLimit, err := strconv.ParseInt(requestedLimit, 10, 64)
		if err != nil || parsedLimit < 1 || parsedLimit > defaultDLQListLimit {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		limit = parsedLimit
	}

	deadTasks, err := task.ListDeadTasks(ctx, rdb, limit)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(deadTasks); err != nil {
		log.Println("Could not encode dead-letter queue:", err)
	}
}

func retryDLQHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST request accepted", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/dlq/")
	if !strings.HasSuffix(path, "/retry") {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	id := strings.TrimSuffix(path, "/retry")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	replayedTask, err := task.ReplayDeadTask(ctx, rdb, task.PriorityQueue, id)
	if errors.Is(err, task.ErrNotFound) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	if err != nil {
		http.Error(w, "Job is not dead", http.StatusConflict)
		return
	}
	metrics.Replayed()
	logger.Job("job_replayed", replayedTask, nil)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(replayedTask); err != nil {
		log.Println("Could not encode replayed task:", err)
	}
}
