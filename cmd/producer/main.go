package main

import (
	"TaskForge/internal/logger"
	"TaskForge/internal/observability"
	"TaskForge/internal/task"
	"TaskForge/internal/tracing"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var rdb *redis.Client
var metrics = observability.NewMetrics()

var ctx = context.Background()

const (
	defaultDLQListLimit int64 = 100
	maxEnqueueBodyBytes       = 1 << 20
)

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
	godotenv.Load()
	shutdownTracing, tracingErr := tracing.Init(context.Background(), tracing.ConfigFromEnv("taskforge-producer"))
	if tracingErr != nil {
		log.Printf("producer tracing disabled: %v", tracingErr)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Printf("producer tracing shutdown error: %v", err)
		}
	}()
	rdb = connectRedis()
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("producer Redis close error: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", post_handler)
	mux.HandleFunc("/jobs/", getJobHandler)
	mux.HandleFunc("/dlq", getDLQHandler)
	mux.HandleFunc("/dlq/", retryDLQHandler)
	mux.HandleFunc("/metrics", observability.MetricsHandler(rdb, metrics))
	mux.HandleFunc("/health", observability.HealthHandler(rdb))
	mux.HandleFunc("/ready", observability.ReadyHandler(rdb))
	server := &http.Server{
		Addr:              ":" + producerPort(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("producer HTTP server error: %v", err)
		}
	}()

	log.Printf("producer server started on %s", server.Addr)
	<-serverContext.Done()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("producer HTTP server shutdown error: %v", err)
	}
	<-serverDone
}

func producerPort() string {
	if port := os.Getenv("PORT_PRODUCER"); port != "" {
		return port
	}
	return "8080"
}

func post_handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		fmt.Println("Only POST request accepted")
		http.Error(w, "Only POST request accepted", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	spanContext := tracing.ExtractHTTP(r.Context(), r.Header)
	spanContext, span := tracing.Start(spanContext, "task.enqueue", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	defer span.End()
	r.Body = http.MaxBytesReader(w, r.Body, maxEnqueueBodyBytes)
	defer r.Body.Close()

	// Read the request body
	var request enqueueRequest
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&request)

	if err != nil {
		tracing.SetError(span, err)
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		tracing.SetError(span, err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if request.Type == "" {
		tracing.SetError(span, errors.New("task type is required"))
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if request.Type == "send_email" {
		if request.Payload["to"] == nil || request.Payload["subject"] == nil {
			tracing.SetError(span, errors.New("send_email payload is incomplete"))
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
			tracing.SetError(span, errors.New("max_retries must not be negative"))
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		maxRetries = *request.MaxRetries
	}

	id, err := task.NewID()
	if err != nil {
		tracing.SetError(span, err)
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
	queuedTask.TraceParent = tracing.TraceParent(spanContext)
	span.SetAttributes(tracing.JobAttributes(queuedTask.ID, queuedTask.Type, queuedTask.Priority, queuedTask.Attempts, string(queuedTask.Status))...)

	if queuedTask.ScheduledAt != nil && queuedTask.ScheduledAt.After(time.Now().UTC()) {
		if err := task.StoreAndSchedule(ctx, rdb, queuedTask); err != nil {
			tracing.SetError(span, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		metrics.Enqueued()
		span.SetStatus(codes.Ok, "scheduled")
		logger.JobContext(spanContext, "job_enqueued", queuedTask, nil)
		fmt.Fprintf(w, "Task of type '%s' has been successfully scheduled", queuedTask.Type)
		return
	}
	queueLength, err := task.StoreAndEnqueue(ctx, rdb, task.PriorityQueue, queuedTask)
	if err != nil {
		tracing.SetError(span, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	metrics.Enqueued()
	span.SetStatus(codes.Ok, "enqueued")
	logger.JobContext(spanContext, "job_enqueued", queuedTask, nil)
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

	spanContext := tracing.ExtractHTTP(r.Context(), r.Header)
	if queuedTask, err := task.GetMetadata(ctx, rdb, id); err == nil {
		spanContext = tracing.ContextWithTraceParent(spanContext, queuedTask.TraceParent)
	}
	spanContext, span := tracing.Start(spanContext, "task.dlq.replay", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(tracing.JobAttributes(id, "", 0, 0, string(task.StatusPending))...)
	replayedTask, err := task.ReplayDeadTask(ctx, rdb, task.PriorityQueue, id)
	if errors.Is(err, task.ErrNotFound) {
		tracing.SetError(span, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	if err != nil {
		tracing.SetError(span, err)
		http.Error(w, "Job is not dead", http.StatusConflict)
		return
	}
	metrics.Replayed()
	span.SetStatus(codes.Ok, "replayed")
	logger.JobContext(spanContext, "job_replayed", replayedTask, nil)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(replayedTask); err != nil {
		log.Println("Could not encode replayed task:", err)
	}
}
