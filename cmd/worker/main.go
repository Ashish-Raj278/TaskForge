package main

import (
	"TaskForge/internal/task"
	"TaskForge/internal/worker"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func connectRedis() *redis.Client {
	redisURL := os.Getenv("REDIS_URL")
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatal("Could not parse Redis URL:", err)
	}
	return redis.NewClient(opt)
}

func main() {
	godotenv.Load()
	rdb := connectRedis()
	workerCount := worker.WorkerCount(os.Getenv("WORKER_COUNT"))
	visibilityTimeout := worker.VisibilityTimeout(os.Getenv("TASK_VISIBILITY_TIMEOUT"))
	retryBaseDelay := worker.RetryBaseDelay(os.Getenv("TASK_RETRY_BASE_DELAY"))
	pool := worker.NewPool(rdb, task.PriorityQueue, workerCount, visibilityTimeout, retryBaseDelay, log.Default())
	workerContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerDone := make(chan struct{})
	go func() { pool.Run(workerContext); close(workerDone) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metricsHandler(pool))
	server := &http.Server{Addr: ":" + os.Getenv("PORT_WORKER"), Handler: mux}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("worker HTTP server error: %v", err)
		}
	}()

	log.Printf("worker pool started with %d workers", workerCount)
	<-workerContext.Done()
	log.Println("worker shutdown requested")
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("worker HTTP server shutdown error: %v", err)
	}
	<-workerDone
	<-serverDone
	if err := rdb.Close(); err != nil {
		log.Printf("Redis close error: %v", err)
	}
	log.Println("worker shutdown complete")
}

func metricsHandler(pool *worker.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET request allowed", http.StatusMethodNotAllowed)
			return
		}
		stats := pool.Stats()
		metrics := task.Metrics{Total_jobs_in_queue: stats.QueueLength, Jobs_done: int(stats.JobsDone), Jobs_failed: int(stats.JobsFailed)}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(metrics); err != nil {
			log.Printf("Could not encode metrics: %v", err)
		}
	}
}
