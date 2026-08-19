# TaskForge

TaskForge is a distributed background-job processing system written in Go and backed by Redis. It accepts HTTP job submissions, persists job metadata, schedules priority-aware work, processes jobs with a concurrent worker pool, and exposes job state and operational metrics.

The project starts with a common backend problem: a request should not have to wait for expensive or unreliable background work such as sending email, generating a PDF, or resizing media. A producer records the job and returns quickly; workers execute the job separately and update durable metadata that the client can inspect.

## Why a job queue?

Moving work out of the request path improves responsiveness and isolates transient task failures from the client request. The difficult parts are not just storing a message: TaskForge must avoid losing a claimed job when a worker stops, prevent concurrent workers from claiming the same job, distinguish worker crashes from application failures, preserve priority, and make state observable.

## Engineering challenges

- Selecting the highest-priority job without allowing two workers to claim it.
- Keeping execution state inspectable after the queue entry has been claimed.
- Recovering jobs whose worker stopped after metadata has entered `processing`.
- Separating application retries from worker-crash recovery.
- Delaying retries and scheduled jobs without holding a worker idle.
- Making concurrent worker lifecycle and queue-state metrics safe to observe.

## Architecture

```mermaid
flowchart TB
    Client[Client] -->|POST /enqueue| Producer[Producer HTTP API]
    Client -->|GET /jobs/{id}, /dlq, /metrics| Producer
    Producer --> Metadata[(task:{jobID})]
    Producer --> Priority[(task_priority_queue)]
    Producer --> Schedule[(task_schedule_queue)]

    subgraph Redis[Redis]
        Priority
        Processing[task_processing]
        Retry[task_retry_queue]
        Schedule
        DLQ[task_dlq]
        Metadata
    end

    subgraph WorkerService[Worker service]
        Pool[Worker pool]
        RetryScheduler[Retry scheduler]
        ScheduleScheduler[Schedule scheduler]
        Recovery[Recovery loop]
    end

    Priority -->|atomic Lua claim| Processing
    Processing --> Pool
    Pool -->|success + ACK| Completed[completed metadata]
    Pool -->|application failure| Retry
    RetryScheduler -->|eligible retry| Priority
    Pool -->|attempts exhausted| DLQ
    DLQ --> Dead[dead metadata]
    ScheduleScheduler -->|due job| Priority
    Processing -->|worker crash / expired lease| Recovery
    Recovery --> Priority
```

`task_processing` is deliberately separate from the pending queue. A task is moved there before execution and removed only by an acknowledgement transition. Once its metadata is marked `processing`, a worker crash can be recovered by returning it to the priority queue.

## Core features

- Server-generated UUIDv4 job IDs and lifecycle-owned metadata.
- Persistent Redis JSON metadata, queryable through `GET /jobs/{id}`.
- Configurable worker pool through `WORKER_COUNT`.
- Strict priority scheduling: higher integer priority runs first; equal priority preserves enqueue order.
- Reliable at-least-once delivery using a processing/in-flight list and recovery loop.
- Retries with exponential backoff in a Redis sorted set, without blocking workers.
- Dead-letter queue (DLQ) for exhausted application failures and explicit replay.
- Future scheduling with `scheduled_at` and a dedicated schedule queue.
- Structured JSON lifecycle logs and Prometheus-compatible metrics.
- Context cancellation, bounded polling, HTTP timeouts, and graceful worker shutdown.

## Technology stack

- Go 1.23+
- Redis 7+ via `github.com/redis/go-redis/v9`
- Standard-library HTTP server and `context`
- Docker and Docker Compose for local deployment
- GitHub Actions for CI

## Redis data model

| Key | Redis type | Purpose and important behavior |
| --- | --- | --- |
| `task_priority_queue` | Sorted set | Pending jobs. Score is `-priority`, so higher priority is selected first. Members contain a zero-padded Redis sequence plus serialized job data, preserving FIFO for equal priorities. |
| `task_priority_queue:sequence` | String integer | Redis monotonic counter used to form deterministic FIFO tie-breakers. |
| `task_processing` | List | In-flight raw task JSON. A job remains here until an atomic terminal/retry acknowledgement succeeds. |
| `task_retry_queue` | Sorted set | Retry job IDs scored by eligible Unix milliseconds. The retry scheduler promotes only due jobs still marked `retrying`. |
| `task_schedule_queue` | Sorted set | Initially scheduled job IDs scored by `scheduled_at` Unix milliseconds. A scheduler promotes due pending jobs to the priority queue. |
| `task_dlq` | List | Index of dead job IDs. Full job metadata remains in `task:{jobID}`. |
| `task:{jobID}` | String | JSON metadata: identity, payload, status, priority, attempts, timestamps, retry information, and most recent error. |

## Job lifecycle and reliability

### Normal execution

```text
pending → task_priority_queue → processing → completed
```

When a worker claims a job, a Lua script atomically removes the highest-priority entry from the sorted set and pushes its serialized task into `task_processing`. Processing starts by updating metadata and incrementing `Attempts`. Completion updates metadata and removes the processing entry atomically.

### Application failure and retry

```text
processing → retrying → task_retry_queue → pending / task_priority_queue → processing
```

For an application error, TaskForge increments `Attempts` when execution begins. If attempts remain, it atomically records `retrying`, removes the in-flight entry, and inserts the job ID into `task_retry_queue`. The delay is:

```text
delay = TASK_RETRY_BASE_DELAY × 2^(attempts - 1)
```

The scheduler later moves an eligible retry back through the normal priority queue. Workers never sleep for retry backoff. `MaxRetries` is the maximum number of execution attempts: with `MaxRetries = 3`, attempts 1 and 2 can retry; failure on attempt 3 is terminal.

### Terminal failure and DLQ

```text
processing → dead → task_dlq
```

When attempts are exhausted, TaskForge atomically updates metadata to `dead`, stores the last useful error, removes the processing entry, and inserts the ID exactly once in `task_dlq`. A dead job is never retried automatically. `POST /dlq/{id}/retry` explicitly resets it to `pending`, clears the error, resets attempts to zero, removes it from the DLQ, and returns it directly to the priority queue.

### Worker crash / abandoned processing

```text
task_processing → expired visibility timeout → pending / task_priority_queue
```

The recovery loop examines processing entries whose metadata is still `processing` and whose lease began before `TASK_VISIBILITY_TIMEOUT`. Its guarded Lua transition returns the job to the priority queue without increasing `Attempts`. This is intentionally different from an application failure.

### Scheduled jobs

```text
pending with future ScheduledAt → task_schedule_queue → due → task_priority_queue → processing
```

`scheduled_at` is an optional RFC3339 timestamp. Future jobs wait in the schedule queue; due or past timestamps enter the priority queue immediately. Metadata remains `pending` while waiting—the queue location represents the scheduled state. Once eligible, priority applies normally. The original `ScheduledAt` is not changed by retry backoff. Equal scheduled timestamps are read in deterministic job-ID order before priority-queue insertion.

### Delivery semantics

TaskForge provides **at-least-once delivery**, not exactly-once execution. If a handler finishes but a worker dies before its completion/acknowledgement transition, recovery can cause a second execution. Handlers should therefore be idempotent where an external side effect matters.

## Priority and concurrency

Priority is an integer: `10` is higher than `5`, `0` is normal, and negative values are allowed. Scheduling is strict priority, so continuous high-priority traffic can starve lower-priority jobs. Fairness/aging is intentionally out of scope.

Workers are independent goroutines sharing one atomic Redis claim path. `WORKER_COUNT` defaults to `3` if missing or invalid. The pool also runs cancellation-aware retry, schedule, and recovery loops. On `SIGINT` or `SIGTERM`, it stops claiming new work, waits for active handlers where practical, stops schedulers and its HTTP server, then closes Redis.

## Observability and hardening

Lifecycle logs are structured JSON events such as `job_enqueued`, `job_claimed`, `job_started`, `job_completed`, `job_retry_scheduled`, `job_dead`, `job_replayed`, and scheduler/worker start-stop events. Logs include IDs and errors where useful but do not include full payloads.

`GET /metrics` exports Prometheus text metrics. Lifecycle counters are concurrency-safe in memory; queue-depth gauges are read from Redis at scrape time and are therefore authoritative for the current queue state. Producer and worker health/readiness endpoints return non-200 when Redis is unavailable.

Production-hardening measures include bounded HTTP request bodies, malformed-JSON validation, HTTP read/write/idle timeouts, environment defaults and validation, Redis error backoff, panic recovery at the task-handler boundary, and idempotent cancellation paths.

## Distributed tracing

TaskForge uses optional OpenTelemetry tracing to answer a different question
from logs and metrics:

- **Metrics:** “What is happening?”
- **Logs:** “What happened?”
- **Traces:** “Where did this particular job spend its time and what happened across its lifecycle?”

Tracing is disabled by default and the system remains fully functional without
an OTLP backend. It activates only when both are set:

```text
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=<host:port>
```

`OTEL_SERVICE_NAME` defaults to `taskforge-producer` or `taskforge-worker`.
`OTEL_EXPORTER_OTLP_INSECURE` defaults to `true` for local OTLP/gRPC collectors.
When disabled, TaskForge installs a no-op provider and creates no exporter.

The producer extracts a client W3C `traceparent` header and starts
`task.enqueue`. It persists only the resulting W3C `traceparent` in
server-owned job metadata; payloads, credentials, and request bodies are never
placed in span attributes or structured logs. The worker restores that parent
for `task.claim` and `task.execute`, with child spans for `task.acknowledge`,
`task.retry.schedule`, `task.retry.promote`, `task.schedule.promote`,
`task.recover`, and `task.dlq.transition`. Trace/span IDs are included in JSON
lifecycle logs when a valid span is active.

Example trace relationships:

```text
task.enqueue
├── task.claim
│   └── task.execute
│       └── task.acknowledge
├── task.retry.schedule
├── task.retry.promote
└── task.execute
    └── task.dlq.transition
```

The asynchronous schedulers create spans from the stored W3C parent when they
promote, recover, or requeue a job. This preserves one trace across retries,
scheduling, and explicit DLQ replay without adding a queue-wide tracing
service. It does not change queue state transitions or delivery semantics.

For a local backend, Compose contains an optional Jaeger profile. It is not
started in the normal stack:

```powershell
$env:OTEL_TRACING_ENABLED = "true"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "jaeger:4317"
docker compose --profile tracing up --build
```

Open `http://localhost:16686` and inspect the `taskforge-producer` and
`taskforge-worker` services. A normal job shows enqueue → claim → execute →
acknowledge; a failure shows execution error plus retry/DLQ spans; a scheduled
job shows schedule promotion before claim. Tracing introduces SDK/exporter
overhead, so Task 12’s tracing-disabled benchmark measurements remain the
authoritative baseline.

## API

The producer listens on `:8080` by default. The worker observability server listens on `:8081` by default.

### `POST /enqueue`

Creates a job. The producer controls ID, status, attempts, and creation timestamp; client input cannot override lifecycle fields.

```json
{
  "type": "generate_pdf",
  "priority": 10,
  "max_retries": 3,
  "scheduled_at": "2026-08-19T15:30:00Z",
  "payload": {}
}
```

`priority`, `max_retries`, and `scheduled_at` are optional. Omitting priority uses `0`; omitting max retries uses `3`. A future `scheduled_at` queues the job for later; a past/due value is eligible immediately.

Success is HTTP `200` with a plain-text confirmation. Important errors: `400` malformed JSON, missing type, invalid retry count, or invalid request shape; `413` body larger than 1 MiB; `500` Redis/internal failure. The confirmation does not include a job ID—use the `job_enqueued` structured log to obtain it for inspection.

### `GET /jobs/{id}`

Returns JSON metadata for a job, including status, attempts, priority, timestamps, and error/retry fields when applicable.

```json
{
  "id": "8f3b...",
  "type": "generate_pdf",
  "status": "completed",
  "priority": 10,
  "max_retries": 3,
  "attempts": 1,
  "created_at": "2026-08-19T10:00:00Z",
  "payload": {}
}
```

Returns `200`, `400` for a malformed ID path, or `404` with `{"error":"job not found"}`.

### `GET /dlq?limit=100`

Lists dead job metadata from the DLQ. `limit` is optional and must be from `1` to `100`. Returns `200`, or `400` for an invalid limit.

### `POST /dlq/{id}/retry`

Explicitly replays one dead job. It resets the job to pending, clears the last error, resets attempts to zero, removes the DLQ entry, and inserts it into the priority queue with its original priority. Returns `200` and the replayed metadata, `404` if missing, or `409` if the job is no longer dead.

### `GET /metrics`

Available on producer and worker. Returns Prometheus text exposition, including lifecycle counters, Redis-backed queue depths, and `taskforge_active_workers`. Returns `503` if Redis cannot be read.

### `GET /health` and `GET /ready`

Available on producer and worker. Both return JSON such as `{"status":"ok","redis":"ok"}` or `{"status":"ready","redis":"ok"}` with HTTP `200` when Redis is reachable; otherwise they return `503`.

Only `GET` is accepted by read-only endpoints and only `POST` by enqueue/replay endpoints; other methods return `405`.

## Local Docker setup

Prerequisites: Docker Engine and Docker Compose v2.

```powershell
docker compose build
docker compose up --build
```

Compose starts three services:

- `redis`: Redis 7 with append-only persistence in the named `redis-data` volume.
- `producer`: the HTTP submission/query API on `http://localhost:8080`.
- `worker`: worker pool and observability API on `http://localhost:8081`.

Redis must pass `redis-cli ping` before producer/worker start. Producer and worker Docker health checks call their existing `/ready` endpoints. The default Compose worker count is `4`; override it before startup, for example `$env:WORKER_COUNT = "1"`.

Stop the stack cleanly without removing persisted Redis data:

```powershell
docker compose down
```

`docker compose down --volumes` also deletes `redis-data` and is intentionally not required for normal shutdown.

Configuration:

| Variable | Default | Used by |
| --- | --- | --- |
| `REDIS_URL` | required outside Compose | producer, worker |
| `PORT_PRODUCER` | `8080` | producer |
| `PORT_WORKER` | `8081` | worker |
| `WORKER_COUNT` | `3` in application / `4` in Compose | worker |
| `TASK_VISIBILITY_TIMEOUT` | `30s` | worker recovery |
| `TASK_RETRY_BASE_DELAY` | `2s` | worker retries |

For direct host execution with local Redis, run `go run ./cmd/producer` and `go run ./cmd/worker` in separate PowerShell terminals with `REDIS_URL=redis://localhost:6379/0` set in each environment.

## Five-minute demo

1. Start the local stack:

   ```powershell
   docker compose up --build -d
   ```

2. Check service health:

   ```powershell
   Invoke-RestMethod http://localhost:8080/health
   Invoke-RestMethod http://localhost:8080/ready
   Invoke-RestMethod http://localhost:8081/ready
   ```

3. Submit a normal job:

   ```powershell
   $body = '{"type":"generate_pdf","priority":5,"payload":{}}'
   Invoke-WebRequest -Method Post -Uri http://localhost:8080/enqueue -ContentType 'application/json' -Body $body
   ```

4. Show the structured lifecycle logs. Copy the `job_id` from `job_enqueued`, then observe the worker claim/start/complete events:

   ```powershell
   docker compose logs producer --tail 30
   docker compose logs worker --tail 50
   ```

5. Inspect the completed job:

   ```powershell
   Invoke-RestMethod http://localhost:8080/jobs/<job-id>
   ```

6. Show metrics:

   ```powershell
   Invoke-WebRequest http://localhost:8081/metrics | Select-Object -ExpandProperty Content
   ```

7. Optional failure/DLQ demonstration: submit `{"type":"unsupported","max_retries":2,"payload":{}}`, get its ID from logs, and watch retry then dead-letter events. Query `http://localhost:8080/dlq`, then replay with:

   ```powershell
   Invoke-RestMethod -Method Post http://localhost:8080/dlq/<job-id>/retry
   ```

8. Optional priority demonstration: run `docker compose stop worker`, enqueue several jobs with distinct `priority` values, set `$env:WORKER_COUNT = "1"`, recreate the worker with `docker compose up -d --force-recreate worker`, and inspect `job_claimed` logs. Higher priorities are claimed first.

9. Explain graceful shutdown with `docker compose down`: the worker receives termination, stops claim/scheduler loops, waits for goroutines, and returns `taskforge_active_workers` to zero after shutdown.

## Performance benchmarks

These are **local Task 12 measurements**, not universal production-capacity guarantees. They used Redis locally, isolated test data, and fixed benchmark batches.

| Workload | Batch | Observed throughput | Average batch-time-per-job |
| --- | ---: | ---: | ---: |
| Concurrent enqueue | 48 jobs | 472.6 jobs/sec | 2,115,931 ns |
| Concurrent priority claims | 48 claims | 745.3 jobs/sec | 1,341,829 ns |
| Retry then DLQ | 16 jobs | 78.92 jobs/sec | 12,671,238 ns |
| Scheduled promotion | 32 jobs | 575.7 jobs/sec | 1,736,994 ns |

Worker-scaling batch, 48 successful jobs:

| Workers | Observed throughput |
| ---: | ---: |
| 1 | 167.8 jobs/sec |
| 4 | 506.7 jobs/sec |
| 8 | 647.5 jobs/sec |

Increasing from one to four workers produced roughly a 3× improvement. Eight workers improved throughput further, but with diminishing returns as Redis operations, synchronization, and local runtime overhead became a larger share. Retry/DLQ work is slower because it performs additional metadata and Redis queue-state transitions.

## Performance engineering

All numbers in this section are **local measurements**, not universal
production-capacity guarantees. Task 12 remains the authoritative unoptimized
baseline above. Task 16 CPU profiles showed that the hot paths were dominated
by Redis/network coordination (`runtime.cgocall` and go-redis I/O), while JSON
work was secondary. The normal successful worker path included an observational
post-claim Redis `ZCARD` in addition to the state-transition operations.

Task 17 removed only that synchronous worker-side `ZCARD`. It did not alter
claiming, metadata transitions, priority, recovery, retries, DLQ, or
acknowledgements. Queue-depth gauges remain Redis-authoritative because
`/metrics` still pipelines fresh Redis `ZCARD`/`LLEN` reads when scraped.

| Benchmark | Task 12 baseline | Task 17 optimized | Change |
| --- | ---: | ---: | ---: |
| Concurrent enqueue, 48 jobs | 472.6 jobs/sec | 484.2 jobs/sec | +2.45% |
| Concurrent priority claims, 48 claims | 745.3 jobs/sec | 505.1 jobs/sec | -32.23% |
| Worker scaling, 1 worker, 48 jobs | 167.8 jobs/sec | 194.5 jobs/sec | +15.91% |
| Worker scaling, 4 workers, 48 jobs | 506.7 jobs/sec | 532.7 jobs/sec | **+5.13%** |
| Worker scaling, 8 workers, 48 jobs | 647.5 jobs/sec | 611.9 jobs/sec | -5.50% |
| Retry/DLQ, 16 jobs | 78.92 jobs/sec | 74.81 jobs/sec | -5.21% |
| Scheduled promotion, 32 jobs | 575.7 jobs/sec | 509.3 jobs/sec | -11.53% |

The four-worker end-to-end result—**506.7 → 532.7 jobs/sec (+5.13%)**—is the
most relevant result because it executes the removed post-claim worker
operation. Concurrent priority claims do not execute that worker-side path;
retry/DLQ and scheduled promotion exercise different paths with extra Redis
state transitions. Their one-run `-benchtime=1x` results, and the other small
local workloads, are susceptible to host and Redis timing variability. They
are not evidence that the targeted optimization changed those paths.

The final engineering decision is to stop here. Removing the unnecessary
round trip is low risk and produced a measurable end-to-end improvement.
Further improvements—such as batching or consolidating metadata and queue
transitions—would touch delivery-state boundaries and carry substantially more
reliability and semantic risk than this measured gain justifies. Those changes
should be designed, profiled, and reviewed as separate reliability work.

## Testing and CI

The project includes unit tests, Redis-backed integration tests, Task 11 concurrent stress tests, scheduling/retry/DLQ coverage, and race-detector validation. Redis-backed tests use isolated logical databases and unique queue keys to avoid test interference.

Validation completed:

```text
go test ./...       PASS
go test -race ./... PASS
go build ./...      PASS
```

Task 13 Docker Compose was also live-verified locally: Redis became healthy, producer and worker started, the worker pool ran with four workers, and a `generate_pdf` job completed with `attempts=1` through producer → Redis → priority queue → worker → metadata API.

GitHub Actions in `.github/workflows/ci.yml` checks formatting, runs Redis-backed tests and the race detector, builds Go binaries, and builds both Docker images. It installs `build-essential` for race detection.

## Engineering tradeoffs

- **Redis:** Redis provides fast queue primitives, sorted sets for priority/time eligibility, and simple metadata access. It is also the central queue/state dependency.
- **At-least-once delivery:** it favors avoiding silent loss of a claimed job over exactly-once complexity. Handlers should be idempotent.
- **Processing list:** an explicit in-flight list makes a worker crash observable and recoverable rather than dropping a claimed job.
- **Lua atomic operations:** priority claims and queue-state transitions use Redis scripts to avoid check-then-act races. The priority source is a sorted set, so TaskForge uses `ZPOPMIN` inside Lua plus `RPUSH`, rather than `BLMOVE`.
- **Scheduled retries:** backoff lives in a sorted set so a worker can process other jobs rather than sleeping.
- **Strict priority:** simple and predictable, but can starve low-priority work under sustained high-priority load.
- **DLQ:** terminal failures are retained for inspection and explicit replay instead of disappearing or retrying forever.
- **Redis-authoritative queue depths:** gauges reflect actual Redis structures at scrape time; lifecycle counters are deliberately process-local.
- **Worker scaling:** more workers increase parallelism until Redis, coordination, or the handler itself becomes the limiting resource.

## Limitations and future work

- Strict priority can starve lower-priority work.
- Task handlers are not context-aware, so an already-running handler is allowed to finish where practical during shutdown.
- Lifecycle counters are process-local; a multi-instance deployment does not aggregate them automatically.
- Redis is the central queue and state dependency.
- Docker live verification is local; cloud credentials, TLS, a domain, and managed Redis are deliberately outside this repository.
- Scheduled jobs with identical due timestamps have deterministic job-ID ordering before entering the priority queue.
- OpenTelemetry tracing is optional and exports asynchronously through OTLP;
  exported spans can be dropped if the configured collector is unavailable or
  the process exits before the batch processor flushes. Tracing does not close
  the documented claim-to-metadata crash window.
- Claiming into `task_processing` and changing metadata to `processing` are separate operations. A worker crash in that narrow interval can leave pending metadata with an in-flight entry that the current recovery guard does not promote. This is a known reliability gap to address in a future atomic state-transition improvement.

## Repository structure

```text
.
├── cmd/
│   ├── producer/            # HTTP submission, job query, DLQ, metrics
│   └── worker/              # worker pool process and observability HTTP server
├── internal/
│   ├── task/                # model, metadata, Redis queue/retry/DLQ/schedule logic
│   ├── worker/              # pool, handlers, schedulers, stress/benchmark tests
│   ├── observability/       # metrics and health/readiness handlers
│   └── logger/              # structured lifecycle logging
├── docs/
│   └── DEPLOYMENT.md        # container and deployment details
├── .github/workflows/ci.yml # CI checks and Docker image builds
├── Dockerfile.producer
├── Dockerfile.worker
├── docker-compose.yml
├── go.mod
└── README.md
```

For deeper deployment guidance, see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).
