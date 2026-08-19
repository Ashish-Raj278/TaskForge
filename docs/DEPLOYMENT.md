# TaskForge deployment

## Prerequisites

- Docker Engine with Docker Compose v2 for the container stack.
- Go 1.23+ and Redis 7+ when running services directly on the host.

TaskForge keeps all job metadata and queue state in Redis. The Compose stack
uses a named `redis-data` volume with Redis append-only persistence. Running
`docker compose down` stops the stack but does not delete that volume; only
`docker compose down --volumes` removes it.

## Local container stack

Start Redis, the producer API, and the worker pool:

```sh
docker compose up --build
```

The producer is available at `http://localhost:8080`; the worker observability
server is available at `http://localhost:8081`.

Useful checks:

```sh
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8081/health
curl http://localhost:8081/metrics
```

Submit a job:

```sh
curl -X POST http://localhost:8080/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"type":"generate_pdf","payload":{}}'
```

Stop services cleanly:

```sh
docker compose down
```

## Configuration

| Variable | Service | Default | Purpose |
| --- | --- | --- | --- |
| `REDIS_URL` | producer, worker | required | Redis URL, for example `redis://redis:6379/0`. |
| `PORT_PRODUCER` | producer | `8080` | Producer HTTP listen port. |
| `PORT_WORKER` | worker | `8081` | Worker observability HTTP listen port. |
| `WORKER_COUNT` | worker | `3` | Concurrent worker goroutines; invalid or non-positive values use the default. |
| `TASK_VISIBILITY_TIMEOUT` | worker | `30s` | Lease timeout before abandoned processing jobs are recovered. |
| `TASK_RETRY_BASE_DELAY` | worker | `2s` | First retry delay; subsequent delays use exponential backoff. |
| `OTEL_TRACING_ENABLED` | producer, worker | `false` | Enables OpenTelemetry only when an OTLP endpoint is also configured. |
| `OTEL_SERVICE_NAME` | producer, worker | process-specific | Name reported to the tracing backend. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | producer, worker | empty | OTLP/gRPC endpoint, for example `jaeger:4317`. |
| `OTEL_EXPORTER_OTLP_INSECURE` | producer, worker | `true` | Uses insecure OTLP/gRPC transport for local collectors. |

All values can be supplied through the runtime environment. Do not place Redis
credentials or other secrets in committed Compose overrides; use a secret store
or an untracked environment file in deployment environments.

## Health and readiness

Both services expose `GET /health` and `GET /ready`. They return HTTP 200 only
when Redis is reachable. The producer also exposes job APIs; both services
expose Prometheus text metrics at `GET /metrics`.

## Direct host execution

With Redis running locally:

```sh
REDIS_URL=redis://localhost:6379/0 go run ./cmd/producer
REDIS_URL=redis://localhost:6379/0 WORKER_COUNT=4 go run ./cmd/worker
```

Run the two commands in separate terminals.

## CI

`.github/workflows/ci.yml` runs formatting verification, tests, the Go race
detector, a build, and both Docker image builds on pushes and pull requests.
It installs `build-essential` so the race detector has a C compiler.

## Deployment approach

The Compose definition is the recommended small-scale deployment shape: deploy
it to a single Docker-capable host, expose only the producer port through a
reverse proxy or firewall, and keep the worker metrics port restricted to
operators. For a managed production deployment, run the same producer and
worker images as separate services, provide a managed Redis endpoint through
`REDIS_URL`, and store credentials in that platform's secret manager.

Cloud credentials, a domain, TLS certificates, and a managed Redis service are
environment-specific and are intentionally not included in this repository.

## Optional local tracing

Tracing is disabled by default and has no exporter unless both
`OTEL_TRACING_ENABLED=true` and `OTEL_EXPORTER_OTLP_ENDPOINT` are set. The
Compose `tracing` profile provides Jaeger with OTLP enabled:

```powershell
$env:OTEL_TRACING_ENABLED = "true"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "jaeger:4317"
docker compose --profile tracing up --build
```

Open `http://localhost:16686`, select either `taskforge-producer` or
`taskforge-worker`, and inspect job spans. Stop the optional profile with the
normal `docker compose down` command. Do not expose an unauthenticated Jaeger
UI or insecure OTLP collector directly to the public internet.
