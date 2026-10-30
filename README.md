# Network & Service Health Monitor

[![CI](https://github.com/adoherty44/service-health-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/adoherty44/service-health-monitor/actions/workflows/ci.yml)

An asynchronous uptime and latency monitor with a built-in **chaos engine**. It polls websites and APIs on a schedule, stores every result in PostgreSQL, rolls history into daily aggregates, and exposes a REST API. The chaos engine can fake outages on demand, so detection and recovery can be demonstrated without breaking anything real.

**Stack:** Python 3.12 · FastAPI · SQLAlchemy 2.0 (async) · PostgreSQL 16 · Alembic · httpx · Docker Compose · pytest · GitHub Actions

**Status:** the backend is complete and tested (96 tests, 98% coverage, enforced at 90% in CI). The dashboard (TypeScript) is built against the API documented in [section 6](#6-frontend-integration-guide).

## Contents

1. [Features](#1-features)
2. [Quick start](#2-quick-start)
3. [Architecture](#3-architecture)
4. [How it works](#4-how-it-works)
5. [Data model](#5-data-model)
6. [Frontend integration guide](#6-frontend-integration-guide) *(start here if you are building the UI)*
7. [API reference](#7-api-reference)
8. [Configuration](#8-configuration)
9. [Testing and quality](#9-testing-and-quality)
10. [Design decisions](#10-design-decisions)
11. [Known limitations and roadmap](#11-known-limitations-and-roadmap)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Features

- **Periodic health checks** with per-target intervals (5 seconds to 24 hours), sub-millisecond latency capture, and a hard timeout.
- **Uptime history**: raw check logs plus 30 days of daily rollups (uptime %, avg/min/max/p95 latency).
- **Analytics API**: latency percentiles (p50/p95/p99), uptime %, and bucketed time series, all computed inside PostgreSQL.
- **Chaos engine**: inject `TIMEOUT`, `500_ERROR`, or `DNS_FAILURE` on any target, optionally self-healing after N seconds.
- **REST API** with interactive docs, keyset pagination, validation, and consistent errors.
- **Demo mode**: first start seeds 3 targets and 7 days of realistic history so the UI is never empty.
- **Production habits**: migrations, health/readiness probes, graceful shutdown, non-root container, CI with a coverage gate.

---

## 2. Quick start

**Prerequisites:** Docker Desktop. For local development you also need Python 3.12 or newer.

### Option A: everything in Docker (recommended for demos)

```bash
git clone https://github.com/adoherty44/service-health-monitor.git
cd service-health-monitor
docker compose up -d --build
```

Wait about 20 seconds, then open:

| URL | What |
|---|---|
| http://localhost:8000/docs | Interactive API explorer (Swagger UI) |
| http://localhost:8000/redoc | Read-only API docs |
| http://localhost:8000/openapi.json | Machine-readable spec (use it to generate TypeScript types) |
| http://localhost:8000/ready | Readiness probe (checks the database) |

Stop with `docker compose down` (keeps data) or `docker compose down -v` (**deletes** the database).

### Option B: local development with auto-reload

```bash
docker compose up -d db                      # database only, on localhost:5433
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
alembic upgrade head                         # create tables
uvicorn app.main:app --reload --reload-dir app --port 8000
```

**Never run the Docker `api` service and a local `uvicorn` at the same time:** both use port 8000. Use `docker compose stop api` first.

### Everyday commands

| I want to... | Command |
|---|---|
| Run the tests | `docker compose up -d db`, then `cd backend && pytest --cov=app` |
| Lint and format | `cd backend && ruff check --fix . && ruff format .` |
| Reset the demo data | `cd backend && python -m scripts.reset_demo --yes` (**deletes all targets and history**) |
| Create a migration after changing `models.py` | `cd backend && alembic revision --autogenerate -m "describe change"` then `alembic upgrade head` |
| Watch the API logs in Docker | `docker compose logs -f api` |

> After closing a terminal, remember `source .venv/bin/activate` before any Python command. Docker commands never need it.

### Try the chaos engine in 30 seconds

```bash
# Pick the first target (the API's own /health endpoint)
ID=$(curl -s localhost:8000/api/targets | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")

# Break it for 60 seconds, then force a check immediately
curl -s -X POST localhost:8000/api/targets/$ID/faults/inject \
  -H 'content-type: application/json' \
  -d '{"fault_type":"500_ERROR","duration_seconds":60}'
curl -s -X POST localhost:8000/api/targets/$ID/check

# See it flagged DOWN; after 60s the next scheduled check recovers it on its own
curl -s localhost:8000/api/overview | python3 -m json.tool | head -30
```

---

## 3. Architecture

```mermaid
flowchart LR
    UI["Frontend (browser)"] -->|"REST / JSON"| API

    subgraph proc["FastAPI process (exactly one worker)"]
        API["routes.py<br/>REST endpoints"]
        Sim["simulation.py<br/>INJECTED_FAULTS (in memory)"]
        Sched["scheduler.py<br/>one asyncio task per target<br/>+ maintenance loop"]
        API -->|"inject / clear"| Sim
        Sched -->|"is a fault active?"| Sim
        API -->|"sync targets, check now"| Sched
    end

    API --> DB[("PostgreSQL 16")]
    Sched --> DB
    Sched -->|"httpx GET"| Sites["Monitored sites and APIs"]
```

**Why exactly one worker?** Fault-injection state lives in this process's memory, and every worker would run its own scheduler (duplicating every check). Do not use `uvicorn --workers N`. See [limitations](#11-known-limitations-and-roadmap) for the scale-out path.

### Docker Compose services

| Service | Role |
|---|---|
| `db` | PostgreSQL 16, data on a named volume, published on host port **5433** (not 5432, to avoid clashing with a local Postgres) |
| `migrate` | One-shot job: runs `alembic upgrade head` then exits. The API waits for it to succeed |
| `api` | The FastAPI app plus scheduler on port 8000, with a health check against `/ready` |

### Project layout

```
service-health-monitor/
├── .github/
│   ├── workflows/ci.yml            # lint, tests (Py 3.12 + 3.14), Docker smoke test
│   └── dependabot.yml
├── docker-compose.yml
├── README.md
└── backend/
    ├── app/
    │   ├── models.py               # DB schema: targets, health_logs, uptime_daily_aggregates
    │   ├── scheduler.py            # background checks, daily rollups, retention
    │   ├── simulation.py           # chaos engine (INJECTED_FAULTS)
    │   ├── routes.py               # REST API
    │   ├── schemas.py              # Pydantic request/response models (the API contract)
    │   ├── database.py             # async engine and session factory
    │   ├── config.py               # typed settings from environment variables
    │   ├── seed.py                 # demo targets and backfilled history
    │   └── main.py                 # app wiring, lifespan, CORS, health probes
    ├── migrations/                 # Alembic
    ├── scripts/reset_demo.py
    ├── tests/                      # 96 tests against real PostgreSQL
    ├── Dockerfile, .dockerignore
    ├── pyproject.toml              # ruff config
    ├── pytest.ini, .coveragerc
    ├── requirements.txt, requirements-dev.txt
    └── .env.example
```

Dependency direction: `routes → scheduler → simulation`, everything → `models`. No cycles. `simulation.py` knows nothing about HTTP or the database.

---

## 4. How it works

### A check, step by step

For each target, the scheduler runs a loop like this:

1. Wait a small random delay (at most `min(interval, 5s)`) so many targets do not all fire at once.
2. **Ask the chaos engine** whether a fault is active for this target. If so, fabricate a failure and **never touch the network**.
3. Otherwise send `GET <url>` using one shared `httpx.AsyncClient`, timed with `time.perf_counter()`. The latency is time until **response headers arrive**; bodies are never downloaded.
4. Classify the result: status below 400 is healthy; HTTP error, timeout, DNS failure, and other connection errors are unhealthy with a message.
5. Insert one `health_logs` row.
6. Sleep until the next tick. The schedule is drift-corrected, and if a check overruns its interval the loop waits one full interval rather than bursting to catch up.

Safety rails: a hard wall-clock deadline per check (`asyncio.timeout`), a semaphore capping concurrent requests (default 50), and a try/except around every iteration so one bad check cannot kill a target's loop.

### Keeping the scheduler in sync

The API calls `scheduler.sync_targets()` after every create, update, and delete, so changes take effect immediately (editing a target's URL or interval restarts its loop). A maintenance loop repeats this reconciliation every 5 minutes as a safety net.

### The chaos engine

`INJECTED_FAULTS` maps `target_id` to a fault type. Every fabricated message starts with `[SIMULATED] ` so drills are always distinguishable from real incidents.

| Fault | `status_code` | `response_time_ms` | `error_message` |
|---|---|---|---|
| `TIMEOUT` | `null` | about the configured timeout (default 10,000), fabricated instantly | `[SIMULATED] Request timed out after 10s` |
| `500_ERROR` | `500` | 15 to 120 | `[SIMULATED] HTTP 500 Internal Server Error` |
| `DNS_FAILURE` | `null` | 2 to 40 | `[SIMULATED] DNS resolution failed: Name or service not known` |

Faults can carry `duration_seconds` and then heal themselves; expiry is evaluated lazily when the scheduler next asks, so no timers are needed. Faults are in memory only: they are lost when the server restarts.

### Rollups and retention

Every 5 minutes the maintenance loop:

1. **Rolls up** today and yesterday into `uptime_daily_aggregates` with a single `INSERT ... SELECT ... ON CONFLICT DO UPDATE`, using PostgreSQL's `percentile_cont` for p95 and `FILTER` clauses so latency statistics cover **healthy checks only**.
2. **Prunes** raw logs older than 14 days and aggregates older than 30 days.

Two details protect correctness: raw-log pruning is aligned to UTC midnight (a day is fully present or fully gone), and rolling up a day with no raw logs is a no-op. Together, history in the aggregate table can never be overwritten with partial data or zeros.

### Demo data

On first start with an empty database, `seed.py` creates three targets (this API's own `/health`, example.com, wikipedia.org) and backfills 7 days of history at one point per minute: daily latency rhythm, noise, slow patches, and a few outages. **This history is synthetic**, and its failures carry the `[SIMULATED]` label. Disable with `SEED_DEMO_TARGETS=false`.

---

## 5. Data model

```mermaid
erDiagram
    targets ||--o{ health_logs : "has"
    targets ||--o{ uptime_daily_aggregates : "has"

    targets {
        int id PK
        string name UK
        string url
        int check_interval "seconds"
        timestamptz created_at
    }
    health_logs {
        bigint id PK
        int target_id FK
        timestamptz timestamp
        float response_time_ms
        bool is_healthy
        text error_message "nullable"
        int status_code "nullable"
    }
    uptime_daily_aggregates {
        int id PK
        int target_id FK
        date day "UTC day, unique with target_id"
        int total_checks
        int healthy_checks
        float uptime_percentage
        float avg_response_time_ms "healthy only"
        float min_response_time_ms "healthy only"
        float max_response_time_ms "healthy only"
        float p95_response_time_ms "healthy only"
        timestamptz updated_at
    }
```

| Index | Purpose |
|---|---|
| `health_logs (target_id, timestamp DESC)` | The hot path: latest checks and time-window queries |
| `health_logs (target_id, timestamp DESC) WHERE is_healthy = false` | Partial index: tiny, makes incident queries fast |
| `uptime_daily_aggregates (target_id, day DESC)` and unique `(target_id, day)` | 30-day lookups and idempotent upserts |

Foreign keys use `ON DELETE CASCADE`: deleting a target removes its logs and aggregates.

---

## 6. Frontend integration guide

Everything a UI developer needs to connect to this backend.

### 6.1 Connection basics

| Topic | Detail |
|---|---|
| **Base URL** | `http://localhost:8000` (all data endpoints live under `/api`) |
| **Format** | JSON in and out. Send `Content-Type: application/json` on `POST`/`PATCH` |
| **Auth** | None. The API is open (see [limitations](#11-known-limitations-and-roadmap)) |
| **CORS** | Browser calls are allowed from `http://localhost:5173` (Vite) and `http://localhost:3000` (CRA/Next). Credentials/cookies are not enabled |
| **Other origin?** | Set `CORS_ORIGINS` (comma-separated) on the API, for example `CORS_ORIGINS=http://localhost:4200`, then restart it. In Docker, edit it under `api.environment` in `docker-compose.yml` |
| **Timestamps** | ISO-8601 in **UTC**, always ending in `Z`: `2026-10-30T19:09:58.595137Z`. Parse with `new Date(s)`. Send query timestamps as `date.toISOString()` |
| **IDs** | Integers |
| **Numbers** | Latencies are milliseconds (floats). Percentages are 0 to 100 (not 0 to 1) |
| **Errors** | `{"detail": ...}`. See [error handling](#68-error-handling) |
| **Live docs** | http://localhost:8000/docs lets you try every call |

**Avoiding CORS entirely in development:** proxy through Vite.

```ts
// vite.config.ts
import { defineConfig } from "vite";
export default defineConfig({
  server: { proxy: { "/api": "http://localhost:8000" } },
});
// then call fetch("/api/overview") with a relative URL
```

**Generating TypeScript types from the live spec** (optional but recommended, keeps the UI in lockstep with the backend):

```bash
npx openapi-typescript http://localhost:8000/openapi.json -o src/api/schema.d.ts
```

### 6.2 TypeScript types

Hand-written equivalents of the API models, if you prefer not to generate them:

```ts
export type FaultType = "TIMEOUT" | "500_ERROR" | "DNS_FAILURE";
export type TargetStatus = "UP" | "DOWN" | "UNKNOWN";

export interface Fault {
  target_id: number;
  fault_type: FaultType;
  injected_at: string;          // ISO UTC
  expires_at: string | null;    // null = until cleared
}

export interface Target {
  id: number;
  name: string;
  url: string;
  check_interval: number;       // seconds
  created_at: string;
  active_fault: Fault | null;
}

export interface TargetCreate { name: string; url: string; check_interval?: number } // default 30
export type TargetUpdate = Partial<TargetCreate>;

export interface HealthLog {
  id: number;
  target_id: number;
  timestamp: string;
  response_time_ms: number;
  is_healthy: boolean;
  status_code: number | null;
  error_message: string | null; // starts with "[SIMULATED] " for injected faults
}

export interface HealthLogPage { items: HealthLog[]; next_cursor: string | null }

export interface OverviewItem {
  target: Target;
  status: TargetStatus;
  latest_check: HealthLog | null;
  checks_24h: number;
  uptime_24h: number | null;    // percent, null when no checks in the last 24h
}

export interface AnalyticsSummary {
  total_checks: number;
  healthy_checks: number;
  failed_checks: number;
  uptime_percentage: number | null;      // null when total_checks = 0
  // Latency fields cover HEALTHY checks only, and are null if there were none
  avg_response_time_ms: number | null;
  min_response_time_ms: number | null;
  max_response_time_ms: number | null;
  p50_response_time_ms: number | null;
  p95_response_time_ms: number | null;
  p99_response_time_ms: number | null;
}

export interface AnalyticsBucket {
  bucket_start: string;
  total_checks: number;
  healthy_checks: number;
  uptime_percentage: number;
  avg_response_time_ms: number | null;   // null if the bucket had no healthy checks
  max_response_time_ms: number | null;
}

export interface Analytics {
  target_id: number;
  start: string;
  end: string;
  bucket_seconds: number;
  summary: AnalyticsSummary;
  buckets: AnalyticsBucket[];
}

export interface DailyAggregate {
  target_id: number;
  day: string;                           // "YYYY-MM-DD" (UTC day)
  total_checks: number;
  healthy_checks: number;
  uptime_percentage: number;
  avg_response_time_ms: number | null;
  min_response_time_ms: number | null;
  max_response_time_ms: number | null;
  p95_response_time_ms: number | null;
}
```

### 6.3 Endpoint cheat sheet

| Need | Call |
|---|---|
| Dashboard grid (status, latest check, 24h uptime, active fault) | `GET /api/overview` |
| Target detail: latency graph and summary numbers | `GET /api/targets/{id}/analytics?start=&end=` |
| 30-day uptime bars | `GET /api/targets/{id}/aggregates` |
| Incident list (failed checks, newest first) | `GET /api/targets/{id}/logs?healthy=false` |
| Raw check history table with "load more" | `GET /api/targets/{id}/logs?limit=50&cursor=...` |
| Add / edit / remove a target | `POST /api/targets`, `PATCH /api/targets/{id}`, `DELETE /api/targets/{id}` |
| Chaos panel: start outage | `POST /api/targets/{id}/faults/inject` |
| Chaos panel: end outage | `POST /api/targets/{id}/faults/clear` |
| Show all active outages | `GET /api/faults` (or read `active_fault` from the overview) |
| "Check now" button | `POST /api/targets/{id}/check` |
| Health indicator for the API itself | `GET /health` (alive) and `GET /ready` (database OK) |

### 6.4 Screen recipes

**Dashboard (landing page).** One call, `GET /api/overview`, returns everything for the status grid. Color cards by `status` (`UP` green, `DOWN` red, `UNKNOWN` grey for brand-new targets). Show `uptime_24h` as a percentage and `latest_check.response_time_ms` as the current latency. If `target.active_fault` is non-null, show a "chaos" badge with the fault type and, if `expires_at` is set, a countdown.

**Target detail: latency chart.** Call `GET /api/targets/{id}/analytics` with a window (default is the last 24 hours; maximum 31 days). Plot `buckets[].avg_response_time_ms` against `bucket_start`. The response includes `bucket_seconds`, which you can use for axis labels and tooltips. Show `summary` as stat tiles (uptime %, p50/p95/p99, max).

```ts
const end = new Date();
const start = new Date(end.getTime() - 6 * 3600 * 1000);          // last 6 hours
const qs = new URLSearchParams({ start: start.toISOString(), end: end.toISOString() });
const data = await api<Analytics>(`/targets/${id}/analytics?${qs}`);

const points = data.buckets.map(b => ({
  x: new Date(b.bucket_start),
  y: b.avg_response_time_ms,            // null => draw a gap, not a zero
  degraded: b.uptime_percentage < 100,  // e.g. tint red
}));
```

**Uptime history (30-day bars).** `GET /api/targets/{id}/aggregates` returns one entry per UTC day, oldest first. Days with no data are simply absent, so build a 30-day calendar and fill in what exists.

**Incidents.** `GET /api/targets/{id}/logs?healthy=false` gives failed checks newest first. Group consecutive failures client-side into incidents (start, end, duration, message). Show a "simulated" tag when `error_message` starts with `[SIMULATED]`.

**Chaos panel.** A dropdown for the three fault types, an optional duration, and an Inject button, plus Clear. **After injecting, also call `POST /api/targets/{id}/check`**: otherwise the dashboard will not change until the target's next scheduled check (up to `check_interval` seconds later), which feels broken in a live demo. Then refetch `/api/overview`.

**Target management.** Standard forms against `POST`/`PATCH`/`DELETE /api/targets`. Names must be unique (409 otherwise). Intervals are 5 to 86,400 seconds. Deleting a target also deletes its history, so confirm in the UI.

### 6.5 Polling strategy

The API has no WebSocket or push channel yet, so poll:

| Data | Suggested refresh |
|---|---|
| `/api/overview` | every 5 to 10 seconds |
| `/api/targets/{id}/analytics` (open detail view only) | every 15 to 30 seconds |
| `/api/targets/{id}/aggregates` | on page load (it changes slowly) |
| Chaos action, then `/check`, then `/overview` | immediately after the user acts |

With TanStack Query:

```ts
export const useOverview = () =>
  useQuery({
    queryKey: ["overview"],
    queryFn: () => api<OverviewItem[]>("/overview"),
    refetchInterval: 5_000,
  });
```

### 6.6 A small fetch wrapper

```ts
const BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000"; // use "" with the Vite proxy

export class ApiError extends Error {
  constructor(public status: number, public detail: unknown) {
    super(typeof detail === "string" ? detail : "Request failed");
  }
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (res.status === 204) return undefined as T;            // DELETE returns no body
  const body = await res.json().catch(() => null);
  if (!res.ok) throw new ApiError(res.status, body?.detail ?? res.statusText);
  return body as T;
}

// usage
await api<Target>("/targets", { method: "POST", body: JSON.stringify({ name: "My API", url: "https://example.com" }) });
await api<Fault>(`/targets/${id}/faults/inject`, { method: "POST", body: JSON.stringify({ fault_type: "TIMEOUT", duration_seconds: 60 }) });
```

### 6.7 Gotchas worth knowing up front

- **Empty buckets are omitted.** A gap in `buckets` means "no data", not zero. Detect gaps by comparing consecutive `bucket_start` values against `bucket_seconds`.
- **Latency stats ignore failures.** `avg`, `min`, `max`, and percentiles are computed over healthy checks only, so a fully failed bucket has `avg_response_time_ms: null`. Failed checks still count toward `uptime_percentage`.
- **Simulated outages count.** Injected faults are stored like real failures, so they lower `uptime_24h`. That is intentional (the demo shows the impact), but label them using the `[SIMULATED]` prefix.
- **Seeded history is synthetic.** The first 7 days of demo data are generated; real data begins when the server starts.
- **`active_fault` is ephemeral.** Faults live in server memory and vanish on restart or expiry.
- **Reaction time depends on `check_interval`.** Seeded targets check every 30 or 60 seconds. Use `POST .../check` for instant feedback.
- **Cursors are opaque.** Pass `next_cursor` back unchanged. `next_cursor: null` means the last page. There is no total count and no page numbers (keyset pagination).
- **Window limits.** `start`/`end` windows are capped at 31 days. Timestamps without a timezone are treated as UTC. Use `/aggregates` for longer history.
- **`id` ordering.** Lists are ordered by id; logs are newest first.

### 6.8 Error handling

| Status | When | Body |
|---|---|---|
| `404` | Unknown target id | `{"detail": "Target 999 not found"}` |
| `409` | Target name already exists | `{"detail": "A target with this name already exists"}` |
| `422` | Validation failure | **Either** `{"detail": "start must be earlier than end"}` (a string) **or** `{"detail": [{"loc": ["body","check_interval"], "msg": "Input should be greater than or equal to 5", "type": "greater_than_equal", ...}]}` (an array) |
| `503` | `/ready` when the database is down | `{"status": "unavailable"}` |

**`detail` can be a string or an array**, so handle both. For form validation, map each array entry's `loc` (for example `["body", "url"]`) to the field named by its last element.

---

## 7. API reference

All paths are under `/api` unless noted. Dates are ISO-8601 UTC.

### Targets

| Method | Path | Body | Success |
|---|---|---|---|
| `POST` | `/targets` | `{name, url, check_interval?}` | `201` Target |
| `GET` | `/targets` | | `200` Target[] |
| `GET` | `/targets/{id}` | | `200` Target |
| `PATCH` | `/targets/{id}` | any of `name`, `url`, `check_interval` | `200` Target |
| `DELETE` | `/targets/{id}` | | `204` (cascades logs, aggregates, and clears any fault) |
| `POST` | `/targets/{id}/check` | | `200` HealthLog (runs and stores one check now) |

Validation: `name` 1 to 100 characters (whitespace trimmed, must be unique); `url` must be `http` or `https` (normalised, so `https://example.com` becomes `https://example.com/`); `check_interval` 5 to 86,400 (default 30). `PATCH` leaves omitted or null fields unchanged and restarts the target's monitor loop.

### History and analytics

| Method | Path | Query parameters | Success |
|---|---|---|---|
| `GET` | `/overview` | | `200` OverviewItem[] |
| `GET` | `/targets/{id}/logs` | `start`, `end`, `healthy` (bool), `limit` (1 to 1000, default 100), `cursor` | `200` `{items, next_cursor}` |
| `GET` | `/targets/{id}/analytics` | `start`, `end`, `bucket_seconds` (1 to 86400) | `200` Analytics |
| `GET` | `/targets/{id}/aggregates` | `days` (1 to 30, default 30) | `200` DailyAggregate[] (oldest first) |

Window rules for `logs` and `analytics`: `end` defaults to now, `start` to `end - 24h`, `start` must be before `end`, and the span is at most 31 days. If `bucket_seconds` is omitted it is chosen automatically (about 200 points, from 10 seconds up to 1 day); asking for more than 5,000 buckets returns `422`.

### Simulation

| Method | Path | Body | Success |
|---|---|---|---|
| `GET` | `/faults` | | `200` Fault[] |
| `POST` | `/targets/{id}/faults/inject` | `{fault_type, duration_seconds?}` (1 to 86,400) | `200` Fault |
| `POST` | `/targets/{id}/faults/clear` | | `200` `{target_id, cleared}` (`cleared` is `false` if none was active) |

### System (no `/api` prefix)

| Method | Path | Response |
|---|---|---|
| `GET` | `/health` | `{"status":"ok"}` (process is alive; does not touch the database) |
| `GET` | `/ready` | `{"status":"ready"}`, or `503 {"status":"unavailable"}` if the database is unreachable |

### Sample responses

`GET /api/overview` (abridged)

```json
[
  {
    "target": {
      "id": 6,
      "name": "Monitor API (self-check)",
      "url": "http://127.0.0.1:8000/health",
      "check_interval": 30,
      "created_at": "2026-10-26T01:33:49.621352Z",
      "active_fault": {
        "target_id": 6,
        "fault_type": "500_ERROR",
        "injected_at": "2026-10-26T01:38:22.081857Z",
        "expires_at": "2026-10-26T01:39:22.081857Z"
      }
    },
    "status": "DOWN",
    "latest_check": {
      "id": 30270,
      "target_id": 6,
      "timestamp": "2026-10-26T01:38:22.125020Z",
      "response_time_ms": 111.644,
      "is_healthy": false,
      "status_code": 500,
      "error_message": "[SIMULATED] HTTP 500 Internal Server Error"
    },
    "checks_24h": 1444,
    "uptime_24h": 99.9307
  }
]
```

`GET /api/targets/6/analytics` (abridged; values are illustrative)

```json
{
  "target_id": 6,
  "start": "2026-10-25T19:00:00Z",
  "end": "2026-10-26T19:00:00Z",
  "bucket_seconds": 900,
  "summary": {
    "total_checks": 1444, "healthy_checks": 1443, "failed_checks": 1,
    "uptime_percentage": 99.9307,
    "avg_response_time_ms": 6.412, "min_response_time_ms": 3.1, "max_response_time_ms": 61.9,
    "p50_response_time_ms": 5.9, "p95_response_time_ms": 10.8, "p99_response_time_ms": 24.3
  },
  "buckets": [
    { "bucket_start": "2026-10-25T19:00:00Z", "total_checks": 15, "healthy_checks": 15,
      "uptime_percentage": 100.0, "avg_response_time_ms": 6.1, "max_response_time_ms": 9.8 }
  ]
}
```

`GET /api/targets/6/logs?limit=2` and the next page

```json
{ "items": [ { "id": 30270, "...": "..." }, { "id": 30269, "...": "..." } ],
  "next_cursor": "MjAyNi0xMC0yNlQwMTozODoyMi4xMjUwMjArMDA6MDB8MzAyNzA=" }
```

Then `GET /api/targets/6/logs?limit=2&cursor=<that value>`.

---

## 8. Configuration

All settings are environment variables (or a `.env` file in `backend/`). Real environment variables take priority over `.env`. Defaults work out of the box.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://monitor:monitor@localhost:5433/monitor` | Must use the `+asyncpg` driver |
| `REQUEST_TIMEOUT_SECONDS` | `10` | Hard deadline per check; also the simulated `TIMEOUT` latency |
| `MAX_CONCURRENT_CHECKS` | `50` | Outbound request concurrency cap |
| `MIN_CHECK_INTERVAL_SECONDS` / `MAX_CHECK_INTERVAL_SECONDS` | `5` / `86400` | Bounds the API enforces |
| `SCHEDULER_INITIAL_JITTER_SECONDS` | `5` | Max random delay before a target's first check |
| `RAW_LOG_RETENTION_DAYS` | `14` | Minimum 2 |
| `AGGREGATE_RETENTION_DAYS` | `30` | |
| `MAINTENANCE_INTERVAL_SECONDS` | `300` | Rollup, prune, and reconcile cadence |
| `SEED_DEMO_TARGETS` | `true` | Seed demo data into an empty database |
| `SEED_HISTORY_DAYS` | `7` | 0 to 14 |
| `CORS_ORIGINS` | `http://localhost:5173,http://localhost:3000` | Comma-separated allow-list |
| `LOG_LEVEL` | `INFO` | |
| `SQL_ECHO` | `false` | Log every SQL statement |

---

## 9. Testing and quality

```bash
docker compose up -d db
cd backend
pytest --cov=app          # 96 tests, about 20 seconds, fails if coverage < 90%
ruff check . && ruff format --check .
```

- **Real PostgreSQL, not mocks.** The code uses `FILTER`, `percentile_cont`, `date_bin`, `LATERAL` joins, partial indexes, and `ON CONFLICT`, none of which SQLite can run. Tests use a separate `monitor_test` database that is created automatically and **wiped on every run**; the suite refuses to start unless the database name ends in `_test`.
- **Migrations are tested.** The test schema is built by running the real Alembic migration.
- **16 unit tests need no database** (the chaos engine and error classification), and run in milliseconds.
- **Stub server.** Integration tests monitor a local HTTP server with `/ok`, `/fail` (503), and `/slow` (3 seconds) endpoints, and assert that **no real request is made while a fault is active**.
- **Coverage** is about 98% and gated at 90%. `.coveragerc` sets `concurrency = greenlet` because SQLAlchemy's async layer runs on greenlets and coverage would otherwise under-report badly.
- **CI** (`.github/workflows/ci.yml`) runs on every push and pull request: lint, format check, and the full suite on Python 3.12 and 3.14 against a PostgreSQL service container; then builds the Docker stack and runs a smoke test that injects an outage, confirms detection, clears it, and confirms recovery.

---

## 10. Design decisions

The choices made while building, with the main alternative considered for each. Useful context for reviewers and future contributors.

| Area | Decision | Alternative considered |
|---|---|---|
| Database | PostgreSQL 16 in Docker | SQLite (cannot run the SQL features used here) |
| Timestamps | `TIMESTAMPTZ`, always UTC | Naive timestamps (a classic source of bugs) |
| Log primary key | `BIGINT` on `health_logs` | 32-bit `INTEGER` (ceiling is about 2.1 billion rows) |
| History | Real `uptime_daily_aggregates` table, upserted by a job | Materialized view, or computing from raw logs on every read |
| Extra index | Partial index on failed checks | Only the required composite index |
| Migrations | Alembic, run by a one-shot Compose service | `create_all()` at startup; migrating inside the API container (races with multiple instances) |
| Scheduling | One asyncio task per target | A single 1-second tick loop; APScheduler/Celery |
| Latency measurement | Time to response headers (`httpx` streaming) | Full body download |
| Writes | Commit each check immediately | Buffered bulk inserts (better at thousands of checks/second) |
| Fault state | In-memory dict | A Postgres table or Redis (needed for multi-process scale) |
| Rollups | One SQL statement with `percentile_cont` and `FILTER` | Pull rows into Python and compute there |
| Analytics | Computed in SQL (`date_bin`, `FILTER`) | Aggregate in Python (memory grows with data) |
| Pagination | Keyset/cursor on `(timestamp, id)` | Offset/limit (deep pages get slow) |
| Overview query | `LATERAL` join (one index seek per target) | `DISTINCT ON` (scans every row) |
| Container | Multi-stage build, non-root user, `python:3.12-slim` | Alpine (musl compile issues); a single-stage image |
| Coverage | 90% floor enforced in CI | Chasing 100% |

---

## 11. Known limitations and roadmap

| Area | Today | Production upgrade |
|---|---|---|
| **Scale-out** | One process: fault state and the scheduler live in memory | Move faults to Redis/Postgres; run the scheduler as its own worker with leader election |
| **Realtime** | The UI must poll | Add SSE/WebSocket pushing each new `health_logs` row (insertion point: `HealthCheckScheduler.execute_check`) |
| **Alerting** | Detection only; no notifications | Incident table (open after N consecutive failures, close on first success, track MTTR) plus webhook/email |
| **Auth** | None; fault endpoints are open | API keys or OAuth; at minimum protect `inject`/`clear` and target writes |
| **SSRF** | Any `http(s)` URL is accepted, including private addresses | Egress allow-list or private-range blocking if exposed publicly |
| **Check depth** | Status code only | Per-target content or JSON assertions, custom headers, methods |
| **Connection pooling** | In-process pool of 10 (+10 overflow) | PgBouncer in front of Postgres at scale |
| **Observability** | Plain logs | Prometheus metrics, structured JSON logs, tracing |

---

## 12. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Cannot reach PostgreSQL. Is it running?` on startup | The database is not up. Run `docker compose up -d db` |
| `Database tables are missing. Run: alembic upgrade head` | Migrations have not been applied. Run `cd backend && alembic upgrade head` |
| `port is already allocated` on 8000 | A local `uvicorn` and the Docker `api` are both trying to use it. Stop one |
| `port is already allocated` on 5433 | Another process uses 5433. Change the host port in `docker-compose.yml` and `DATABASE_URL` |
| Browser shows a CORS error | Your frontend origin is not in `CORS_ORIGINS`. Add it and restart the API (or use the Vite proxy) |
| A target never leaves `UNKNOWN` | It has no checks yet; wait one interval or `POST /api/targets/{id}/check` |
| Seeded external targets show `DOWN` | Your machine cannot reach them (offline, VPN, firewall). That is the monitor working correctly |
| Tests: `Cannot reach PostgreSQL for tests` | Run `docker compose up -d db` first |
| Tests: `Refusing to run: test database name must end in '_test'` | `TEST_DATABASE_URL` points at a non-test database; the guard stopped it. Unset it or point it at a `*_test` database |
| `ModuleNotFoundError` after reopening a terminal | The virtualenv is not active. Run `source .venv/bin/activate` in `backend/` |
| Container keeps restarting | `docker compose logs api` shows why; the usual cause is a wrong `DATABASE_URL` |
