# Network & Service Health Monitor — Backend (Part 1)

An asynchronous uptime and latency monitor built with **Python 3.11+, FastAPI and SQLAlchemy 2.0 (`AsyncSession`)**. It polls websites and APIs on a schedule, stores every result, rolls history up into daily aggregates, and includes a **chaos engine** that can fake outages on demand so detection and recovery can be demonstrated without breaking anything real.

This document is the hand-off for **Part 2** (the TypeScript dashboard). Section 9 is the contract you will build against.

---

## 1. Quick start

```bash
cd backend
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

uvicorn app.main:app --reload --port 8000              # SQLite file ./monitor.db, 3 demo targets seeded
```

* Interactive API docs: <http://localhost:8000/docs>
* Liveness probe: <http://localhost:8000/health>
* Tests: `pytest` (no external network needed; a local stub HTTP server is used)

> **Run exactly one worker.** Do not use `--workers N`. Fault state lives in process memory and each worker would also run its own scheduler, duplicating every check. See [Known limitations](#10-known-limitations--design-trade-offs).

### 60-second demo

```bash
# 1. Watch the demo targets get checked, then look at the dashboard payload
curl -s localhost:8000/api/overview | python -m json.tool

# 2. Break GitHub API for 2 minutes (target id 2)
curl -X POST localhost:8000/api/targets/2/faults/inject \
     -H 'content-type: application/json' \
     -d '{"fault_type": "TIMEOUT", "duration_seconds": 120}'

# 3. Force a check now instead of waiting for the interval
curl -X POST localhost:8000/api/targets/2/check

# 4. Recover immediately (or just wait for the 120s to elapse)
curl -X POST localhost:8000/api/targets/2/faults/clear
```

---

## 2. Project layout

```
backend/
├── app/
│   ├── models.py       # DB schema: targets, health_logs, uptime_daily_aggregates
│   ├── scheduler.py    # Background async checks, rollups, retention
│   ├── simulation.py   # Stateful fault-injection engine (INJECTED_FAULTS)
│   ├── routes.py       # REST API
│   ├── schemas.py      # Pydantic request/response models (the API contract)
│   ├── database.py     # Async engine, session factory, create_all
│   ├── config.py       # Environment-driven settings
│   └── main.py         # FastAPI app, lifespan, CORS, router wiring
├── tests/              # pytest + pytest-asyncio
├── requirements.txt / requirements-dev.txt
└── pytest.ini
```

The four required modules have strictly separate responsibilities; the other files are plumbing.

| Module | Owns | Must not |
|---|---|---|
| `models.py` | Table definitions, indexes, UTC datetime handling | Import anything from the app |
| `simulation.py` | Fault state and fabricated failure results | Know about HTTP, the DB, or the scheduler |
| `scheduler.py` | Check execution, persistence, rollups, pruning | Know about FastAPI routes |
| `routes.py` | HTTP surface, validation, query composition | Perform network checks itself (it delegates to the scheduler) |

Dependency direction: `routes → scheduler → simulation`, and everything → `models`. There are no cycles.

---

## 3. Architecture

```
                         FastAPI process (single event loop)
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                                                                         │
 │  HTTP clients ──► routes.py ──┬──► SQLAlchemy AsyncSession ──► Database │
 │  (Part 2 UI)                  │            ▲                            │
 │                               │            │ writes health_logs         │
 │              inject / clear   ▼            │                            │
 │                       simulation.py   scheduler.py                      │
 │                     INJECTED_FAULTS ◄── get_active_fault()              │
 │                                          │                              │
 │                  ┌───────────────────────┴───────────────┐              │
 │                  │ one asyncio.Task per target           │              │
 │                  │ + one maintenance task                │              │
 │                  └───────────────────────┬───────────────┘              │
 │                                          ▼                              │
 │                        shared httpx.AsyncClient ──► monitored URLs      │
 └─────────────────────────────────────────────────────────────────────────┘
```

### Lifecycle (`main.py` lifespan)

1. `init_db()` creates tables and indexes (idempotent).
2. Demo targets are seeded if the table is empty and `SEED_DEMO_TARGETS=true`.
3. `HealthCheckScheduler.start()` creates the shared `httpx.AsyncClient`, back-fills aggregates from existing raw logs, and starts one monitor task per target plus a maintenance task.
4. On shutdown: tasks are cancelled and awaited, the HTTP client is closed, faults are cleared, and the DB engine is disposed.

---

## 4. Data model

```
targets                         health_logs                          uptime_daily_aggregates
───────────────────             ───────────────────────────          ──────────────────────────
id            PK                id               PK                  id                    PK
name          UNIQUE            target_id        FK → targets (CASCADE)   target_id        FK → targets (CASCADE)
url                             timestamp        UTC, aware          day                   DATE (UTC)
check_interval  (seconds)       response_time_ms FLOAT, ≥ 0          total_checks
created_at                      is_healthy       BOOL                healthy_checks
                                error_message    TEXT, nullable      uptime_percentage
                                status_code      INT, nullable  *    avg / min / max_response_time_ms
                                                                     updated_at
                                                                     UNIQUE (target_id, day)
```

\* `status_code` is an addition to the spec; it makes incidents far easier to diagnose.

**Indexes**

| Index | Columns | Serves |
|---|---|---|
| `ix_health_logs_target_id_timestamp_desc` | `(target_id, timestamp DESC)` | Dashboard "latest N checks" and window queries |
| `ix_uptime_daily_target_id_day_desc` | `(target_id, day DESC)` | 30-day uptime lookups |
| `uq_uptime_daily_target_day` | `(target_id, day)` | Idempotent upserts during rollup |

**Timestamps.** Everything is timezone-aware UTC end to end. SQLite cannot store time zones, so the custom `UTCDateTime` type converts on write and re-attaches UTC on read; PostgreSQL behaves identically through the same type.

**Deletes.** Removing a target cascades to its logs and aggregates through `ON DELETE CASCADE` (SQLite foreign keys are enabled via a `PRAGMA` in `database.py`). There are intentionally no ORM `relationship()`s: async sessions forbid implicit lazy loading, and every query here is explicit.

---

## 5. How a check works (`scheduler.py`)

```
_monitor_loop(target)                         one task per target
  └─ sleep(random jitter ≤ min(interval, 5s))     spreads first checks out
  └─ loop:
       execute_check(spec)
         ├─ 1. fault = simulation.get_active_fault(target_id)
         │      └─ fault present → fabricate failure, DO NOT touch the network
         ├─ 2. else real I/O:
         │      start = time.perf_counter()
         │      async with asyncio.timeout(T):             hard wall-clock deadline
         │        async with client.stream("GET", url):    returns at response headers
         │          elapsed_ms = (perf_counter() - start) * 1000
         ├─ 3. classify: HTTP <400 healthy │ HTTP ≥400 │ timeout │ DNS │ other
         └─ 4. INSERT health_logs row
       sleep until next_run  (drift-corrected: next_run += interval)
```

Key behaviours:

* **Latency** is `time.perf_counter()` (monotonic, sub-millisecond), stored with 3 decimals. It measures time to *response headers*; bodies are never downloaded, so a large page cannot inflate the figure or the bandwidth bill.
* **Healthy** means the final status code is `< 400` (redirects are followed).
* **Never dies silently.** Each iteration is wrapped so one bad check cannot kill a target's loop. Failures are logged, and `CancelledError` is always re-raised so shutdown works.
* **Overrun protection.** If a check takes longer than the interval, the loop waits one full interval rather than bursting to catch up.
* **Concurrency cap.** A semaphore (`MAX_CONCURRENT_CHECKS`, default 50) bounds simultaneous outbound requests.
* **Reconciliation.** `sync_targets()` diffs the `targets` table against running tasks and starts, stops or restarts loops. Routes call it after every create/update/delete (changes take effect immediately), and the maintenance loop calls it periodically as a safety net.

### Maintenance (every `MAINTENANCE_INTERVAL_SECONDS`, default 300)

1. **Rollup** — recompute aggregates for today and yesterday for each target. Latency stats are over *healthy* checks only; uptime counts all checks.
2. **Prune** — delete raw logs older than `RAW_LOG_RETENTION_DAYS` (default 14) and aggregates older than `AGGREGATE_RETENTION_DAYS` (default 30).

Two details protect data integrity: the log cutoff is aligned to UTC midnight, so a day is either fully present or fully gone, and a rollup over a day with zero raw logs is a no-op. Together they guarantee a re-rollup can never overwrite a historical aggregate with partial data or zeros.

---

## 6. The chaos engine (`simulation.py`)

```python
INJECTED_FAULTS: dict[int, FaultType]   # target_id -> fault
```

| Fault | Simulated result | `status_code` | `response_time_ms` |
|---|---|---|---|
| `TIMEOUT` | "Request timed out after Ns" | `null` | ≈ the configured timeout (fabricated instantly, no sleeping) |
| `500_ERROR` | "HTTP 500 Internal Server Error" | `500` | 15–120 ms |
| `DNS_FAILURE` | "DNS resolution failed…" | `null` | 2–40 ms |

* **Interception, not mocking.** The scheduler asks `get_active_fault()` *before* any real request. While a fault is active, no HTTP request leaves the process (the test suite asserts this by counting hits on a stub server).
* **Recovery.** Clear the fault (API call, `duration_seconds` expiry, or deleting the target) and the next check hits the real endpoint again. Expiry is lazy: it is evaluated on lookup, so no timer tasks are needed.
* **Labelling.** Every fabricated message starts with `[SIMULATED] ` (exported as `simulation.SIMULATED_PREFIX`) so simulated failures are unmistakable in the UI, SQL and logs. Simulated checks are written to `health_logs` like any other, so uptime and latency charts show the incident exactly as a real one.
* **Thread-safety.** All mutators are synchronous and run on the single event loop, so they are atomic relative to other coroutines. No locks are needed.

---

## 7. Configuration

All settings are environment variables, read once at startup (`config.py`).

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `sqlite+aiosqlite:///./monitor.db` | Use `postgresql+asyncpg://user:pw@host/db` for Postgres |
| `REQUEST_TIMEOUT_SECONDS` | `10` | Hard deadline per check; also the simulated `TIMEOUT` latency |
| `MAX_CONCURRENT_CHECKS` | `50` | Outbound request concurrency cap |
| `MIN_CHECK_INTERVAL_SECONDS` | `5` | Lower bound enforced by the API |
| `MAX_CHECK_INTERVAL_SECONDS` | `86400` | Upper bound enforced by the API |
| `SCHEDULER_INITIAL_JITTER_SECONDS` | `5` | Max random delay before a target's first check |
| `RAW_LOG_RETENTION_DAYS` | `14` | Minimum `2` |
| `AGGREGATE_RETENTION_DAYS` | `30` | |
| `MAINTENANCE_INTERVAL_SECONDS` | `300` | Rollup / prune / reconcile cadence |
| `SEED_DEMO_TARGETS` | `true` | Seed 3 demo targets into an empty DB |
| `CORS_ORIGINS` | `http://localhost:5173,http://localhost:3000` | Comma-separated; covers Vite and CRA defaults |
| `LOG_LEVEL` / `SQL_ECHO` | `INFO` / `false` | Diagnostics |

---

## 8. Testing

```bash
pytest -q          # 11 tests, ~3 seconds
```

The suite spins up a throwaway HTTP server on `127.0.0.1` and a temporary SQLite file, then drives the real app through its lifespan. It covers CRUD and validation, real failure modes (HTTP 503, connection refused, DNS), all three injected faults (including proof that no real request fires and that recovery works), timed-fault expiry, cascade deletes, analytics maths, the overview endpoint, aggregate rollup and pruning safety.

---

## 9. API reference (the contract for Part 2)

Base path `/api`. All timestamps are ISO-8601 UTC (`2026-10-25T19:09:58.932147Z`). Errors use FastAPI's standard `{"detail": ...}` shape (`404` unknown id, `409` duplicate name, `422` validation).

### Targets

| Method | Path | Body | Notes |
|---|---|---|---|
| `POST` | `/targets` | `{name, url, check_interval?}` | `201`. `url` must be http(s); `check_interval` 5–86400 s, default 30 |
| `GET` | `/targets` | | Includes `active_fault` when one is set |
| `GET` | `/targets/{id}` | | |
| `PATCH` | `/targets/{id}` | any of `name, url, check_interval` | Monitor loop restarts automatically |
| `DELETE` | `/targets/{id}` | | `204`. Cascades logs and aggregates, clears any fault |
| `POST` | `/targets/{id}/check` | | Run and persist one check now; returns the log row |

### History & analytics

| Method | Path | Query params | Returns |
|---|---|---|---|
| `GET` | `/overview` | | **Best landing-page call**: per target `status` (`UP`/`DOWN`/`UNKNOWN`), `latest_check`, `checks_24h`, `uptime_24h` |
| `GET` | `/targets/{id}/analytics` | `start`, `end`, `bucket_seconds` | `summary` (uptime %, avg/min/max, p50/p95/p99) + `buckets[]` time series for charts |
| `GET` | `/targets/{id}/logs` | `start`, `end`, `healthy`, `limit` (≤1000), `offset` | Paginated raw checks, newest first |
| `GET` | `/targets/{id}/aggregates` | `days` (1–30) | Daily rollups, oldest first |

Window rules: `end` defaults to now, `start` to `end − 24h`, `start < end`, span ≤ 31 days (use `/aggregates` beyond that). `bucket_seconds` is auto-chosen (about 200 points) when omitted. **Empty buckets are omitted**, so a gap in the series means *no data*, not zero. Latency stats exclude failed checks.

### Simulation

| Method | Path | Body | Notes |
|---|---|---|---|
| `GET` | `/faults` | | All active faults |
| `POST` | `/targets/{id}/faults/inject` | `{fault_type, duration_seconds?}` | `fault_type`: `TIMEOUT` \| `500_ERROR` \| `DNS_FAILURE`; omit duration for "until cleared" |
| `POST` | `/targets/{id}/faults/clear` | | Returns `{target_id, cleared}` (idempotent) |

### Suggestions for the Part 2 dashboard

* Poll `/overview` every ~5–10 s for the status grid; fetch `/analytics` only for the selected target.
* Render the `[SIMULATED]` prefix distinctly (e.g. a "chaos" badge) so demos are honest.
* Derive **incidents** client-side from `/logs?healthy=false`, or add a server-side `incidents` table in Part 3 (below).

---

## 10. Known limitations & design trade-offs

These are deliberate scoping decisions and good talking points.

| Area | Current behaviour | Production upgrade |
|---|---|---|
| **Scaling out** | Single process: fault state and the scheduler are in memory | Move faults to Redis/DB; run the scheduler as its own worker with leader election |
| **Migrations** | `create_all()` at startup | Alembic |
| **Analytics** | Window rows are bucketed in Python (fine for ≤ 31 days at typical intervals) | Push bucketing into SQL (`date_bin` / `time_bucket`) or use TimescaleDB |
| **Auth** | None; the API is open | API keys or OAuth; at minimum protect the fault endpoints |
| **SSRF** | Any http(s) URL is accepted, including private addresses | Egress allow-list or private-range blocking if exposed publicly |
| **Redirects/bodies** | Follows redirects; checks response headers only, not content | Optional keyword/JSON assertions per target |
| **Alerting** | Detection only; no notifications | Part 3 candidate: consecutive-failure threshold → incident rows → webhook/email/WebSocket push |

## 11. Possible Part 3 hooks

* **Incident model**: open an incident after *N* consecutive failures, close it on the first success, store MTTR.
* **Realtime**: a WebSocket/SSE channel emitting each new `health_logs` row (the insertion point is `HealthCheckScheduler.execute_check`).
* **Docker**: `uvicorn app.main:app --host 0.0.0.0 --port 8000` with a single worker, plus a Postgres service and a `/health` healthcheck.
