"""Background health-check service.

Design
------
* One long-lived asyncio.Task per target (_monitor_loop), each honouring that
  target's own check_interval with drift correction.
* One shared httpx.AsyncClient (connection pooling, hard timeout), created in
  start() and closed in stop(); both are called from the FastAPI lifespan.
* sync_targets() reconciles running tasks with the targets table. API routes call it
  after every create/update/delete; the maintenance loop calls it as a safety net.
* A maintenance loop rolls raw logs into uptime_daily_aggregates (one SQL statement,
  using Postgres percentile_cont for p95) and prunes old data.
* Before any real network I/O the chaos engine is consulted; an active fault
  short-circuits the check with a fabricated result.
"""

import asyncio
import logging
import random
import socket
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime

import httpx
from sqlalchemy import Date, cast, delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import simulation
from app.config import Settings
from app.models import HealthLog, Target, UptimeDailyAggregate, utcnow

logger = logging.getLogger("monitor.scheduler")


@dataclass(frozen=True)
class TargetSpec:
    """Immutable snapshot of the fields a monitor loop needs (no ORM objects cross tasks)."""

    id: int
    url: str
    check_interval: int


@dataclass(frozen=True)
class CheckOutcome:
    timestamp: datetime
    response_time_ms: float
    is_healthy: bool
    status_code: int | None
    error_message: str | None


# --------------------------------------------------------------------------------
# Aggregation & retention (module-level functions so they are directly testable)
# --------------------------------------------------------------------------------
def _midnight_utc(day: date) -> datetime:
    return datetime.combine(day, dtime.min, tzinfo=UTC)


async def rollup_daily_aggregates(
    session_factory: async_sessionmaker[AsyncSession], days_back: int
) -> int:
    """Recompute daily aggregates for today plus the previous days_back UTC days.

    A single INSERT ... SELECT ... ON CONFLICT DO UPDATE does all the work inside
    Postgres, for every target at once. Latency statistics use FILTER so they cover
    healthy checks only; uptime counts every check. Days with no raw logs produce no
    row, so existing history is never overwritten with zeros.
    """
    window_start = _midnight_utc(utcnow().date() - timedelta(days=days_back))

    is_up = HealthLog.is_healthy.is_(True)
    day = cast(func.timezone("UTC", HealthLog.timestamp), Date).label("day")
    total = func.count(HealthLog.id)
    healthy = func.count(HealthLog.id).filter(is_up)

    source = (
        select(
            HealthLog.target_id,
            day,
            total.label("total_checks"),
            healthy.label("healthy_checks"),
            (healthy * 100.0 / total).label("uptime_percentage"),
            func.avg(HealthLog.response_time_ms).filter(is_up).label("avg_response_time_ms"),
            func.min(HealthLog.response_time_ms).filter(is_up).label("min_response_time_ms"),
            func.max(HealthLog.response_time_ms).filter(is_up).label("max_response_time_ms"),
            func.percentile_cont(0.95)
            .within_group(HealthLog.response_time_ms)
            .filter(is_up)
            .label("p95_response_time_ms"),
        )
        .where(HealthLog.timestamp >= window_start)
        .group_by(HealthLog.target_id, day)
    )

    columns = [c.name for c in source.selected_columns]
    insert = pg_insert(UptimeDailyAggregate).from_select(columns, source, include_defaults=False)
    upsert = insert.on_conflict_do_update(
        index_elements=["target_id", "day"],
        set_={
            **{name: insert.excluded[name] for name in columns if name not in ("target_id", "day")},
            "updated_at": func.now(),
        },
    )
    async with session_factory() as session:
        result = await session.execute(upsert)
        await session.commit()
        return result.rowcount or 0


async def prune_old_data(
    session_factory: async_sessionmaker[AsyncSession],
    raw_retention_days: int,
    aggregate_retention_days: int,
) -> tuple[int, int]:
    """Delete expired raw logs and aggregates. Returns (logs_deleted, aggregates_deleted).

    The raw-log cutoff is aligned to UTC midnight so a day is either fully present or
    fully gone; a later re-rollup can therefore never compute an aggregate from a
    partially pruned day.
    """
    today = utcnow().date()
    log_cutoff = _midnight_utc(today - timedelta(days=raw_retention_days))
    aggregate_cutoff = today - timedelta(days=aggregate_retention_days)
    async with session_factory() as session:
        logs = await session.execute(delete(HealthLog).where(HealthLog.timestamp < log_cutoff))
        aggs = await session.execute(
            delete(UptimeDailyAggregate).where(UptimeDailyAggregate.day < aggregate_cutoff)
        )
        await session.commit()
        return logs.rowcount or 0, aggs.rowcount or 0


# --------------------------------------------------------------------------------
# Scheduler
# --------------------------------------------------------------------------------
def _is_dns_failure(exc: BaseException) -> bool:
    """Walk the exception chain looking for a resolver error."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, socket.gaierror):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


class HealthCheckScheduler:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], settings: Settings):
        self._session_factory = session_factory
        self._settings = settings
        self._client: httpx.AsyncClient | None = None
        self._monitors: dict[int, tuple[TargetSpec, asyncio.Task[None]]] = {}
        self._maintenance_task: asyncio.Task[None] | None = None
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_checks)
        self._sync_lock = asyncio.Lock()
        self._running = False

    # -- lifecycle -------------------------------------------------------------
    async def start(self) -> None:
        if self._running:
            return
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._settings.request_timeout_seconds),
            follow_redirects=True,
            headers={"User-Agent": "ServiceHealthMonitor/1.0"},
            limits=httpx.Limits(
                max_connections=self._settings.max_concurrent_checks,
                max_keepalive_connections=self._settings.max_concurrent_checks,
            ),
        )
        self._running = True
        try:
            # Recompute every day still covered by raw logs, so aggregates are correct
            # after downtime or a crash.
            await rollup_daily_aggregates(
                self._session_factory, days_back=self._settings.raw_log_retention_days
            )
        except Exception:  # noqa: BLE001 - a rollup hiccup must not stop monitoring
            logger.exception("Startup aggregate backfill failed")
        await self.sync_targets()
        self._maintenance_task = asyncio.create_task(
            self._maintenance_loop(), name="monitor-maintenance"
        )
        logger.info("Scheduler started with %d target(s)", len(self._monitors))

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        tasks = [task for _, task in self._monitors.values()]
        if self._maintenance_task is not None:
            tasks.append(self._maintenance_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._monitors.clear()
        self._maintenance_task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("Scheduler stopped")

    # -- target reconciliation -------------------------------------------------
    async def sync_targets(self) -> None:
        """Make the set of running monitor tasks match the targets table."""
        if not self._running:
            return
        async with self._sync_lock:
            async with self._session_factory() as session:
                rows = (
                    await session.execute(select(Target.id, Target.url, Target.check_interval))
                ).all()
            desired = {row.id: TargetSpec(row.id, row.url, row.check_interval) for row in rows}

            stale: list[asyncio.Task[None]] = []
            for target_id, (spec, task) in list(self._monitors.items()):
                if target_id not in desired or desired[target_id] != spec or task.done():
                    task.cancel()
                    stale.append(task)
                    del self._monitors[target_id]
            if stale:
                await asyncio.gather(*stale, return_exceptions=True)

            for target_id, spec in desired.items():
                if target_id not in self._monitors:
                    task = asyncio.create_task(
                        self._monitor_loop(spec), name=f"monitor-{target_id}"
                    )
                    self._monitors[target_id] = (spec, task)

    # -- per-target loop -------------------------------------------------------
    async def _monitor_loop(self, spec: TargetSpec) -> None:
        loop = asyncio.get_running_loop()
        # Spread first checks out so N targets do not all fire in the same instant.
        jitter_cap = min(
            float(spec.check_interval), self._settings.scheduler_initial_jitter_seconds
        )
        if jitter_cap > 0:
            await asyncio.sleep(random.uniform(0.0, jitter_cap))

        next_run = loop.time()
        while True:
            try:
                await self.execute_check(spec)
            except asyncio.CancelledError:
                raise
            except IntegrityError:
                logger.warning("Target %s was deleted mid-check; result discarded", spec.id)
            except Exception:  # noqa: BLE001 - one bad check must never kill the loop
                logger.exception("Check failed unexpectedly for target %s", spec.id)

            next_run += spec.check_interval
            delay = next_run - loop.time()
            if delay <= 0:
                # We overran the interval (slow check / suspended laptop). Do not burst to
                # catch up; wait one full interval from now.
                next_run = loop.time() + spec.check_interval
                delay = spec.check_interval
            await asyncio.sleep(delay)

    # -- a single check --------------------------------------------------------
    async def execute_check(self, spec: TargetSpec) -> HealthLog:
        """Probe the target, persist the result, return the stored row."""
        outcome = await self._probe(spec)
        log = HealthLog(
            target_id=spec.id,
            timestamp=outcome.timestamp,
            response_time_ms=outcome.response_time_ms,
            is_healthy=outcome.is_healthy,
            status_code=outcome.status_code,
            error_message=outcome.error_message,
        )
        async with self._session_factory() as session:
            session.add(log)
            await session.commit()
        logger.debug(
            "target=%s healthy=%s latency=%.3fms error=%s",
            spec.id,
            outcome.is_healthy,
            outcome.response_time_ms,
            outcome.error_message,
        )
        return log

    async def run_check_now(self, target_id: int) -> HealthLog | None:
        """On-demand check (used by the API and handy for demos/tests)."""
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(Target.id, Target.url, Target.check_interval).where(
                        Target.id == target_id
                    )
                )
            ).one_or_none()
        if row is None:
            return None
        return await self.execute_check(TargetSpec(row.id, row.url, row.check_interval))

    async def _probe(self, spec: TargetSpec) -> CheckOutcome:
        timeout = self._settings.request_timeout_seconds
        started_at = utcnow()

        # 1. Chaos engine first: intercept *before* any real I/O happens.
        fault = simulation.get_active_fault(spec.id)
        if fault is not None:
            simulated = simulation.build_simulated_outcome(fault, timeout)
            return CheckOutcome(
                timestamp=started_at,
                response_time_ms=simulated.response_time_ms,
                is_healthy=False,
                status_code=simulated.status_code,
                error_message=simulated.error_message,
            )

        # 2. Real network I/O, timed with a monotonic high-resolution clock.
        if self._client is None:
            raise RuntimeError("Scheduler is not running")
        async with self._semaphore:
            start = time.perf_counter()
            try:
                # asyncio.timeout is a hard wall-clock deadline for the whole exchange;
                # httpx's own timeout only bounds each individual socket operation.
                async with asyncio.timeout(timeout):
                    # stream() returns once headers arrive, so we measure time-to-response
                    # without downloading (potentially huge) bodies.
                    async with self._client.stream("GET", spec.url) as response:
                        elapsed_ms = (time.perf_counter() - start) * 1000.0
                        status_code = response.status_code
                healthy = status_code < 400
                return CheckOutcome(
                    timestamp=started_at,
                    response_time_ms=round(elapsed_ms, 3),
                    is_healthy=healthy,
                    status_code=status_code,
                    error_message=None if healthy else f"HTTP {status_code}",
                )
            except (TimeoutError, httpx.TimeoutException):
                return self._failure(started_at, start, f"Request timed out after {timeout:g}s")
            except httpx.RequestError as exc:
                if _is_dns_failure(exc):
                    message = f"DNS resolution failed: {exc}"
                else:
                    message = f"{type(exc).__name__}: {exc}"
                return self._failure(started_at, start, message)
            except Exception as exc:  # noqa: BLE001 - e.g. httpx.InvalidURL
                return self._failure(started_at, start, f"{type(exc).__name__}: {exc}")

    @staticmethod
    def _failure(started_at: datetime, start: float, message: str) -> CheckOutcome:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return CheckOutcome(
            timestamp=started_at,
            response_time_ms=round(elapsed_ms, 3),
            is_healthy=False,
            status_code=None,
            error_message=message.strip() or "Unknown error",
        )

    # -- maintenance -----------------------------------------------------------
    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(self._settings.maintenance_interval_seconds)
            try:
                await self.sync_targets()
                await rollup_daily_aggregates(self._session_factory, days_back=1)
                logs, aggs = await prune_old_data(
                    self._session_factory,
                    self._settings.raw_log_retention_days,
                    self._settings.aggregate_retention_days,
                )
                if logs or aggs:
                    logger.info("Pruned %d health log(s) and %d aggregate(s)", logs, aggs)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Maintenance cycle failed")
