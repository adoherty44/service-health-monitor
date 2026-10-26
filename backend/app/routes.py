"""REST API: target CRUD, historical analytics, and fault injection."""
import base64
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import func, literal_column, select, true, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app import simulation
from app.database import get_session
from app.models import HealthLog, Target, UptimeDailyAggregate, utcnow
from app.scheduler import HealthCheckScheduler
from app.schemas import (
    AnalyticsBucket,
    AnalyticsResponse,
    AnalyticsSummary,
    DailyAggregateRead,
    FaultClearResult,
    FaultInject,
    FaultRead,
    HealthLogPage,
    HealthLogRead,
    OverviewItem,
    TargetCreate,
    TargetRead,
    TargetUpdate,
)

router = APIRouter(prefix="/api")

UNPROCESSABLE = 422  # literal: the named constant was renamed across Starlette versions
MAX_WINDOW = timedelta(days=31)
MAX_BUCKETS = 5_000
_AUTO_BUCKET_CHOICES = (10, 30, 60, 300, 900, 1_800, 3_600, 7_200, 21_600, 43_200, 86_400)
_NAME_TAKEN = "A target with this name already exists"

# --- Dependencies & helpers --------------------------------------------------------
SessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_scheduler(request: Request) -> HealthCheckScheduler:
    return request.app.state.scheduler


SchedulerDep = Annotated[HealthCheckScheduler, Depends(get_scheduler)]


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _round(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(float(value), digits)


def _fault_read(target_id: int) -> FaultRead | None:
    details = simulation.get_fault_details(target_id)
    if details is None:
        return None
    return FaultRead(
        target_id=details.target_id,
        fault_type=details.fault_type,
        injected_at=details.injected_at,
        expires_at=details.expires_at,
    )


def _target_read(target: Target) -> TargetRead:
    return TargetRead(
        id=target.id,
        name=target.name,
        url=target.url,
        check_interval=target.check_interval,
        created_at=target.created_at,
        active_fault=_fault_read(target.id),
    )


async def _get_target_or_404(session: AsyncSession, target_id: int) -> Target:
    target = await session.get(Target, target_id)
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Target {target_id} not found")
    return target


def _resolve_window(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    """Default to the last 24h; validate ordering and cap the span."""
    window_end = _as_utc(end) if end else utcnow()
    window_start = _as_utc(start) if start else window_end - timedelta(hours=24)
    if window_start >= window_end:
        raise HTTPException(UNPROCESSABLE, "start must be earlier than end")
    if window_end - window_start > MAX_WINDOW:
        raise HTTPException(
            UNPROCESSABLE,
            f"Window too large; maximum is {MAX_WINDOW.days} days. Use /aggregates for longer history.",
        )
    return window_start, window_end


def _auto_bucket(span_seconds: float) -> int:
    """Smallest 'nice' bucket size that yields at most ~200 points."""
    for choice in _AUTO_BUCKET_CHOICES:
        if span_seconds / choice <= 200:
            return choice
    return _AUTO_BUCKET_CHOICES[-1]


def _encode_cursor(timestamp: datetime, log_id: int) -> str:
    return base64.urlsafe_b64encode(f"{timestamp.isoformat()}|{log_id}".encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        timestamp_text, id_text = raw.rsplit("|", 1)
        return _as_utc(datetime.fromisoformat(timestamp_text)), int(id_text)
    except Exception:  # noqa: BLE001 - any malformed token is a client error
        raise HTTPException(UNPROCESSABLE, "Invalid cursor") from None


# --- Targets: CRUD -----------------------------------------------------------------
@router.post("/targets", response_model=TargetRead, status_code=status.HTTP_201_CREATED, tags=["targets"])
async def create_target(payload: TargetCreate, session: SessionDep, scheduler: SchedulerDep):
    target = Target(name=payload.name, url=payload.url, check_interval=payload.check_interval)
    session.add(target)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, _NAME_TAKEN) from None
    await scheduler.sync_targets()
    return _target_read(target)


@router.get("/targets", response_model=list[TargetRead], tags=["targets"])
async def list_targets(session: SessionDep):
    targets = (await session.execute(select(Target).order_by(Target.id))).scalars().all()
    return [_target_read(t) for t in targets]


@router.get("/targets/{target_id}", response_model=TargetRead, tags=["targets"])
async def get_target(target_id: int, session: SessionDep):
    return _target_read(await _get_target_or_404(session, target_id))


@router.patch("/targets/{target_id}", response_model=TargetRead, tags=["targets"])
async def update_target(
    target_id: int, payload: TargetUpdate, session: SessionDep, scheduler: SchedulerDep
):
    target = await _get_target_or_404(session, target_id)
    for field, value in payload.model_dump(exclude_unset=True).items():
        if value is not None:
            setattr(target, field, value)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, _NAME_TAKEN) from None
    await scheduler.sync_targets()  # restarts the monitor loop if url/interval changed
    return _target_read(target)


@router.delete("/targets/{target_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["targets"])
async def delete_target(target_id: int, session: SessionDep, scheduler: SchedulerDep):
    target = await _get_target_or_404(session, target_id)
    await session.delete(target)  # health_logs / aggregates removed by ON DELETE CASCADE
    await session.commit()
    simulation.clear_fault(target_id)
    await scheduler.sync_targets()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/targets/{target_id}/check", response_model=HealthLogRead, tags=["targets"])
async def check_target_now(target_id: int, session: SessionDep, scheduler: SchedulerDep):
    """Run (and persist) one check immediately, outside the regular schedule."""
    await _get_target_or_404(session, target_id)
    log = await scheduler.run_check_now(target_id)
    if log is None:  # deleted between the two calls
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Target {target_id} not found")
    return log


# --- History & analytics -----------------------------------------------------------
@router.get("/targets/{target_id}/logs", response_model=HealthLogPage, tags=["analytics"])
async def get_target_logs(
    target_id: int,
    session: SessionDep,
    start: Annotated[datetime | None, Query(description="ISO-8601; default end - 24h")] = None,
    end: Annotated[datetime | None, Query(description="ISO-8601; default now")] = None,
    healthy: Annotated[bool | None, Query(description="Filter by outcome")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    cursor: Annotated[str | None, Query(description="next_cursor from the previous page")] = None,
):
    """Raw check history, newest first, with keyset pagination.

    Keyset (not OFFSET) pagination: each page is an index seek on
    (target_id, timestamp DESC), so page 500 costs the same as page 1.
    """
    await _get_target_or_404(session, target_id)
    window_start, window_end = _resolve_window(start, end)
    conditions = [
        HealthLog.target_id == target_id,
        HealthLog.timestamp >= window_start,
        HealthLog.timestamp <= window_end,
    ]
    if healthy is not None:
        conditions.append(HealthLog.is_healthy.is_(healthy))
    if cursor is not None:
        cursor_ts, cursor_id = _decode_cursor(cursor)
        conditions.append(
            tuple_(HealthLog.timestamp, HealthLog.id) < tuple_(cursor_ts, cursor_id)
        )

    rows = (
        await session.execute(
            select(HealthLog)
            .where(*conditions)
            .order_by(HealthLog.timestamp.desc(), HealthLog.id.desc())
            .limit(limit + 1)  # one extra row tells us whether another page exists
        )
    ).scalars().all()
    page, has_more = rows[:limit], len(rows) > limit
    return HealthLogPage(
        items=[HealthLogRead.model_validate(r) for r in page],
        next_cursor=_encode_cursor(page[-1].timestamp, page[-1].id) if has_more else None,
    )


@router.get("/targets/{target_id}/analytics", response_model=AnalyticsResponse, tags=["analytics"])
async def get_target_analytics(
    target_id: int,
    session: SessionDep,
    start: Annotated[datetime | None, Query(description="ISO-8601; default end - 24h")] = None,
    end: Annotated[datetime | None, Query(description="ISO-8601; default now")] = None,
    bucket_seconds: Annotated[
        int | None, Query(ge=1, le=86_400, description="Bucket size; auto-selected if omitted")
    ] = None,
):
    """Window summary (uptime %, latency percentiles) plus a bucketed time series.

    All aggregation happens inside Postgres (FILTER, percentile_cont, date_bin); the API
    only receives the finished numbers. Empty buckets are omitted: a gap means 'no data'.
    """
    await _get_target_or_404(session, target_id)
    window_start, window_end = _resolve_window(start, end)
    span = (window_end - window_start).total_seconds()
    size = bucket_seconds or _auto_bucket(span)
    if span / size > MAX_BUCKETS:
        raise HTTPException(
            UNPROCESSABLE, f"bucket_seconds too small for this window (max {MAX_BUCKETS} buckets)"
        )

    where = (
        HealthLog.target_id == target_id,
        HealthLog.timestamp >= window_start,
        HealthLog.timestamp <= window_end,
    )
    is_up = HealthLog.is_healthy.is_(True)
    latency = HealthLog.response_time_ms

    summary_row = (
        await session.execute(
            select(
                func.count(HealthLog.id),
                func.count(HealthLog.id).filter(is_up),
                func.avg(latency).filter(is_up),
                func.min(latency).filter(is_up),
                func.max(latency).filter(is_up),
                func.percentile_cont(0.50).within_group(latency).filter(is_up),
                func.percentile_cont(0.95).within_group(latency).filter(is_up),
                func.percentile_cont(0.99).within_group(latency).filter(is_up),
            ).where(*where)
        )
    ).one()
    total, healthy_total, avg_ms, min_ms, max_ms, p50, p95, p99 = summary_row

    # `size` is a validated int, so inlining it into the interval literal is safe and
    # lets Postgres treat the GROUP BY expression as identical to the SELECT expression.
    bucket = func.date_bin(
        literal_column(f"INTERVAL '{int(size)} seconds'"),
        HealthLog.timestamp,
        literal_column("TIMESTAMPTZ 'epoch'"),
    )
    bucket_rows = (
        await session.execute(
            select(
                bucket.label("bucket_start"),
                func.count(HealthLog.id).label("total"),
                func.count(HealthLog.id).filter(is_up).label("healthy"),
                func.avg(latency).filter(is_up).label("avg_ms"),
                func.max(latency).filter(is_up).label("max_ms"),
            )
            .where(*where)
            .group_by(bucket)
            .order_by(bucket)
        )
    ).all()

    return AnalyticsResponse(
        target_id=target_id,
        start=window_start,
        end=window_end,
        bucket_seconds=size,
        summary=AnalyticsSummary(
            total_checks=total,
            healthy_checks=healthy_total,
            failed_checks=total - healthy_total,
            uptime_percentage=_round(healthy_total / total * 100.0, 4) if total else None,
            avg_response_time_ms=_round(avg_ms),
            min_response_time_ms=_round(min_ms),
            max_response_time_ms=_round(max_ms),
            p50_response_time_ms=_round(p50),
            p95_response_time_ms=_round(p95),
            p99_response_time_ms=_round(p99),
        ),
        buckets=[
            AnalyticsBucket(
                bucket_start=row.bucket_start,
                total_checks=row.total,
                healthy_checks=row.healthy,
                uptime_percentage=_round(row.healthy / row.total * 100.0, 4),
                avg_response_time_ms=_round(row.avg_ms),
                max_response_time_ms=_round(row.max_ms),
            )
            for row in bucket_rows
        ],
    )


@router.get("/targets/{target_id}/aggregates", response_model=list[DailyAggregateRead], tags=["analytics"])
async def get_target_daily_aggregates(
    target_id: int,
    session: SessionDep,
    days: Annotated[int, Query(ge=1, le=30, description="UTC days back, inclusive of today")] = 30,
):
    """Daily uptime rollups, oldest first. Cheap enough to call on every dashboard load."""
    await _get_target_or_404(session, target_id)
    first_day = utcnow().date() - timedelta(days=days - 1)
    result = await session.execute(
        select(UptimeDailyAggregate)
        .where(UptimeDailyAggregate.target_id == target_id, UptimeDailyAggregate.day >= first_day)
        .order_by(UptimeDailyAggregate.day.asc())
    )
    return [DailyAggregateRead.model_validate(a) for a in result.scalars().all()]


@router.get("/overview", response_model=list[OverviewItem], tags=["analytics"])
async def get_overview(session: SessionDep):
    """One call for the dashboard landing page: status, latest check, 24h uptime per target.

    The latest check per target uses a LATERAL join: one index seek per target on
    (target_id, timestamp DESC), regardless of how large health_logs grows.
    """
    latest = (
        select(HealthLog)
        .where(HealthLog.target_id == Target.id)
        .order_by(HealthLog.timestamp.desc(), HealthLog.id.desc())
        .limit(1)
        .lateral("latest")
    )
    latest_log = aliased(HealthLog, latest)
    rows = (
        await session.execute(
            select(Target, latest_log).outerjoin(latest, true()).order_by(Target.id)
        )
    ).all()

    since = utcnow() - timedelta(hours=24)
    stats = {
        row.target_id: (row.total, row.healthy)
        for row in (
            await session.execute(
                select(
                    HealthLog.target_id,
                    func.count(HealthLog.id).label("total"),
                    func.count(HealthLog.id).filter(HealthLog.is_healthy.is_(True)).label("healthy"),
                )
                .where(HealthLog.timestamp >= since)
                .group_by(HealthLog.target_id)
            )
        ).all()
    }

    items: list[OverviewItem] = []
    for target, log in rows:
        total, healthy = stats.get(target.id, (0, 0))
        items.append(
            OverviewItem(
                target=_target_read(target),
                status="UNKNOWN" if log is None else ("UP" if log.is_healthy else "DOWN"),
                latest_check=HealthLogRead.model_validate(log) if log is not None else None,
                checks_24h=total,
                uptime_24h=_round(healthy / total * 100.0, 4) if total else None,
            )
        )
    return items


# --- Simulated outages -------------------------------------------------------------
@router.get("/faults", response_model=list[FaultRead], tags=["simulation"])
async def list_faults():
    return [
        FaultRead(
            target_id=d.target_id,
            fault_type=d.fault_type,
            injected_at=d.injected_at,
            expires_at=d.expires_at,
        )
        for d in simulation.list_active_faults()
    ]


@router.post("/targets/{target_id}/faults/inject", response_model=FaultRead, tags=["simulation"])
async def inject_fault(target_id: int, payload: FaultInject, session: SessionDep):
    """Start a simulated outage. Subsequent checks are faked until cleared or expired."""
    await _get_target_or_404(session, target_id)
    details = simulation.inject_fault(target_id, payload.fault_type, payload.duration_seconds)
    return FaultRead(
        target_id=details.target_id,
        fault_type=details.fault_type,
        injected_at=details.injected_at,
        expires_at=details.expires_at,
    )


@router.post("/targets/{target_id}/faults/clear", response_model=FaultClearResult, tags=["simulation"])
async def clear_fault(target_id: int, session: SessionDep):
    """End a simulated outage; the next check hits the real endpoint (recovery)."""
    await _get_target_or_404(session, target_id)
    return FaultClearResult(target_id=target_id, cleared=simulation.clear_fault(target_id))
