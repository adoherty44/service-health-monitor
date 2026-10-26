"""Scheduler: real checks, chaos interception, reconciliation, rollups and pruning."""
import asyncio
from datetime import datetime, time as dtime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, select, update

from app import simulation
from app.config import Settings
from app.database import SessionFactory
from app.models import HealthLog, Target, UptimeDailyAggregate, utcnow
from app.scheduler import (
    HealthCheckScheduler,
    TargetSpec,
    prune_old_data,
    rollup_daily_aggregates,
)
from app.simulation import FaultType
from tests.helpers import add_logs, add_target_row

pytestmark = pytest.mark.usefixtures("clean_db")


@pytest_asyncio.fixture
async def scheduler():
    """A running scheduler with a 1-second request timeout (so timeout tests are quick)."""
    instance = HealthCheckScheduler(
        SessionFactory, Settings(request_timeout_seconds=1, scheduler_initial_jitter_seconds=3600)
    )
    await instance.start()
    yield instance
    await instance.stop()


async def count_logs(target_id: int) -> int:
    async with SessionFactory() as session:
        return (
            await session.execute(select(func.count(HealthLog.id)).where(HealthLog.target_id == target_id))
        ).scalar_one()


def day_at(days_ago: int, hour: int = 12, minute: int = 0, second: int = 0, micro: int = 0) -> datetime:
    day = utcnow().date() - timedelta(days=days_ago)
    return datetime.combine(day, dtime(hour, minute, second, micro), tzinfo=timezone.utc)


# --- A single check ------------------------------------------------------------------
async def test_healthy_check_is_recorded_with_subsecond_latency(scheduler, stub):
    tid = await add_target_row(url=stub.url("/ok"))
    log = await scheduler.run_check_now(tid)
    assert log.is_healthy and log.status_code == 200 and log.error_message is None
    assert 0 < log.response_time_ms < 1_000
    assert log.response_time_ms == round(log.response_time_ms, 3)  # stored at microsecond resolution
    assert await count_logs(tid) == 1


async def test_http_error_status_is_unhealthy(scheduler, stub):
    tid = await add_target_row(url=stub.url("/fail"))
    log = await scheduler.run_check_now(tid)
    assert not log.is_healthy and log.status_code == 503 and log.error_message == "HTTP 503"


async def test_connection_refused_is_unhealthy_with_message(scheduler):
    tid = await add_target_row(url="http://127.0.0.1:9/")
    log = await scheduler.run_check_now(tid)
    assert not log.is_healthy and log.status_code is None and log.error_message


async def test_slow_server_hits_the_hard_timeout(scheduler, stub):
    tid = await add_target_row(url=stub.url("/slow"))
    log = await scheduler.run_check_now(tid)
    assert not log.is_healthy and "timed out after 1s" in log.error_message
    assert 900 <= log.response_time_ms < 2_500  # waited ~1s, not the server's 3s


async def test_unknown_target_returns_none(scheduler):
    assert await scheduler.run_check_now(999_999) is None


# --- Chaos engine integration --------------------------------------------------------
@pytest.mark.parametrize("fault", list(FaultType))
async def test_fault_is_intercepted_before_any_network_call(scheduler, stub, fault):
    tid = await add_target_row(url=stub.url("/ok"))
    simulation.inject_fault(tid, fault)
    hits_before = stub.hits
    log = await scheduler.run_check_now(tid)
    assert stub.hits == hits_before, "no real request may be made while a fault is active"
    assert not log.is_healthy and log.error_message.startswith("[SIMULATED]")


async def test_target_recovers_after_fault_is_cleared(scheduler, stub):
    tid = await add_target_row(url=stub.url("/ok"))
    simulation.inject_fault(tid, FaultType.SERVER_ERROR)
    assert not (await scheduler.run_check_now(tid)).is_healthy
    simulation.clear_fault(tid)
    recovered = await scheduler.run_check_now(tid)
    assert recovered.is_healthy and stub.hits >= 1


async def test_timed_fault_heals_without_intervention(scheduler, stub):
    tid = await add_target_row(url=stub.url("/ok"))
    details = simulation.inject_fault(tid, FaultType.DNS_FAILURE, duration_seconds=60)
    assert not (await scheduler.run_check_now(tid)).is_healthy
    details.expires_at = utcnow() - timedelta(seconds=1)
    assert (await scheduler.run_check_now(tid)).is_healthy


# --- Scheduling loop and reconciliation ----------------------------------------------
async def test_monitor_loop_checks_repeatedly_at_its_interval(scheduler, stub):
    tid = await add_target_row(url=stub.url("/ok"))
    task = asyncio.create_task(scheduler._monitor_loop(TargetSpec(tid, stub.url("/ok"), 1)))
    await asyncio.sleep(3.6)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert await count_logs(tid) >= 3


async def test_loop_survives_deleted_target(scheduler, stub):
    tid = await add_target_row(url=stub.url("/ok"))
    task = asyncio.create_task(scheduler._monitor_loop(TargetSpec(tid, stub.url("/ok"), 1)))
    await asyncio.sleep(1.5)
    async with SessionFactory() as session:
        await session.execute(delete(Target).where(Target.id == tid))
        await session.commit()
    await asyncio.sleep(2)  # next checks hit a foreign-key violation, which must be absorbed
    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_sync_targets_starts_restarts_and_stops_monitors(scheduler):
    assert scheduler._monitors == {}
    tid = await add_target_row(url="http://127.0.0.1:9/", interval=60)

    await scheduler.sync_targets()
    spec, first_task = scheduler._monitors[tid]
    assert spec.check_interval == 60

    await scheduler.sync_targets()  # unchanged: same task kept
    assert scheduler._monitors[tid][1] is first_task

    async with SessionFactory() as session:
        await session.execute(update(Target).where(Target.id == tid).values(check_interval=120))
        await session.commit()
    await scheduler.sync_targets()  # changed: loop restarted with the new interval
    new_spec, new_task = scheduler._monitors[tid]
    assert new_spec.check_interval == 120 and new_task is not first_task and first_task.cancelled()

    async with SessionFactory() as session:
        await session.execute(delete(Target).where(Target.id == tid))
        await session.commit()
    await scheduler.sync_targets()
    assert tid not in scheduler._monitors


async def test_stop_cancels_every_task():
    instance = HealthCheckScheduler(
        SessionFactory, Settings(request_timeout_seconds=1, scheduler_initial_jitter_seconds=3600)
    )
    await add_target_row(name="a")
    await add_target_row(name="b")
    await instance.start()
    tasks = [task for _, task in instance._monitors.values()]
    assert len(tasks) == 2
    await instance.stop()
    assert all(task.done() for task in tasks) and instance._monitors == {}


# --- Daily rollups -------------------------------------------------------------------
async def get_aggregates(target_id: int) -> list[UptimeDailyAggregate]:
    async with SessionFactory() as session:
        return list(
            (
                await session.execute(
                    select(UptimeDailyAggregate)
                    .where(UptimeDailyAggregate.target_id == target_id)
                    .order_by(UptimeDailyAggregate.day)
                )
            ).scalars()
        )


async def test_rollup_computes_exact_statistics():
    tid = await add_target_row()
    healthy = [10.0, 20.0, 30.0, 40.0, 50.0]
    rows = [dict(timestamp=day_at(3, minute=i), response_time_ms=v, is_healthy=True) for i, v in enumerate(healthy)]
    rows += [dict(timestamp=day_at(3, minute=10 + i), response_time_ms=999.0, is_healthy=False) for i in range(2)]
    await add_logs(tid, rows)

    await rollup_daily_aggregates(SessionFactory, days_back=5)
    (agg,) = await get_aggregates(tid)
    assert (agg.total_checks, agg.healthy_checks) == (7, 5)
    assert agg.uptime_percentage == pytest.approx(71.4286, abs=1e-3)
    # Latency stats exclude the two failures (999ms) entirely.
    assert (agg.min_response_time_ms, agg.avg_response_time_ms, agg.max_response_time_ms) == (10.0, 30.0, 50.0)
    assert agg.p95_response_time_ms == pytest.approx(48.0)  # continuous percentile: 40 + 0.8 * (50 - 40)


async def test_rollup_is_an_idempotent_upsert():
    tid = await add_target_row()
    await add_logs(tid, [dict(timestamp=day_at(1, minute=0), response_time_ms=10.0, is_healthy=True)])
    await rollup_daily_aggregates(SessionFactory, days_back=3)
    await rollup_daily_aggregates(SessionFactory, days_back=3)
    assert len(await get_aggregates(tid)) == 1

    await add_logs(tid, [dict(timestamp=day_at(1, minute=5), response_time_ms=30.0, is_healthy=False)])
    await rollup_daily_aggregates(SessionFactory, days_back=3)
    (agg,) = await get_aggregates(tid)
    assert (agg.total_checks, agg.healthy_checks) == (2, 1)  # updated in place, no duplicate row


async def test_rollup_splits_days_at_utc_midnight():
    tid = await add_target_row()
    await add_logs(
        tid,
        [
            dict(timestamp=day_at(2, 23, 59, 59, 999_999), response_time_ms=10.0, is_healthy=True),
            dict(timestamp=day_at(1, 0, 0, 0, 0), response_time_ms=20.0, is_healthy=True),
        ],
    )
    await rollup_daily_aggregates(SessionFactory, days_back=5)
    first, second = await get_aggregates(tid)
    assert first.day == (utcnow().date() - timedelta(days=2)) and first.total_checks == 1
    assert second.day == (utcnow().date() - timedelta(days=1)) and second.total_checks == 1


async def test_rollup_ignores_days_outside_the_window():
    tid = await add_target_row()
    await add_logs(tid, [dict(timestamp=day_at(10), response_time_ms=10.0, is_healthy=True)])
    await rollup_daily_aggregates(SessionFactory, days_back=3)
    assert await get_aggregates(tid) == []


async def test_all_failed_day_has_zero_uptime_and_null_latency():
    tid = await add_target_row()
    await add_logs(tid, [dict(timestamp=day_at(1, minute=i), response_time_ms=5.0, is_healthy=False) for i in range(3)])
    await rollup_daily_aggregates(SessionFactory, days_back=2)
    (agg,) = await get_aggregates(tid)
    assert agg.uptime_percentage == 0.0 and agg.healthy_checks == 0
    assert agg.avg_response_time_ms is None and agg.p95_response_time_ms is None


async def test_rollup_handles_multiple_targets_in_one_pass():
    a, b = await add_target_row(name="a"), await add_target_row(name="b")
    await add_logs(a, [dict(timestamp=day_at(1), response_time_ms=10.0, is_healthy=True)])
    await add_logs(b, [dict(timestamp=day_at(1), response_time_ms=90.0, is_healthy=False)])
    await rollup_daily_aggregates(SessionFactory, days_back=2)
    assert (await get_aggregates(a))[0].uptime_percentage == 100.0
    assert (await get_aggregates(b))[0].uptime_percentage == 0.0


# --- Retention -----------------------------------------------------------------------
async def test_prune_deletes_whole_days_only():
    tid = await add_target_row()
    today = utcnow().date()
    keep_edge = datetime.combine(today - timedelta(days=14), dtime(0, 0, 0), tzinfo=timezone.utc)  # first kept second
    drop_edge = keep_edge - timedelta(microseconds=1)  # last dropped moment
    await add_logs(
        tid,
        [
            dict(timestamp=keep_edge, response_time_ms=1.0, is_healthy=True),
            dict(timestamp=drop_edge, response_time_ms=1.0, is_healthy=True),
            dict(timestamp=day_at(20), response_time_ms=1.0, is_healthy=True),
            dict(timestamp=day_at(0, minute=0), response_time_ms=1.0, is_healthy=True),
        ],
    )
    logs_deleted, _ = await prune_old_data(SessionFactory, raw_retention_days=14, aggregate_retention_days=30)
    assert logs_deleted == 2 and await count_logs(tid) == 2


async def test_history_survives_pruning_of_raw_logs():
    tid = await add_target_row()
    await add_logs(tid, [dict(timestamp=day_at(20, minute=i), response_time_ms=10.0, is_healthy=True) for i in range(4)])
    await rollup_daily_aggregates(SessionFactory, days_back=25)
    await prune_old_data(SessionFactory, raw_retention_days=14, aggregate_retention_days=30)
    assert await count_logs(tid) == 0

    await rollup_daily_aggregates(SessionFactory, days_back=25)  # must NOT overwrite with zeros
    (agg,) = await get_aggregates(tid)
    assert agg.total_checks == 4 and agg.uptime_percentage == 100.0


async def test_prune_removes_expired_aggregates():
    tid = await add_target_row()
    async with SessionFactory() as session:
        session.add_all(
            UptimeDailyAggregate(
                target_id=tid, day=utcnow().date() - timedelta(days=age),
                total_checks=1, healthy_checks=1, uptime_percentage=100.0,
            )
            for age in (5, 29, 31, 60)
        )
        await session.commit()
    _, aggs_deleted = await prune_old_data(SessionFactory, raw_retention_days=14, aggregate_retention_days=30)
    assert aggs_deleted == 2 and len(await get_aggregates(tid)) == 2


# --- Maintenance loop and lifecycle idempotency ----------------------------------------
async def test_maintenance_loop_reconciles_rolls_up_and_prunes():
    instance = HealthCheckScheduler(
        SessionFactory,
        Settings(request_timeout_seconds=1, scheduler_initial_jitter_seconds=3600, maintenance_interval_seconds=1),
    )
    await instance.start()
    try:
        # Created behind the scheduler's back: only the maintenance loop can notice it.
        tid = await add_target_row(name="late-arrival")
        await add_logs(tid, [
            dict(timestamp=day_at(1), response_time_ms=10.0, is_healthy=True),
            dict(timestamp=day_at(20), response_time_ms=10.0, is_healthy=True),  # beyond raw retention
        ])
        assert tid not in instance._monitors

        await asyncio.sleep(1.8)  # one maintenance cycle

        assert tid in instance._monitors  # reconciled
        assert len(await get_aggregates(tid)) == 1  # yesterday rolled up
        assert await count_logs(tid) == 1  # the 20-day-old raw log was pruned
    finally:
        await instance.stop()


async def test_maintenance_loop_survives_a_failing_cycle(monkeypatch):
    calls = {"n": 0}

    async def flaky(*args, **kwargs):
        calls["n"] += 1
        raise RuntimeError("simulated database hiccup")

    monkeypatch.setattr("app.scheduler.rollup_daily_aggregates", flaky)
    instance = HealthCheckScheduler(
        SessionFactory,
        Settings(request_timeout_seconds=1, scheduler_initial_jitter_seconds=3600, maintenance_interval_seconds=1),
    )
    # start() also calls the (patched) rollup; that failure must be tolerated as well.
    await instance.start()
    try:
        await asyncio.sleep(2.5)
        assert calls["n"] >= 3  # startup attempt plus at least two failed cycles; loop kept going
        assert not instance._maintenance_task.done()
    finally:
        await instance.stop()


async def test_start_and_stop_are_idempotent():
    instance = HealthCheckScheduler(
        SessionFactory, Settings(request_timeout_seconds=1, scheduler_initial_jitter_seconds=3600)
    )
    await instance.start()
    await instance.start()  # second call is a no-op, not a second set of tasks
    await instance.stop()
    await instance.stop()  # also harmless
    assert instance._monitors == {} and instance._maintenance_task is None


async def test_sync_targets_before_start_does_nothing():
    instance = HealthCheckScheduler(SessionFactory, Settings(request_timeout_seconds=1))
    await add_target_row()
    await instance.sync_targets()
    assert instance._monitors == {}
