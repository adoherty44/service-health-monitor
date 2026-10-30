"""Demo data seeding and application startup behaviour."""

import random
from datetime import timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from app import main
from app.database import SessionFactory
from app.models import HealthLog, Target, UptimeDailyAggregate, utcnow
from app.seed import DEMO_TARGETS, HISTORY_STEP_SECONDS, _generate_history, seed_demo_data


async def scalar(query):
    async with SessionFactory() as session:
        return (await session.execute(query)).scalar_one()


# --- Seeding ----------------------------------------------------------------------------
@pytest.mark.usefixtures("clean_db")
async def test_seed_creates_targets_history_and_aggregates():
    assert await seed_demo_data(SessionFactory, history_days=2, timeout_seconds=10) is True
    assert await scalar(select(func.count(Target.id))) == len(DEMO_TARGETS)
    expected_points = 2 * 86_400 // HISTORY_STEP_SECONDS * len(DEMO_TARGETS)
    assert await scalar(select(func.count(HealthLog.id))) == expected_points
    assert await scalar(select(func.count(UptimeDailyAggregate.id))) >= 2 * len(DEMO_TARGETS)


@pytest.mark.usefixtures("clean_db")
async def test_seed_failures_are_all_labelled_simulated():
    await seed_demo_data(SessionFactory, history_days=2, timeout_seconds=10)
    unlabelled = await scalar(
        select(func.count(HealthLog.id)).where(
            HealthLog.is_healthy.is_(False), ~HealthLog.error_message.startswith("[SIMULATED]")
        )
    )
    failures = await scalar(select(func.count(HealthLog.id)).where(HealthLog.is_healthy.is_(False)))
    assert failures > 0 and unlabelled == 0


@pytest.mark.usefixtures("clean_db")
async def test_seed_is_skipped_when_data_exists():
    assert await seed_demo_data(SessionFactory, history_days=1, timeout_seconds=10) is True
    before = await scalar(select(func.count(HealthLog.id)))
    assert await seed_demo_data(SessionFactory, history_days=1, timeout_seconds=10) is False
    assert await scalar(select(func.count(HealthLog.id))) == before


@pytest.mark.usefixtures("clean_db")
async def test_seed_without_history_only_creates_targets():
    await seed_demo_data(SessionFactory, history_days=0, timeout_seconds=10)
    assert await scalar(select(func.count(Target.id))) == len(DEMO_TARGETS)
    assert await scalar(select(func.count(HealthLog.id))) == 0


def test_generated_history_ends_before_now_and_is_ordered():
    end = utcnow() - timedelta(minutes=1)
    rows = _generate_history(1, 50.0, 1, end, 10.0, random.Random(1))
    assert len(rows) == 1440
    stamps = [r["timestamp"] for r in rows]
    assert stamps == sorted(stamps) and stamps[-1] < end
    assert all(r["response_time_ms"] >= 0 for r in rows)
    assert any(not r["is_healthy"] for r in rows) and any(r["is_healthy"] for r in rows)


# --- Startup safety net ----------------------------------------------------------------------
class _BrokenEngine:
    """Stands in for the real engine to simulate startup problems."""

    def __init__(self, error: Exception):
        self._error = error

    def connect(self):
        raise self._error


async def test_startup_explains_unreachable_database(monkeypatch):
    monkeypatch.setattr(main, "engine", _BrokenEngine(ConnectionRefusedError()))
    with pytest.raises(RuntimeError, match="docker compose up -d"):
        await main._assert_database_ready()


async def test_startup_explains_missing_tables(monkeypatch):
    monkeypatch.setattr(
        main, "engine", _BrokenEngine(ProgrammingError("SELECT 1", {}, Exception("no table")))
    )
    with pytest.raises(RuntimeError, match="alembic upgrade head"):
        await main._assert_database_ready()


async def test_readiness_reports_503_when_database_is_down(client, monkeypatch):
    monkeypatch.setattr(main, "engine", _BrokenEngine(ConnectionRefusedError()))
    response = await client.get("/ready")
    assert response.status_code == 503 and response.json() == {"status": "unavailable"}
