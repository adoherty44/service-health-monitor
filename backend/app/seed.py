"""Demo data: a few targets plus realistic backfilled history.

A fresh install should show something worth looking at: multi-day latency curves,
daily uptime bars, and a few past incidents. Backfilled failures are produced by the
chaos engine's own generator, so they carry the same [SIMULATED] label as live drills.
"""

import logging
import math
import random
from datetime import datetime, timedelta

from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import HealthLog, Target, utcnow
from app.scheduler import rollup_daily_aggregates
from app.simulation import FaultType, build_simulated_outcome

logger = logging.getLogger("monitor.seed")

# name, url, check_interval (s), typical latency (ms)
DEMO_TARGETS = (
    ("Monitor API (self-check)", "http://127.0.0.1:8000/health", 30, 6.0),
    ("Example Domain", "https://example.com", 60, 85.0),
    ("Wikipedia", "https://www.wikipedia.org", 60, 120.0),
)
HISTORY_STEP_SECONDS = 60  # one backfilled data point per minute
_INSERT_CHUNK = 2_000


def _generate_history(
    target_id: int,
    base_ms: float,
    days: int,
    end: datetime,
    timeout_seconds: float,
    rng: random.Random,
) -> list[dict]:
    steps = days * 86_400 // HISTORY_STEP_SECONDS
    start = end - timedelta(days=days)

    # A handful of outages and slow patches scattered across the period.
    outages = []
    for _ in range(rng.randint(2, 4)):
        length = rng.randint(3, 25)
        begin = rng.randint(0, steps - length - 1)
        outages.append((begin, begin + length, rng.choice(list(FaultType))))
    slow_patches = []
    for _ in range(2):
        length = rng.randint(10, 60)
        begin = rng.randint(0, steps - length - 1)
        slow_patches.append((begin, begin + length, rng.uniform(3.0, 5.0)))

    rows: list[dict] = []
    for i in range(steps):
        ts = start + timedelta(seconds=i * HISTORY_STEP_SECONDS)
        fault = next((f for b, e, f in outages if b <= i < e), None)
        if fault is not None:
            sim = build_simulated_outcome(fault, timeout_seconds)
            rows.append(
                dict(
                    target_id=target_id,
                    timestamp=ts,
                    response_time_ms=sim.response_time_ms,
                    is_healthy=False,
                    status_code=sim.status_code,
                    error_message=sim.error_message,
                )
            )
            continue
        # Gentle daily rhythm, log-normal noise, rare spikes, occasional slow patches.
        daily = 1.0 + 0.25 * math.sin(2 * math.pi * (ts.hour + ts.minute / 60) / 24)
        latency = base_ms * daily * rng.lognormvariate(0, 0.25)
        if rng.random() < 0.01:
            latency *= rng.uniform(2.0, 4.0)
        for b, e, multiplier in slow_patches:
            if b <= i < e:
                latency *= multiplier
        rows.append(
            dict(
                target_id=target_id,
                timestamp=ts,
                response_time_ms=round(latency, 3),
                is_healthy=True,
                status_code=200,
                error_message=None,
            )
        )
    return rows


async def seed_demo_data(
    session_factory: async_sessionmaker[AsyncSession], history_days: int, timeout_seconds: float
) -> bool:
    """Seed demo targets (and history) into an empty database. Returns False if skipped."""
    async with session_factory() as session:
        if (await session.execute(select(func.count(Target.id)))).scalar_one():
            return False
        targets = [Target(name=n, url=u, check_interval=i) for n, u, i, _ in DEMO_TARGETS]
        session.add_all(targets)
        await session.commit()

        if history_days > 0:
            end = utcnow() - timedelta(minutes=1)  # leave room for live checks to continue it
            total = 0
            for index, (target, spec) in enumerate(zip(targets, DEMO_TARGETS, strict=True)):
                rows = _generate_history(
                    target.id,
                    spec[3],
                    history_days,
                    end,
                    timeout_seconds,
                    random.Random(42 + index),
                )
                for offset in range(0, len(rows), _INSERT_CHUNK):
                    await session.execute(insert(HealthLog), rows[offset : offset + _INSERT_CHUNK])
                total += len(rows)
            await session.commit()
            logger.info("Backfilled %d historical checks over %d day(s)", total, history_days)

    if history_days > 0:
        await rollup_daily_aggregates(session_factory, days_back=history_days)
    logger.info("Seeded %d demo target(s)", len(DEMO_TARGETS))
    return True
