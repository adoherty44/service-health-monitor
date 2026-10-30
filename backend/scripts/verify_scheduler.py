"""Stage 5 check: runs the real scheduler against a local stub server and your Postgres.

Takes about 30 seconds. Creates a throwaway target and deletes it at the end.
"""

import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from sqlalchemy import delete, func, select

from app import simulation
from app.config import Settings
from app.database import SessionFactory, engine
from app.models import HealthLog, Target, UptimeDailyAggregate
from app.scheduler import HealthCheckScheduler, prune_old_data, rollup_daily_aggregates

NAME = "stage5-demo"


class Stub(BaseHTTPRequestHandler):
    hits = 0

    def do_GET(self):  # noqa: N802
        type(self).hits += 1
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


async def count_logs(target_id: int, **filters) -> int:
    async with SessionFactory() as s:
        q = select(func.count(HealthLog.id)).where(HealthLog.target_id == target_id)
        if "healthy" in filters:
            q = q.where(HealthLog.is_healthy.is_(filters["healthy"]))
        return (await s.execute(q)).scalar_one()


async def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8765), Stub)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    async with SessionFactory() as s:
        await s.execute(delete(Target).where(Target.name == NAME))  # clean up any earlier run
        target = Target(name=NAME, url="http://127.0.0.1:8765/", check_interval=5)
        s.add(target)
        await s.commit()
    tid = target.id

    cfg = Settings(request_timeout_seconds=2, scheduler_initial_jitter_seconds=0)
    scheduler = HealthCheckScheduler(SessionFactory, cfg)
    await scheduler.start()

    print("1) Polling a healthy target for 11s (interval 5s, expect >= 3 checks)...")
    await asyncio.sleep(11)
    healthy = await count_logs(tid, healthy=True)
    print(f"   healthy checks stored: {healthy}  ->", "PASS" if healthy >= 3 else "FAIL")

    print("2) Injecting a TIMEOUT fault for 6s (expect simulated failures, zero real requests)...")
    hits_before = Stub.hits
    simulation.inject_fault(tid, simulation.FaultType.TIMEOUT)
    await asyncio.sleep(6)
    failed = await count_logs(tid, healthy=False)
    async with SessionFactory() as s:
        msg = (
            await s.execute(
                select(HealthLog.error_message)
                .where(HealthLog.target_id == tid, HealthLog.is_healthy.is_(False))
                .limit(1)
            )
        ).scalar_one_or_none()
    print(f"   failed checks stored: {failed} | message: {msg}")
    print(
        "   real requests during outage:",
        Stub.hits - hits_before,
        "->",
        "PASS" if Stub.hits == hits_before and failed >= 1 else "FAIL",
    )

    print("3) Clearing the fault and forcing a check (expect recovery)...")
    simulation.clear_fault(tid)
    log = await scheduler.run_check_now(tid)
    print(
        f"   healthy={log.is_healthy} status={log.status_code} latency={log.response_time_ms}ms ->",
        "PASS" if log.is_healthy else "FAIL",
    )

    print("4) Rolling up daily aggregates (single SQL statement)...")
    rows = await rollup_daily_aggregates(SessionFactory, days_back=1)
    async with SessionFactory() as s:
        agg = (
            await s.execute(
                select(UptimeDailyAggregate).where(UptimeDailyAggregate.target_id == tid)
            )
        ).scalar_one()
    print(
        f"   rows upserted: {rows} | day={agg.day} total={agg.total_checks} healthy={agg.healthy_checks} "
        f"uptime={agg.uptime_percentage:.1f}% avg={agg.avg_response_time_ms:.2f}ms p95={agg.p95_response_time_ms:.2f}ms"
    )
    print("   ->", "PASS" if agg.total_checks == healthy + failed + 1 else "FAIL")

    print("5) Rolling up again must update in place, not duplicate...")
    await rollup_daily_aggregates(SessionFactory, days_back=1)
    async with SessionFactory() as s:
        n = (
            await s.execute(
                select(func.count(UptimeDailyAggregate.id)).where(
                    UptimeDailyAggregate.target_id == tid
                )
            )
        ).scalar_one()
    print(f"   aggregate rows for this target: {n} ->", "PASS" if n == 1 else "FAIL")

    print("6) Pruning with default retention must not delete today's data...")
    logs_deleted, _ = await prune_old_data(
        SessionFactory, cfg.raw_log_retention_days, cfg.aggregate_retention_days
    )
    print(f"   logs deleted: {logs_deleted} ->", "PASS" if await count_logs(tid) > 0 else "FAIL")

    await scheduler.stop()
    async with SessionFactory() as s:
        await s.execute(delete(Target).where(Target.id == tid))  # cascades logs and aggregates
        await s.commit()
    server.shutdown()
    await engine.dispose()
    print("Done. Throwaway target removed.")


asyncio.run(main())
