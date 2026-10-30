"""Stage 6 check: drives the REST API end to end against your Postgres.

Takes about 5 seconds. Creates throwaway targets and deletes them at the end.
"""

import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from fastapi import FastAPI
from sqlalchemy import delete, func, select

from app import simulation
from app.config import Settings
from app.database import SessionFactory, engine
from app.models import HealthLog, Target
from app.routes import router
from app.scheduler import HealthCheckScheduler, rollup_daily_aggregates

PREFIX = "stage6-"
results: list[bool] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    ok = bool(ok)
    results.append(ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"  ({detail})" if detail else ""))


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


async def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8766), Stub)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    async with SessionFactory() as s:  # clean up any earlier run
        await s.execute(delete(Target).where(Target.name.like(f"{PREFIX}%")))
        await s.commit()

    # Minimal app: just the router plus a running scheduler (main.py comes in Stage 7).
    # A huge jitter keeps background checks out of the way so counts are deterministic.
    app = FastAPI()
    app.include_router(router)
    scheduler = HealthCheckScheduler(
        SessionFactory, Settings(request_timeout_seconds=2, scheduler_initial_jitter_seconds=3600)
    )
    app.state.scheduler = scheduler
    await scheduler.start()
    api = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

    print("Targets: create / validate / read / update")
    body = {"name": f"{PREFIX}a", "url": "http://127.0.0.1:8766/", "check_interval": 3600}
    r = await api.post("/api/targets", json=body)
    check("create returns 201", r.status_code == 201, r.text[:80] if r.status_code != 201 else "")
    tid = r.json()["id"]
    check(
        "duplicate name returns 409", (await api.post("/api/targets", json=body)).status_code == 409
    )
    check(
        "non-http url returns 422",
        (await api.post("/api/targets", json={**body, "name": "x", "url": "ftp://x"})).status_code
        == 422,
    )
    check(
        "interval below minimum returns 422",
        (
            await api.post("/api/targets", json={**body, "name": "x", "check_interval": 1})
        ).status_code
        == 422,
    )
    check("get one", (await api.get(f"/api/targets/{tid}")).json()["name"] == f"{PREFIX}a")
    check("unknown id returns 404", (await api.get("/api/targets/99999999")).status_code == 404)
    check(
        "list contains target", any(t["id"] == tid for t in (await api.get("/api/targets")).json())
    )
    r = await api.patch(f"/api/targets/{tid}", json={"check_interval": 7200})
    check("patch interval", r.status_code == 200 and r.json()["check_interval"] == 7200)

    print("Checks and chaos engine")
    for _ in range(3):
        await api.post(f"/api/targets/{tid}/check")
    r = await api.post(f"/api/targets/{tid}/faults/inject", json={"fault_type": "500_ERROR"})
    check("inject fault", r.status_code == 200 and r.json()["fault_type"] == "500_ERROR")
    check(
        "fault visible on target",
        (await api.get(f"/api/targets/{tid}")).json()["active_fault"]["fault_type"] == "500_ERROR",
    )
    check("fault listed", any(f["target_id"] == tid for f in (await api.get("/api/faults")).json()))
    hits = Stub.hits
    r = await api.post(f"/api/targets/{tid}/check")
    check(
        "simulated check is unhealthy and labelled",
        r.json()["is_healthy"] is False and r.json()["error_message"].startswith("[SIMULATED]"),
    )
    check("no real request during outage", Stub.hits == hits)
    check(
        "bad fault type returns 422",
        (
            await api.post(f"/api/targets/{tid}/faults/inject", json={"fault_type": "NOPE"})
        ).status_code
        == 422,
    )
    check(
        "clear fault",
        (await api.post(f"/api/targets/{tid}/faults/clear")).json()["cleared"] is True,
    )
    r = await api.post(f"/api/targets/{tid}/check")
    check("recovered after clear", r.json()["is_healthy"] is True)

    print("History: keyset pagination")
    page1 = (await api.get(f"/api/targets/{tid}/logs", params={"limit": 2})).json()
    check("page 1 has 2 items and a cursor", len(page1["items"]) == 2 and page1["next_cursor"])
    page2 = (
        await api.get(
            f"/api/targets/{tid}/logs", params={"limit": 2, "cursor": page1["next_cursor"]}
        )
    ).json()
    page3 = (
        await api.get(
            f"/api/targets/{tid}/logs", params={"limit": 2, "cursor": page2["next_cursor"]}
        )
    ).json()
    ids = [i["id"] for p in (page1, page2, page3) for i in p["items"]]
    check(
        "pages cover all 5 checks with no duplicates",
        len(ids) == 5 and len(set(ids)) == 5,
        f"ids={ids}",
    )
    check("newest first", ids == sorted(ids, reverse=True))
    check("last page has no cursor", page3["next_cursor"] is None)
    check(
        "bad cursor returns 422",
        (await api.get(f"/api/targets/{tid}/logs", params={"cursor": "garbage"})).status_code
        == 422,
    )
    failed = (await api.get(f"/api/targets/{tid}/logs", params={"healthy": "false"})).json()
    check("healthy=false filter", len(failed["items"]) == 1)

    print("Analytics (computed inside Postgres)")
    a = (await api.get(f"/api/targets/{tid}/analytics")).json()
    s = a["summary"]
    check(
        "summary counts",
        (s["total_checks"], s["healthy_checks"], s["failed_checks"]) == (5, 4, 1),
        str(s["total_checks"]),
    )
    check("uptime is 80%", s["uptime_percentage"] == 80.0)
    check(
        "percentiles present and ordered",
        s["p50_response_time_ms"] is not None
        and s["p50_response_time_ms"]
        <= s["p95_response_time_ms"]
        <= s["p99_response_time_ms"]
        <= s["max_response_time_ms"],
    )
    check(
        "buckets add up to 5",
        sum(b["total_checks"] for b in a["buckets"]) == 5,
        f"bucket_seconds={a['bucket_seconds']}",
    )
    a60 = (await api.get(f"/api/targets/{tid}/analytics", params={"bucket_seconds": 60})).json()
    check(
        "custom bucket size honoured",
        a60["bucket_seconds"] == 60 and sum(b["total_checks"] for b in a60["buckets"]) == 5,
    )
    check(
        "start after end returns 422",
        (
            await api.get(
                f"/api/targets/{tid}/analytics",
                params={"start": "2030-01-02T00:00:00Z", "end": "2030-01-01T00:00:00Z"},
            )
        ).status_code
        == 422,
    )
    check(
        "window over 31 days returns 422",
        (
            await api.get(
                f"/api/targets/{tid}/analytics",
                params={"start": "2020-01-01T00:00:00Z", "end": "2030-01-01T00:00:00Z"},
            )
        ).status_code
        == 422,
    )

    print("Overview and daily aggregates")
    ov = {i["target"]["id"]: i for i in (await api.get("/api/overview")).json()}
    check(
        "overview status UP with 5 checks in 24h",
        ov[tid]["status"] == "UP" and ov[tid]["checks_24h"] == 5 and ov[tid]["uptime_24h"] == 80.0,
    )
    r = await api.post(
        "/api/targets",
        json={"name": f"{PREFIX}empty", "url": "http://127.0.0.1:8766/", "check_interval": 3600},
    )
    empty_id = r.json()["id"]
    ov = {i["target"]["id"]: i for i in (await api.get("/api/overview")).json()}
    check(
        "target with no checks is UNKNOWN",
        ov[empty_id]["status"] == "UNKNOWN" and ov[empty_id]["latest_check"] is None,
    )
    await rollup_daily_aggregates(SessionFactory, days_back=1)
    agg = (await api.get(f"/api/targets/{tid}/aggregates")).json()
    check(
        "aggregates endpoint",
        len(agg) == 1
        and agg[0]["total_checks"] == 5
        and agg[0]["p95_response_time_ms"] is not None,
    )

    print("Delete")
    check("delete returns 204", (await api.delete(f"/api/targets/{tid}")).status_code == 204)
    check("deleted target returns 404", (await api.get(f"/api/targets/{tid}")).status_code == 404)
    async with SessionFactory() as s:
        left = (
            await s.execute(select(func.count(HealthLog.id)).where(HealthLog.target_id == tid))
        ).scalar_one()
    check("logs cascade-deleted", left == 0)
    check("fault state cleaned up", tid not in simulation.INJECTED_FAULTS)

    await api.delete(f"/api/targets/{empty_id}")
    await api.aclose()
    await scheduler.stop()
    server.shutdown()
    await engine.dispose()
    print(f"\n{sum(results)}/{len(results)} checks passed")


asyncio.run(main())
