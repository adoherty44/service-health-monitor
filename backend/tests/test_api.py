"""REST API behaviour, end to end through the real app and a real database."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from app.database import SessionFactory
from app.models import HealthLog, UptimeDailyAggregate
from app.scheduler import rollup_daily_aggregates
from tests.helpers import add_logs, make_target


async def run_checks(client, target_id: int, count: int) -> list[dict]:
    return [(await client.post(f"/api/targets/{target_id}/check")).json() for _ in range(count)]


# --- Targets: CRUD and validation -------------------------------------------------
async def test_target_lifecycle(client, stub):
    created = await make_target(client, stub.url(), name="web")
    tid = created["id"]
    assert created["check_interval"] == 3600 and created["active_fault"] is None

    assert (await client.get(f"/api/targets/{tid}")).json()["name"] == "web"
    assert [t["id"] for t in (await client.get("/api/targets")).json()] == [tid]

    patched = await client.patch(f"/api/targets/{tid}", json={"name": "web2", "check_interval": 7200})
    assert patched.status_code == 200 and patched.json()["name"] == "web2" and patched.json()["check_interval"] == 7200
    assert patched.json()["url"] == created["url"]  # untouched field preserved

    assert (await client.delete(f"/api/targets/{tid}")).status_code == 204
    assert (await client.get(f"/api/targets/{tid}")).status_code == 404
    assert (await client.get("/api/targets")).json() == []


async def test_default_interval_is_applied(client, stub):
    response = await client.post("/api/targets", json={"name": "d", "url": stub.url()})
    assert response.status_code == 201 and response.json()["check_interval"] == 30


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "x", "url": "ftp://example.com"},
        {"name": "x", "url": "not a url"},
        {"name": "x", "url": "http://example.com", "check_interval": 1},
        {"name": "x", "url": "http://example.com", "check_interval": 999_999},
        {"name": "", "url": "http://example.com"},
        {"name": "n" * 101, "url": "http://example.com"},
        {"url": "http://example.com"},
    ],
)
async def test_invalid_target_payloads_are_rejected(client, payload):
    assert (await client.post("/api/targets", json=payload)).status_code == 422


async def test_name_is_trimmed_and_url_normalised(client):
    created = (await client.post("/api/targets", json={"name": "  spaced  ", "url": "https://example.com"})).json()
    assert created["name"] == "spaced" and created["url"] == "https://example.com/"


async def test_duplicate_names_conflict_on_create_and_rename(client, stub):
    await make_target(client, stub.url(), name="one")
    other = await make_target(client, stub.url(), name="two")
    assert (await client.post("/api/targets", json={"name": "one", "url": stub.url()})).status_code == 409
    assert (await client.patch(f"/api/targets/{other['id']}", json={"name": "one"})).status_code == 409


@pytest.mark.parametrize(
    "method, path, body",
    [
        ("get", "/api/targets/999", None),
        ("patch", "/api/targets/999", {"name": "x"}),
        ("delete", "/api/targets/999", None),
        ("post", "/api/targets/999/check", None),
        ("get", "/api/targets/999/logs", None),
        ("get", "/api/targets/999/analytics", None),
        ("get", "/api/targets/999/aggregates", None),
        ("post", "/api/targets/999/faults/inject", {"fault_type": "TIMEOUT"}),
        ("post", "/api/targets/999/faults/clear", None),
    ],
)
async def test_unknown_target_returns_404_everywhere(client, method, path, body):
    response = await client.request(method, path, json=body)
    assert response.status_code == 404


async def test_changing_url_takes_effect_in_the_scheduler(client, stub):
    tid = (await make_target(client, stub.url("/ok"), name="mutable"))["id"]
    await client.patch(f"/api/targets/{tid}", json={"url": stub.url("/fail")})
    assert (await client.post(f"/api/targets/{tid}/check")).json()["status_code"] == 503


# --- Fault injection ------------------------------------------------------------------
async def test_inject_check_and_clear_flow(client, stub):
    tid = (await make_target(client, stub.url()))["id"]

    injected = await client.post(f"/api/targets/{tid}/faults/inject", json={"fault_type": "500_ERROR", "duration_seconds": 120})
    assert injected.status_code == 200 and injected.json()["expires_at"] is not None
    assert (await client.get(f"/api/targets/{tid}")).json()["active_fault"]["fault_type"] == "500_ERROR"
    assert [f["target_id"] for f in (await client.get("/api/faults")).json()] == [tid]

    hits = stub.hits
    down = (await client.post(f"/api/targets/{tid}/check")).json()
    assert down["is_healthy"] is False and down["status_code"] == 500 and down["error_message"].startswith("[SIMULATED]")
    assert stub.hits == hits

    assert (await client.post(f"/api/targets/{tid}/faults/clear")).json() == {"target_id": tid, "cleared": True}
    assert (await client.post(f"/api/targets/{tid}/faults/clear")).json()["cleared"] is False
    assert (await client.post(f"/api/targets/{tid}/check")).json()["is_healthy"] is True
    assert (await client.get("/api/faults")).json() == []


@pytest.mark.parametrize(
    "body", [{"fault_type": "NOPE"}, {}, {"fault_type": "TIMEOUT", "duration_seconds": 0}, {"fault_type": "TIMEOUT", "duration_seconds": 999_999}]
)
async def test_invalid_fault_requests_are_rejected(client, stub, body):
    tid = (await make_target(client, stub.url()))["id"]
    assert (await client.post(f"/api/targets/{tid}/faults/inject", json=body)).status_code == 422


# --- Logs: keyset pagination ----------------------------------------------------------
async def test_logs_paginate_newest_first_without_gaps_or_duplicates(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    created = await run_checks(client, tid, 7)

    seen, cursor, pages = [], None, 0
    while True:
        params = {"limit": 3, **({"cursor": cursor} if cursor else {})}
        page = (await client.get(f"/api/targets/{tid}/logs", params=params)).json()
        seen += [item["id"] for item in page["items"]]
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert pages == 3 and seen == sorted((c["id"] for c in created), reverse=True)


async def test_logs_exact_page_boundary_has_no_phantom_page(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    await run_checks(client, tid, 4)
    page = (await client.get(f"/api/targets/{tid}/logs", params={"limit": 4})).json()
    assert len(page["items"]) == 4 and page["next_cursor"] is None


async def test_logs_filtering_and_validation(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    await run_checks(client, tid, 2)
    await client.post(f"/api/targets/{tid}/faults/inject", json={"fault_type": "TIMEOUT"})
    await run_checks(client, tid, 1)

    failed = (await client.get(f"/api/targets/{tid}/logs", params={"healthy": "false"})).json()["items"]
    ok = (await client.get(f"/api/targets/{tid}/logs", params={"healthy": "true"})).json()["items"]
    assert len(failed) == 1 and len(ok) == 2

    assert (await client.get(f"/api/targets/{tid}/logs", params={"cursor": "garbage"})).status_code == 422
    assert (await client.get(f"/api/targets/{tid}/logs", params={"limit": 0})).status_code == 422
    assert (await client.get(f"/api/targets/{tid}/logs", params={"limit": 1001})).status_code == 422


async def test_logs_respect_time_window(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    now = datetime.now(timezone.utc)
    await add_logs(tid, [
        dict(timestamp=now - timedelta(hours=30), response_time_ms=1.0, is_healthy=True),
        dict(timestamp=now - timedelta(hours=2), response_time_ms=1.0, is_healthy=True),
    ])
    default = (await client.get(f"/api/targets/{tid}/logs")).json()["items"]
    assert len(default) == 1  # default window is the last 24h
    wide = await client.get(f"/api/targets/{tid}/logs", params={"start": (now - timedelta(hours=48)).isoformat(), "end": now.isoformat()})
    assert len(wide.json()["items"]) == 2


# --- Analytics --------------------------------------------------------------------------
async def test_analytics_exact_statistics(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    now = datetime.now(timezone.utc)
    rows = [dict(timestamp=now - timedelta(minutes=i + 1), response_time_ms=v, is_healthy=True) for i, v in enumerate([10, 20, 30, 40, 50])]
    rows.append(dict(timestamp=now - timedelta(minutes=9), response_time_ms=700.0, is_healthy=False))
    await add_logs(tid, rows)

    s = (await client.get(f"/api/targets/{tid}/analytics")).json()["summary"]
    assert (s["total_checks"], s["healthy_checks"], s["failed_checks"]) == (6, 5, 1)
    assert s["uptime_percentage"] == pytest.approx(83.3333, abs=1e-3)
    assert (s["min_response_time_ms"], s["avg_response_time_ms"], s["max_response_time_ms"]) == (10.0, 30.0, 50.0)
    assert s["p50_response_time_ms"] == 30.0 and s["p95_response_time_ms"] == pytest.approx(48.0) and s["p99_response_time_ms"] == pytest.approx(49.6)


async def test_analytics_buckets_are_aligned_and_counted(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    base = int(datetime.now(timezone.utc).timestamp() // 60) * 60 - 180  # a 60s boundary, 3 minutes ago
    at = lambda offset: datetime.fromtimestamp(base + offset, tz=timezone.utc)  # noqa: E731
    await add_logs(tid, [
        dict(timestamp=at(1), response_time_ms=10.0, is_healthy=True),
        dict(timestamp=at(30), response_time_ms=30.0, is_healthy=False),
        dict(timestamp=at(61), response_time_ms=20.0, is_healthy=True),
    ])
    data = (await client.get(f"/api/targets/{tid}/analytics", params={"bucket_seconds": 60})).json()
    first, second = data["buckets"]
    assert datetime.fromisoformat(first["bucket_start"].replace("Z", "+00:00")).timestamp() == base
    assert (first["total_checks"], first["healthy_checks"], first["uptime_percentage"]) == (2, 1, 50.0)
    assert datetime.fromisoformat(second["bucket_start"].replace("Z", "+00:00")).timestamp() == base + 60
    assert (second["total_checks"], second["avg_response_time_ms"]) == (1, 20.0)


async def test_analytics_with_no_data_and_default_bucket(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    data = (await client.get(f"/api/targets/{tid}/analytics")).json()
    assert data["summary"]["total_checks"] == 0 and data["summary"]["uptime_percentage"] is None
    assert data["buckets"] == [] and data["bucket_seconds"] == 900  # 24h window -> 96 buckets


@pytest.mark.parametrize(
    "params",
    [
        {"start": "2030-01-02T00:00:00Z", "end": "2030-01-01T00:00:00Z"},   # start after end
        {"start": "2020-01-01T00:00:00Z", "end": "2030-01-01T00:00:00Z"},   # window > 31 days
        {"bucket_seconds": 1},                                              # 86,400 buckets
        {"bucket_seconds": 0},
        {"bucket_seconds": 86_401},
    ],
)
async def test_analytics_rejects_bad_parameters(client, stub, params):
    tid = (await make_target(client, stub.url()))["id"]
    assert (await client.get(f"/api/targets/{tid}/analytics", params=params)).status_code == 422


async def test_naive_timestamps_are_treated_as_utc(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    now = datetime.now(timezone.utc)
    await add_logs(tid, [dict(timestamp=now - timedelta(minutes=5), response_time_ms=1.0, is_healthy=True)])
    start = (now - timedelta(hours=1)).replace(tzinfo=None).isoformat()  # no offset given
    data = (await client.get(f"/api/targets/{tid}/analytics", params={"start": start})).json()
    assert data["summary"]["total_checks"] == 1


# --- Overview and aggregates -----------------------------------------------------------
async def test_overview_reports_unknown_up_and_down(client, stub):
    fresh = (await make_target(client, stub.url(), name="a-fresh"))["id"]
    healthy = (await make_target(client, stub.url(), name="b-healthy"))["id"]
    broken = (await make_target(client, stub.url(), name="c-broken"))["id"]
    await run_checks(client, healthy, 3)
    await run_checks(client, broken, 1)
    await client.post(f"/api/targets/{broken}/faults/inject", json={"fault_type": "DNS_FAILURE"})
    await run_checks(client, broken, 1)

    items = {i["target"]["id"]: i for i in (await client.get("/api/overview")).json()}
    assert [i["target"]["id"] for i in (await client.get("/api/overview")).json()] == [fresh, healthy, broken]
    assert items[fresh]["status"] == "UNKNOWN" and items[fresh]["latest_check"] is None and items[fresh]["uptime_24h"] is None
    assert items[healthy]["status"] == "UP" and items[healthy]["uptime_24h"] == 100.0 and items[healthy]["checks_24h"] == 3
    assert items[broken]["status"] == "DOWN" and items[broken]["uptime_24h"] == 50.0
    assert items[broken]["target"]["active_fault"]["fault_type"] == "DNS_FAILURE"


async def test_overview_latest_check_is_the_newest(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    runs = await run_checks(client, tid, 3)
    latest = (await client.get("/api/overview")).json()[0]["latest_check"]
    assert latest["id"] == runs[-1]["id"]


async def test_aggregates_endpoint_returns_rollups_oldest_first(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    now = datetime.now(timezone.utc)
    await add_logs(tid, [
        dict(timestamp=now - timedelta(days=2), response_time_ms=10.0, is_healthy=True),
        dict(timestamp=now - timedelta(days=1), response_time_ms=20.0, is_healthy=False),
    ])
    await rollup_daily_aggregates(SessionFactory, days_back=5)
    rows = (await client.get(f"/api/targets/{tid}/aggregates")).json()
    assert [r["uptime_percentage"] for r in rows] == [100.0, 0.0] and rows[0]["day"] < rows[1]["day"]
    assert len((await client.get(f"/api/targets/{tid}/aggregates", params={"days": 1})).json()) == 0
    for bad in (0, 31):
        assert (await client.get(f"/api/targets/{tid}/aggregates", params={"days": bad})).status_code == 422


# --- Deletion, CORS, probes -------------------------------------------------------------
async def test_delete_cascades_history_and_clears_faults(client, stub):
    tid = (await make_target(client, stub.url()))["id"]
    await run_checks(client, tid, 2)
    await rollup_daily_aggregates(SessionFactory, days_back=1)
    await client.post(f"/api/targets/{tid}/faults/inject", json={"fault_type": "TIMEOUT"})

    assert (await client.delete(f"/api/targets/{tid}")).status_code == 204
    assert (await client.get("/api/faults")).json() == []
    other = await make_target(client, stub.url(), name="other")
    assert (await client.get("/api/overview")).json()[0]["target"]["id"] == other["id"]
    async with SessionFactory() as session:
        assert (await session.execute(select(func.count(HealthLog.id)))).scalar_one() == 0
        assert (await session.execute(select(func.count(UptimeDailyAggregate.id)))).scalar_one() == 0


async def test_cors_allows_configured_origin_only(client):
    ok = await client.options("/api/targets", headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"})
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:5173"
    bad = await client.options("/api/targets", headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in bad.headers


async def test_probes_and_openapi(client):
    assert (await client.get("/health")).json() == {"status": "ok"}
    assert (await client.get("/ready")).json() == {"status": "ready"}
    spec = (await client.get("/openapi.json")).json()
    assert "/api/targets/{target_id}/analytics" in spec["paths"] and "/api/overview" in spec["paths"]
