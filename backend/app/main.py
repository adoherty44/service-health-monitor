"""Application entrypoint: wires the database, scheduler, CORS and routes together."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from app import simulation
from app.config import settings
from app.database import SessionFactory, engine
from app.routes import router
from app.scheduler import HealthCheckScheduler
from app.seed import seed_demo_data

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
logger = logging.getLogger("monitor.main")

OPENAPI_TAGS = [
    {"name": "targets", "description": "Manage monitored endpoints and trigger on-demand checks."},
    {
        "name": "analytics",
        "description": "Uptime history, latency percentiles and dashboard summaries.",
    },
    {"name": "simulation", "description": "Chaos engine: inject and clear simulated outages."},
    {"name": "system", "description": "Liveness and readiness probes."},
]


async def _assert_database_ready() -> None:
    """Fail fast with an actionable message instead of a cryptic traceback."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1 FROM targets LIMIT 1"))
    except OSError as exc:
        raise RuntimeError(
            "Cannot reach PostgreSQL. Is it running? Start it with: docker compose up -d db"
        ) from exc
    except ProgrammingError as exc:
        raise RuntimeError("Database tables are missing. Run: alembic upgrade head") from exc


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _assert_database_ready()
    if settings.seed_demo_targets and await seed_demo_data(
        SessionFactory, settings.seed_history_days, settings.request_timeout_seconds
    ):
        logger.info("Demo data seeded into an empty database")
    scheduler = HealthCheckScheduler(SessionFactory, settings)
    app.state.scheduler = scheduler
    await scheduler.start()
    try:
        yield
    finally:
        await scheduler.stop()
        simulation.clear_all_faults()
        await engine.dispose()


app = FastAPI(
    title="Network & Service Health Monitor",
    version="1.0.0",
    description=(
        "Async uptime and latency monitoring with a built-in chaos engine for "
        "demonstrating outage detection and recovery."
    ),
    openapi_tags=OPENAPI_TAGS,
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)


@app.get("/health", tags=["system"])
async def liveness() -> dict[str, str]:
    """Liveness: the process is up and serving. Does not touch the database."""
    return {"status": "ok"}


@app.get("/ready", tags=["system"])
async def readiness(response: Response) -> dict[str, str]:
    """Readiness: the database is reachable. Used by Docker/Kubernetes health checks."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001 - any failure means 'not ready'
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable"}
    return {"status": "ready"}
