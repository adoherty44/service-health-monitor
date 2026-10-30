"""Test harness: a real PostgreSQL database, migrated with Alembic, plus a stub web server.

Why real Postgres? The code relies on Postgres features (FILTER, percentile_cont, date_bin,
LATERAL joins, partial indexes, ON CONFLICT) that SQLite cannot run. Mocking them away
would test nothing worth testing.

Safety: the suite DROPS the public schema of the test database on every run. It refuses
to start unless the database name ends in "_test", so it can never touch your dev data.
"""

import asyncio
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql+asyncpg://monitor:monitor@localhost:5433/monitor_test"
)

# Settings are read at import time, so configure the environment BEFORE importing `app`.
# Real environment variables take priority over .env, so the dev database is never used.
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["SEED_DEMO_TARGETS"] = "false"
os.environ["SCHEDULER_INITIAL_JITTER_SECONDS"] = "3600"  # keep background checks out of the way
os.environ["REQUEST_TIMEOUT_SECONDS"] = "2"
os.environ["LOG_LEVEL"] = "WARNING"

import httpx  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

from app import simulation  # noqa: E402
from app.database import engine  # noqa: E402
from app.main import app  # noqa: E402


# --- Database lifecycle ------------------------------------------------------------
async def _prepare_database(url) -> None:  # noqa: ANN001
    admin = create_async_engine(
        url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    try:
        async with admin.connect() as conn:
            exists = (
                await conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database}
                )
            ).scalar()
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    finally:
        await admin.dispose()

    fresh = create_async_engine(url, poolclass=NullPool)
    try:
        async with fresh.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
    finally:
        await fresh.dispose()


@pytest.fixture(scope="session")
def _database():
    """Create the test database, wipe it, and build the schema with real migrations."""
    url = make_url(TEST_DATABASE_URL)
    if not url.database or not url.database.endswith("_test"):
        pytest.exit(
            f"Refusing to run: test database name must end in '_test' (got {url.database!r}).",
            returncode=2,
        )
    try:
        asyncio.run(_prepare_database(url))
    except OSError:
        pytest.exit(
            "Cannot reach PostgreSQL for tests. Start it with: docker compose up -d db",
            returncode=2,
        )
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=BACKEND_DIR,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.exit(f"alembic upgrade head failed:\n{result.stdout}\n{result.stderr}", returncode=2)


@pytest_asyncio.fixture
async def clean_db(_database):
    """Empty every table before the test. Request this (or `client`) in any DB test."""
    async with engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE targets, health_logs, uptime_daily_aggregates RESTART IDENTITY CASCADE")
        )
    yield


@pytest.fixture(autouse=True)
def _reset_faults():
    simulation.clear_all_faults()
    yield
    simulation.clear_all_faults()


# --- API client ---------------------------------------------------------------------
@pytest_asyncio.fixture
async def client(clean_db):
    """HTTP client wired to the real app, including its lifespan (scheduler included)."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


# --- Local stub web server ----------------------------------------------------------
class _Handler(BaseHTTPRequestHandler):
    hits = 0

    def do_GET(self):  # noqa: N802
        type(self).hits += 1
        if self.path.startswith("/slow"):
            time.sleep(3)
        code = 503 if self.path.startswith("/fail") else 200
        self.send_response(code)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):  # keep test output clean
        pass


class Stub:
    def __init__(self, port: int):
        self._base = f"http://127.0.0.1:{port}"

    @property
    def hits(self) -> int:
        return _Handler.hits

    def url(self, path: str = "/ok") -> str:
        return f"{self._base}{path}"


@pytest.fixture(scope="session")
def stub():
    """/ok -> 200, /fail -> 503, /slow -> 200 after 3 seconds. Counts requests in .hits."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield Stub(server.server_port)
    server.shutdown()
