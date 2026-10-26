"""Helpers shared by test modules."""
from app.database import SessionFactory
from app.models import HealthLog, Target


async def make_target(client, url: str, name: str = "target", interval: int = 3600) -> dict:
    """Create a target through the API and return its JSON."""
    response = await client.post(
        "/api/targets", json={"name": name, "url": url, "check_interval": interval}
    )
    assert response.status_code == 201, response.text
    return response.json()


async def add_target_row(name: str = "row", url: str = "http://127.0.0.1:9/", interval: int = 3600) -> int:
    """Insert a target directly (bypassing the API) and return its id."""
    async with SessionFactory() as session:
        target = Target(name=name, url=url, check_interval=interval)
        session.add(target)
        await session.commit()
        return target.id


async def add_logs(target_id: int, rows: list[dict]) -> None:
    """Insert health_logs rows directly. Each dict needs timestamp, response_time_ms, is_healthy."""
    async with SessionFactory() as session:
        session.add_all(
            HealthLog(
                target_id=target_id,
                timestamp=r["timestamp"],
                response_time_ms=r["response_time_ms"],
                is_healthy=r["is_healthy"],
                status_code=r.get("status_code", 200 if r["is_healthy"] else 500),
                error_message=r.get("error_message"),
            )
            for r in rows
        )
        await session.commit()
