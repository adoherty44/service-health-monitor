"""Wipe ALL targets and history, then re-seed the demo data.

Usage:  python -m scripts.reset_demo --yes
"""

import argparse
import asyncio

from sqlalchemy import delete

from app.config import settings
from app.database import SessionFactory, engine
from app.models import Target
from app.seed import seed_demo_data


async def main(confirmed: bool) -> None:
    if not confirmed:
        print("This deletes ALL targets and their history. Re-run with --yes to confirm.")
        return
    async with SessionFactory() as session:
        await session.execute(delete(Target))  # logs and aggregates cascade
        await session.commit()
    await seed_demo_data(
        SessionFactory, settings.seed_history_days, settings.request_timeout_seconds
    )
    print(f"Demo data reset ({settings.seed_history_days} day(s) of history).")
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yes", action="store_true", help="confirm the destructive reset")
    asyncio.run(main(parser.parse_args().yes))
