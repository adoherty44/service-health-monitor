"""Async engine and session factory."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=settings.sql_echo,
    pool_size=10,
    max_overflow=10,
    pool_pre_ping=True,  # drop dead connections instead of erroring on them
    pool_recycle=1800,  # recycle connections every 30 minutes
)

# expire_on_commit=False keeps ORM objects readable after commit. Without it,
# touching an attribute triggers a lazy refresh, which raises MissingGreenlet
# under AsyncSession.
SessionFactory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: one session per request."""
    async with SessionFactory() as session:
        yield session
