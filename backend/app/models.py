"""Database schema (SQLAlchemy 2.0 declarative, PostgreSQL).

Tables
------
* targets                 - what we monitor.
* health_logs             - one row per executed (or simulated) check; high volume.
* uptime_daily_aggregates - one row per target per UTC day; survives raw-log pruning.
"""
from datetime import date, datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Deterministic constraint/index names keep Alembic migrations clean and reproducible.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def utcnow() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Target(Base):
    """A website or API endpoint that is polled on a fixed interval."""

    __tablename__ = "targets"
    __table_args__ = (
        CheckConstraint("check_interval > 0", name="check_interval_positive"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    # Seconds between checks.
    check_interval: Mapped[int] = mapped_column(
        Integer, nullable=False, default=30, server_default=text("30")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )


class HealthLog(Base):
    """The outcome of a single check.

    response_time_ms is always populated: for real failures it is the time spent
    before the failure was detected; for simulated faults it is a realistic
    fabricated value (the full timeout for TIMEOUT).
    """

    __tablename__ = "health_logs"
    __table_args__ = (
        CheckConstraint("response_time_ms >= 0", name="latency_non_negative"),
    )

    # BIGINT: this is the high-volume table and must not hit a 32-bit ceiling.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    target_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("targets.id", ondelete="CASCADE"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )
    response_time_ms: Mapped[float] = mapped_column(Float, nullable=False)
    is_healthy: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)


# Hot path: WHERE target_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp DESC
Index("ix_health_logs_target_id_timestamp_desc", HealthLog.target_id, HealthLog.timestamp.desc())

# Partial index: only failed checks. Tiny, and makes incident queries fast.
Index(
    "ix_health_logs_failures",
    HealthLog.target_id,
    HealthLog.timestamp.desc(),
    postgresql_where=HealthLog.is_healthy.is_(False),
)


class UptimeDailyAggregate(Base):
    """Per-target, per-UTC-day rollup of health_logs.

    Latency statistics cover healthy checks only, so an outage does not distort
    "how fast is it when it works". uptime_percentage covers all checks.
    """

    __tablename__ = "uptime_daily_aggregates"
    __table_args__ = (UniqueConstraint("target_id", "day"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    target_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("targets.id", ondelete="CASCADE"), nullable=False
    )
    day: Mapped[date] = mapped_column(Date, nullable=False)
    total_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    healthy_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    uptime_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_response_time_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_response_time_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_response_time_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_response_time_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
        server_default=func.now(),
    )


# Dashboard lookup: "last 30 days for target X, newest first".
Index(
    "ix_uptime_daily_aggregates_target_id_day_desc",
    UptimeDailyAggregate.target_id,
    UptimeDailyAggregate.day.desc(),
)
