"""Pydantic v2 request/response models: the API contract."""
from datetime import date, datetime
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from app.config import settings
from app.simulation import FaultType

_URL_ADAPTER = TypeAdapter(AnyHttpUrl)
_MAX_URL_LENGTH = 2048  # matches the targets.url column


def _validate_http_url(value: str) -> str:
    normalised = str(_URL_ADAPTER.validate_python(value))
    if len(normalised) > _MAX_URL_LENGTH:
        raise ValueError(f"url must be at most {_MAX_URL_LENGTH} characters")
    return normalised


# --- Faults ------------------------------------------------------------------------
class FaultInject(BaseModel):
    fault_type: FaultType
    duration_seconds: int | None = Field(
        default=None,
        ge=1,
        le=86_400,
        description="Auto-clear the fault after this many seconds. Omit for 'until cleared'.",
    )


class FaultRead(BaseModel):
    target_id: int
    fault_type: FaultType
    injected_at: datetime
    expires_at: datetime | None


class FaultClearResult(BaseModel):
    target_id: int
    cleared: bool


# --- Targets -----------------------------------------------------------------------
class TargetCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=100)
    url: str
    check_interval: int = Field(
        default=30,
        ge=settings.min_check_interval_seconds,
        le=settings.max_check_interval_seconds,
        description="Seconds between checks.",
    )

    @field_validator("url")
    @classmethod
    def _url_must_be_http(cls, value: str) -> str:
        return _validate_http_url(value)


class TargetUpdate(BaseModel):
    """PATCH semantics: omitted (or null) fields are left unchanged."""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str | None = Field(default=None, min_length=1, max_length=100)
    url: str | None = None
    check_interval: int | None = Field(
        default=None,
        ge=settings.min_check_interval_seconds,
        le=settings.max_check_interval_seconds,
    )

    @field_validator("url")
    @classmethod
    def _url_must_be_http(cls, value: str | None) -> str | None:
        return None if value is None else _validate_http_url(value)


class TargetRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    url: str
    check_interval: int
    created_at: datetime
    active_fault: FaultRead | None = None


# --- Health logs -------------------------------------------------------------------
class HealthLogRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    target_id: int
    timestamp: datetime
    response_time_ms: float
    is_healthy: bool
    status_code: int | None
    error_message: str | None


class HealthLogPage(BaseModel):
    items: list[HealthLogRead]
    # Opaque token: pass it back as ?cursor= to fetch the next (older) page.
    next_cursor: str | None


# --- Analytics ---------------------------------------------------------------------
class AnalyticsSummary(BaseModel):
    total_checks: int
    healthy_checks: int
    failed_checks: int
    uptime_percentage: float | None
    # Latency statistics cover healthy checks only.
    avg_response_time_ms: float | None
    min_response_time_ms: float | None
    max_response_time_ms: float | None
    p50_response_time_ms: float | None
    p95_response_time_ms: float | None
    p99_response_time_ms: float | None


class AnalyticsBucket(BaseModel):
    bucket_start: datetime
    total_checks: int
    healthy_checks: int
    uptime_percentage: float
    avg_response_time_ms: float | None
    max_response_time_ms: float | None


class AnalyticsResponse(BaseModel):
    target_id: int
    start: datetime
    end: datetime
    bucket_seconds: int
    summary: AnalyticsSummary
    buckets: list[AnalyticsBucket]


class DailyAggregateRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    target_id: int
    day: date
    total_checks: int
    healthy_checks: int
    uptime_percentage: float
    avg_response_time_ms: float | None
    min_response_time_ms: float | None
    max_response_time_ms: float | None
    p95_response_time_ms: float | None


class OverviewItem(BaseModel):
    target: TargetRead
    status: Literal["UP", "DOWN", "UNKNOWN"]
    latest_check: HealthLogRead | None
    checks_24h: int
    uptime_24h: float | None
