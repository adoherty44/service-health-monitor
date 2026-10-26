"""Stateful fault-injection ("chaos") engine.

INJECTED_FAULTS maps target_id -> FaultType. The scheduler calls
get_active_fault() before every check; if a fault is present it asks
build_simulated_outcome() for a fabricated result and never fires the real
HTTP request. Clearing a fault (manually, via expiry, or by deleting the target)
makes the next check hit the real endpoint again, which demonstrates recovery.

Concurrency model: the state is plain in-process dictionaries shared by one
asyncio event loop. Every function below is synchronous (no await), so each
mutation is atomic with respect to other coroutines and no locking is needed.
The state is per process, so run a single worker.
"""
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

# Fabricated messages start with this marker so simulated failures are
# unmistakable in logs, SQL, and the UI.
SIMULATED_PREFIX = "[SIMULATED] "


class FaultType(str, Enum):
    TIMEOUT = "TIMEOUT"
    SERVER_ERROR = "500_ERROR"
    DNS_FAILURE = "DNS_FAILURE"


@dataclass
class FaultDetails:
    """Metadata describing an active fault."""

    target_id: int
    fault_type: FaultType
    injected_at: datetime
    expires_at: datetime | None  # None = until explicitly cleared


@dataclass(frozen=True)
class SimulatedOutcome:
    """A fabricated check result, mirroring what a real failure looks like."""

    response_time_ms: float
    status_code: int | None
    error_message: str


# --- State ---------------------------------------------------------------------
INJECTED_FAULTS: dict[int, FaultType] = {}
_FAULT_DETAILS: dict[int, FaultDetails] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def inject_fault(
    target_id: int, fault_type: FaultType, duration_seconds: int | None = None
) -> FaultDetails:
    """Activate (or replace) a fault for target_id.

    duration_seconds makes the outage self-healing: once it elapses the fault
    disappears on the next lookup and the target recovers on its own.
    """
    if duration_seconds is not None and duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    now = _now()
    details = FaultDetails(
        target_id=target_id,
        fault_type=fault_type,
        injected_at=now,
        expires_at=now + timedelta(seconds=duration_seconds) if duration_seconds else None,
    )
    INJECTED_FAULTS[target_id] = fault_type
    _FAULT_DETAILS[target_id] = details
    return details


def clear_fault(target_id: int) -> bool:
    """Remove a fault. Returns True if one was active."""
    existed = target_id in INJECTED_FAULTS
    INJECTED_FAULTS.pop(target_id, None)
    _FAULT_DETAILS.pop(target_id, None)
    return existed


def clear_all_faults() -> None:
    INJECTED_FAULTS.clear()
    _FAULT_DETAILS.clear()


def get_fault_details(target_id: int) -> FaultDetails | None:
    """Return fault metadata, lazily expiring timed faults."""
    details = _FAULT_DETAILS.get(target_id)
    if details is None:
        # Defensive: keep both structures consistent if the dict was touched directly.
        INJECTED_FAULTS.pop(target_id, None)
        return None
    if details.expires_at is not None and _now() >= details.expires_at:
        clear_fault(target_id)
        return None
    return details


def get_active_fault(target_id: int) -> FaultType | None:
    """The hook used by the scheduler: the active fault type, or None."""
    details = get_fault_details(target_id)
    return details.fault_type if details else None


def list_active_faults() -> list[FaultDetails]:
    active: list[FaultDetails] = []
    for target_id in list(_FAULT_DETAILS):
        details = get_fault_details(target_id)
        if details is not None:
            active.append(details)
    return sorted(active, key=lambda d: d.target_id)


def build_simulated_outcome(fault_type: FaultType, timeout_seconds: float) -> SimulatedOutcome:
    """Fabricate a realistic failure without touching the network."""
    if fault_type is FaultType.TIMEOUT:
        # A real timeout burns the whole budget; fabricate it instantly instead of sleeping.
        return SimulatedOutcome(
            response_time_ms=round(timeout_seconds * 1000 + random.uniform(0.0, 25.0), 3),
            status_code=None,
            error_message=f"{SIMULATED_PREFIX}Request timed out after {timeout_seconds:g}s",
        )
    if fault_type is FaultType.SERVER_ERROR:
        return SimulatedOutcome(
            response_time_ms=round(random.uniform(15.0, 120.0), 3),
            status_code=500,
            error_message=f"{SIMULATED_PREFIX}HTTP 500 Internal Server Error",
        )
    if fault_type is FaultType.DNS_FAILURE:
        return SimulatedOutcome(
            response_time_ms=round(random.uniform(2.0, 40.0), 3),
            status_code=None,
            error_message=f"{SIMULATED_PREFIX}DNS resolution failed: Name or service not known",
        )
    raise ValueError(f"Unsupported fault type: {fault_type!r}")
