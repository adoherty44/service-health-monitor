"""Chaos engine: pure in-memory logic, so these run without a database."""

import socket
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app import simulation
from app.scheduler import _is_dns_failure
from app.simulation import SIMULATED_PREFIX, FaultType


def test_inject_activates_fault_and_exposes_mapping():
    simulation.inject_fault(7, FaultType.TIMEOUT)
    assert simulation.get_active_fault(7) is FaultType.TIMEOUT
    assert simulation.INJECTED_FAULTS == {7: FaultType.TIMEOUT}
    assert simulation.get_active_fault(8) is None


def test_injecting_again_replaces_the_fault():
    simulation.inject_fault(1, FaultType.TIMEOUT)
    simulation.inject_fault(1, FaultType.DNS_FAILURE)
    assert simulation.get_active_fault(1) is FaultType.DNS_FAILURE
    assert len(simulation.INJECTED_FAULTS) == 1


def test_clear_reports_whether_a_fault_existed():
    simulation.inject_fault(1, FaultType.SERVER_ERROR)
    assert simulation.clear_fault(1) is True
    assert simulation.clear_fault(1) is False
    assert simulation.get_active_fault(1) is None


def test_untimed_fault_has_no_expiry():
    details = simulation.inject_fault(1, FaultType.TIMEOUT)
    assert details.expires_at is None


def test_timed_fault_expires_lazily_and_cleans_both_structures():
    details = simulation.inject_fault(1, FaultType.TIMEOUT, duration_seconds=30)
    assert details.expires_at is not None
    assert simulation.get_active_fault(1) is FaultType.TIMEOUT
    details.expires_at = datetime.now(UTC) - timedelta(seconds=1)  # time passes
    assert simulation.get_active_fault(1) is None
    assert 1 not in simulation.INJECTED_FAULTS


@pytest.mark.parametrize("bad", [0, -5])
def test_non_positive_duration_is_rejected(bad):
    with pytest.raises(ValueError):
        simulation.inject_fault(1, FaultType.TIMEOUT, duration_seconds=bad)


def test_list_active_faults_is_sorted_and_skips_expired():
    simulation.inject_fault(3, FaultType.TIMEOUT)
    simulation.inject_fault(1, FaultType.DNS_FAILURE)
    expired = simulation.inject_fault(2, FaultType.SERVER_ERROR, duration_seconds=5)
    expired.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    assert [f.target_id for f in simulation.list_active_faults()] == [1, 3]


def test_clear_all_faults():
    simulation.inject_fault(1, FaultType.TIMEOUT)
    simulation.inject_fault(2, FaultType.TIMEOUT)
    simulation.clear_all_faults()
    assert simulation.list_active_faults() == [] and simulation.INJECTED_FAULTS == {}


@pytest.mark.parametrize(
    "fault, status_code, low, high, fragment",
    [
        (FaultType.TIMEOUT, None, 10_000, 10_025, "timed out after 10s"),
        (FaultType.SERVER_ERROR, 500, 15, 120, "HTTP 500"),
        (FaultType.DNS_FAILURE, None, 2, 40, "DNS resolution failed"),
    ],
)
def test_simulated_outcomes_look_like_real_failures(fault, status_code, low, high, fragment):
    for _ in range(50):  # latency is random, so check the whole range repeatedly
        outcome = simulation.build_simulated_outcome(fault, 10.0)
        assert outcome.status_code == status_code
        assert low <= outcome.response_time_ms <= high
        assert outcome.error_message.startswith(SIMULATED_PREFIX)
        assert fragment in outcome.error_message


def test_simulated_timeout_uses_the_configured_timeout():
    outcome = simulation.build_simulated_outcome(FaultType.TIMEOUT, 2.0)
    assert 2_000 <= outcome.response_time_ms <= 2_025


# --- DNS classification used by the scheduler (pure function) -----------------------
def test_dns_failure_detected_through_exception_chain():
    error = httpx.ConnectError("connection failed")
    error.__cause__ = socket.gaierror(8, "nodename nor servname provided")
    assert _is_dns_failure(error) is True


def test_other_connect_errors_are_not_dns_failures():
    error = httpx.ConnectError("connection refused")
    error.__cause__ = ConnectionRefusedError()
    assert _is_dns_failure(error) is False


def test_circular_exception_chain_does_not_hang():
    a, b = RuntimeError("a"), RuntimeError("b")
    a.__cause__, b.__cause__ = b, a
    assert _is_dns_failure(a) is False
