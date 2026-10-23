"""CVE-2026-114603 regression against the installed, hash-locked dependency.

Reject reintroduction of the unbounded regex-split path without wall-clock
thresholds or running the original CPU-exhaustion workload.
"""
import logging

from opentelemetry.trace import TraceState
import opentelemetry.trace.span as span
import pytest


@pytest.mark.parametrize("whitespace", [" ", "\t"])
def test_long_tracestate_whitespace_never_enters_regex_split(monkeypatch, caplog, whitespace):
    def unbounded_split(*args, **kwargs):
        pytest.fail("Untrusted long tracestate entered the vulnerable regex-split path")
    monkeypatch.setattr(span.re, "split", unbounded_split)
    with caplog.at_level(logging.CRITICAL, logger="opentelemetry.trace.span"):
        state = TraceState.from_header(["vendor=value" + whitespace * 12_000 + "invalid"])
    assert len(state) == 0


def test_telemetry_security_update_retains_valid_tracestate_parsing():
    state = TraceState.from_header(["vendor=value, other=second"])
    assert dict(state.items()) == {"vendor": "value", "other": "second"}
    assert len(TraceState.from_header(["vendor=value,vendor=duplicate"])) == 0
