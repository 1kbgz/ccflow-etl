from __future__ import annotations

from datetime import datetime
from typing import get_args

import pytest

from ccflow_etl import Interval, Offset

TESTED_OFFSETS = {
    "B",
    "C",
    "D",
    "M",
    "W",
    "ME",
    "SME",
    "BME",
    "CBME",
    "MS",
    "SMS",
    "BMS",
    "CBMS",
    "QE",
    "BQE",
    "QS",
    "BQS",
    "YE",
    "BYE",
    "YS",
    "BYS",
    "h",
    "bh",
    "cbh",
    "min",
    "s",
    "ms",
    "us",
    "ns",
}


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def test_all_declared_offsets_have_explicit_step_coverage():
    assert set(get_args(Offset)) == TESTED_OFFSETS


def test_interval_parses_count_and_offset_from_string():
    interval = Interval.model_validate("15D")

    assert interval.n == 15
    assert interval.offset == "D"


def test_interval_defaults_count_to_one_when_string_has_only_offset():
    interval = Interval.model_validate("D")

    assert interval.n == 1
    assert interval.offset == "D"


def test_interval_accepts_named_daily_alias():
    interval = Interval.model_validate("daily")

    assert interval.n == 1
    assert interval.offset == "D"


@pytest.mark.parametrize("value", ["0D", {"offset": "D", "n": 0}, {"offset": "D", "n": -1}])
def test_interval_rejects_non_positive_counts(value):
    with pytest.raises(ValueError, match="positive"):
        Interval.model_validate(value)


def test_interval_generates_inclusive_daily_steps():
    interval = Interval(offset="D", n=2)

    assert interval.steps(dt("2024-01-01"), dt("2024-01-05")) == [dt("2024-01-01"), dt("2024-01-03"), dt("2024-01-05")]


def test_interval_generates_calendar_month_steps_from_start_anchor():
    interval = Interval.model_validate("2M")

    assert interval.steps(dt("2024-01-31"), dt("2024-05-31")) == [dt("2024-01-31"), dt("2024-03-31"), dt("2024-05-31")]


@pytest.mark.parametrize(
    ("offset", "n", "end", "expected"),
    [
        ("W", 1, "2024-01-15", ["2024-01-01", "2024-01-08", "2024-01-15"]),
        ("h", 6, "2024-01-01T12:00:00", ["2024-01-01T00:00:00", "2024-01-01T06:00:00", "2024-01-01T12:00:00"]),
        ("min", 30, "2024-01-01T01:00:00", ["2024-01-01T00:00:00", "2024-01-01T00:30:00", "2024-01-01T01:00:00"]),
        ("s", 15, "2024-01-01T00:00:45", ["2024-01-01T00:00:00", "2024-01-01T00:00:15", "2024-01-01T00:00:30", "2024-01-01T00:00:45"]),
        ("ms", 250, "2024-01-01T00:00:00.500000", ["2024-01-01T00:00:00", "2024-01-01T00:00:00.250000", "2024-01-01T00:00:00.500000"]),
        ("us", 100, "2024-01-01T00:00:00.000200", ["2024-01-01T00:00:00", "2024-01-01T00:00:00.000100", "2024-01-01T00:00:00.000200"]),
    ],
)
def test_interval_generates_fixed_timedelta_steps(offset, n, end, expected):
    interval = Interval(offset=offset, n=n)

    assert interval.steps(dt("2024-01-01"), dt(end)) == [dt(value) for value in expected]


@pytest.mark.parametrize(
    ("n", "start", "end", "expected"),
    [
        (1, "2024-01-05", "2024-01-09", ["2024-01-05", "2024-01-08", "2024-01-09"]),
        (1, "2024-01-06", "2024-01-09", ["2024-01-08", "2024-01-09"]),
        (2, "2024-01-05", "2024-01-10", ["2024-01-05", "2024-01-09"]),
    ],
)
def test_interval_generates_business_day_steps(n, start, end, expected):
    interval = Interval(offset="B", n=n)

    assert interval.steps(dt(start), dt(end)) == [dt(value) for value in expected]


@pytest.mark.parametrize(
    ("offset", "start", "end", "expected"),
    [
        ("MS", "2024-01-10", "2024-04-01", ["2024-02-01", "2024-03-01", "2024-04-01"]),
        ("ME", "2024-01-10", "2024-03-31", ["2024-01-31", "2024-02-29", "2024-03-31"]),
        ("SMS", "2024-01-10", "2024-02-16", ["2024-01-15", "2024-02-01", "2024-02-15"]),
        ("SME", "2024-01-10", "2024-02-29", ["2024-01-15", "2024-01-31", "2024-02-15", "2024-02-29"]),
        ("BMS", "2024-06-01", "2024-08-05", ["2024-06-03", "2024-07-01", "2024-08-01"]),
        ("BME", "2024-05-01", "2024-06-30", ["2024-05-31", "2024-06-28"]),
        ("QS", "2024-02-01", "2024-07-01", ["2024-04-01", "2024-07-01"]),
        ("QE", "2024-02-01", "2024-06-30", ["2024-03-31", "2024-06-30"]),
        ("BQS", "2024-06-01", "2024-10-05", ["2024-07-01", "2024-10-01"]),
        ("BQE", "2024-03-01", "2024-06-30", ["2024-03-29", "2024-06-28"]),
        ("YS", "2024-06-01", "2026-01-01", ["2025-01-01", "2026-01-01"]),
        ("YE", "2024-06-01", "2025-12-31", ["2024-12-31", "2025-12-31"]),
        ("BYS", "2022-12-31", "2024-01-02", ["2023-01-02", "2024-01-01"]),
        ("BYE", "2023-01-01", "2023-12-31", ["2023-12-29"]),
    ],
)
def test_interval_generates_calendar_boundary_steps(offset, start, end, expected):
    interval = Interval(offset=offset)

    assert interval.steps(dt(start), dt(end)) == [dt(value) for value in expected]


def test_interval_applies_count_to_calendar_boundaries():
    interval = Interval(offset="ME", n=2)

    assert interval.steps(dt("2024-01-01"), dt("2024-05-31")) == [dt("2024-01-31"), dt("2024-03-31"), dt("2024-05-31")]


@pytest.mark.parametrize("offset", ["C", "CBME", "CBMS", "bh", "cbh", "ns"])
def test_interval_rejects_offsets_that_require_custom_calendars_or_nanosecond_precision(offset):
    interval = Interval(offset=offset)

    with pytest.raises(ValueError, match="not supported"):
        interval.steps(dt("2024-01-01"), dt("2024-01-02"))
