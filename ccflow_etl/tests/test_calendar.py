from __future__ import annotations

from datetime import date, datetime

from ccflow_etl import BaseCalendar, DailyCalendar, HourlyCalendar, IntervalCalendar, WeekdayCalendar, WeeklyCalendar


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def test_base_calendar_coerces_interval_string_to_interval_calendar():
    calendar = BaseCalendar.model_validate("2M")

    assert isinstance(calendar, IntervalCalendar)
    assert [step.date() for step in calendar.steps(date(2024, 1, 31), date(2024, 5, 31))] == [
        date(2024, 1, 31),
        date(2024, 3, 31),
        date(2024, 5, 31),
    ]


def test_basic_calendar_classes_generate_expected_steps():
    assert DailyCalendar().steps(dt("2024-01-01"), dt("2024-01-03")) == [
        dt("2024-01-01"),
        dt("2024-01-02"),
        dt("2024-01-03"),
    ]
    assert HourlyCalendar().steps(dt("2024-01-01T00:00:00"), dt("2024-01-01T02:00:00")) == [
        dt("2024-01-01T00:00:00"),
        dt("2024-01-01T01:00:00"),
        dt("2024-01-01T02:00:00"),
    ]
    assert WeeklyCalendar().steps(dt("2024-01-01"), dt("2024-01-15")) == [
        dt("2024-01-01"),
        dt("2024-01-08"),
        dt("2024-01-15"),
    ]
    assert WeekdayCalendar().steps(dt("2024-01-05"), dt("2024-01-09")) == [
        dt("2024-01-05"),
        dt("2024-01-08"),
        dt("2024-01-09"),
    ]
