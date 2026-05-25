from __future__ import annotations

from datetime import date, datetime, time
from typing import Any

from ccflow import BaseModel
from pydantic import Field, model_validator

from .interval import Interval

__all__ = (
    "BaseCalendar",
    "IntervalCalendar",
    "DailyCalendar",
    "HourlyCalendar",
    "WeeklyCalendar",
    "WeekdayCalendar",
    "BusinessDayCalendar",
    "MondayFridayCalendar",
)


def _coerce_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.min)


def _is_registry_reference(value: str) -> bool:
    return value.startswith(("/", "./", "../"))


class BaseCalendar(BaseModel):
    @classmethod
    def _coerce_calendar(cls, value: Any):
        if cls is not BaseCalendar:
            return value
        if isinstance(value, Interval):
            return IntervalCalendar(interval=value)
        if isinstance(value, str) and not _is_registry_reference(value):
            return IntervalCalendar(interval=value)
        if isinstance(value, dict) and "_target_" not in value and "type_" not in value and "interval" in value:
            return IntervalCalendar.model_validate(value)
        return value

    @classmethod
    def model_validate(cls, obj: Any, *args: Any, **kwargs: Any):
        if cls is BaseCalendar:
            obj = cls._coerce_calendar(obj)
        return super().model_validate(obj, *args, **kwargs)

    @model_validator(mode="before")
    @classmethod
    def validate_calendar(cls, value):
        return cls._coerce_calendar(value)

    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        raise NotImplementedError


class IntervalCalendar(BaseCalendar):
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1D"))

    @model_validator(mode="before")
    @classmethod
    def validate_interval_calendar(cls, value):
        if isinstance(value, str):
            return {"interval": Interval.model_validate(value)}
        if isinstance(value, Interval):
            return {"interval": value}
        if isinstance(value, dict) and isinstance(value.get("interval"), str):
            value = dict(value)
            value["interval"] = Interval.model_validate(value["interval"])
        return value

    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        return self.interval.steps(_coerce_datetime(start), _coerce_datetime(end))


class DailyCalendar(IntervalCalendar):
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1D"))


class HourlyCalendar(IntervalCalendar):
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1h"))


class WeeklyCalendar(IntervalCalendar):
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1W"))


class WeekdayCalendar(IntervalCalendar):
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1B"))


class BusinessDayCalendar(WeekdayCalendar): ...


class MondayFridayCalendar(WeekdayCalendar): ...
