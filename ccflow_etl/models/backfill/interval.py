from calendar import monthrange
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Literal

from ccflow import BaseModel
from pydantic import model_validator

__all__ = (
    "Offset",
    "Interval",
)


Offset = Literal[
    "B",  # business day frequency
    "C",  # custom business day frequency
    "D",  # calendar day frequency
    "M",  # calendar month frequency from the start anchor
    "W",  # weekly frequency
    "ME",  # month end frequency
    "SME",  # semi-month end frequency (15th and end of month)
    "BME",  # business month end frequency
    "CBME",  # custom business month end frequency
    "MS",  # month start frequency
    "SMS",  # semi-month start frequency (1st and 15th)
    "BMS",  # business month start frequency
    "CBMS",  # custom business month start frequency
    "QE",  # quarter end frequency
    "BQE",  # business quarter end frequency
    "QS",  # quarter start frequency
    "BQS",  # business quarter start frequency
    "YE",  # year end frequency
    "BYE",  # business year end frequency
    "YS",  # year start frequency
    "BYS",  # business year start frequency
    "h",  # hourly frequency
    "bh",  # business hour frequency
    "cbh",  # custom business hour frequency
    "min",  # minutely frequency
    "s",  # secondly frequency
    "ms",  # milliseconds
    "us",  # microseconds
    "ns",  # nanoseconds
]


_FIXED_TIMEDELTAS: dict[str, Callable[[int], timedelta]] = {
    "D": lambda count: timedelta(days=count),
    "W": lambda count: timedelta(weeks=count),
    "h": lambda count: timedelta(hours=count),
    "min": lambda count: timedelta(minutes=count),
    "s": lambda count: timedelta(seconds=count),
    "ms": lambda count: timedelta(milliseconds=count),
    "us": lambda count: timedelta(microseconds=count),
}

_UNSUPPORTED_OFFSETS = {
    "C": "custom business day intervals require an explicit calendar",
    "CBME": "custom business month-end intervals require an explicit calendar",
    "CBMS": "custom business month-start intervals require an explicit calendar",
    "bh": "business-hour intervals require business-hour window rules",
    "cbh": "custom business-hour intervals require an explicit calendar and business-hour window rules",
    "ns": "nanosecond intervals cannot be represented by datetime.datetime",
}

_INTERVAL_ALIASES = {
    "daily": "1D",
    "day": "1D",
    "monthly": "1M",
    "month": "1M",
    "business_daily": "1B",
    "business-day": "1B",
    "business_day": "1B",
    "weekly": "1W",
    "week": "1W",
    "hourly": "1h",
    "hour": "1h",
    "minutely": "1min",
    "minute": "1min",
    "secondly": "1s",
    "second": "1s",
}


def _coerce_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.min)


def _combine_with_start_time(day: date, start: datetime) -> datetime:
    return datetime.combine(day, start.timetz())


def _last_day_of_month(year: int, month: int) -> int:
    return monthrange(year, month)[1]


def _add_months(value: datetime, months: int, anchor_day: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(anchor_day, _last_day_of_month(year, month))
    return value.replace(year=year, month=month, day=day)


def _is_weekday(day: date) -> bool:
    return day.weekday() < 5


def _first_weekday_of_month(year: int, month: int) -> date:
    day = date(year, month, 1)
    while not _is_weekday(day):
        day += timedelta(days=1)
    return day


def _last_weekday_of_month(year: int, month: int) -> date:
    day = date(year, month, _last_day_of_month(year, month))
    while not _is_weekday(day):
        day -= timedelta(days=1)
    return day


def _is_month_end(day: date) -> bool:
    return day.day == _last_day_of_month(day.year, day.month)


def _is_semi_month_end(day: date) -> bool:
    return day.day == 15 or _is_month_end(day)


def _is_business_month_end(day: date) -> bool:
    return day == _last_weekday_of_month(day.year, day.month)


def _is_month_start(day: date) -> bool:
    return day.day == 1


def _is_semi_month_start(day: date) -> bool:
    return day.day in {1, 15}


def _is_business_month_start(day: date) -> bool:
    return day == _first_weekday_of_month(day.year, day.month)


def _is_quarter_end(day: date) -> bool:
    return day.month in {3, 6, 9, 12} and _is_month_end(day)


def _is_business_quarter_end(day: date) -> bool:
    return day.month in {3, 6, 9, 12} and _is_business_month_end(day)


def _is_quarter_start(day: date) -> bool:
    return day.month in {1, 4, 7, 10} and _is_month_start(day)


def _is_business_quarter_start(day: date) -> bool:
    return day.month in {1, 4, 7, 10} and _is_business_month_start(day)


def _is_year_end(day: date) -> bool:
    return day.month == 12 and _is_month_end(day)


def _is_business_year_end(day: date) -> bool:
    return day.month == 12 and _is_business_month_end(day)


def _is_year_start(day: date) -> bool:
    return day.month == 1 and _is_month_start(day)


def _is_business_year_start(day: date) -> bool:
    return day.month == 1 and _is_business_month_start(day)


_CALENDAR_OFFSETS: dict[str, Callable[[date], bool]] = {
    "B": _is_weekday,
    "ME": _is_month_end,
    "SME": _is_semi_month_end,
    "BME": _is_business_month_end,
    "MS": _is_month_start,
    "SMS": _is_semi_month_start,
    "BMS": _is_business_month_start,
    "QE": _is_quarter_end,
    "BQE": _is_business_quarter_end,
    "QS": _is_quarter_start,
    "BQS": _is_business_quarter_start,
    "YE": _is_year_end,
    "BYE": _is_business_year_end,
    "YS": _is_year_start,
    "BYS": _is_business_year_start,
}


class Interval(BaseModel):
    offset: Offset
    n: int = 1

    @classmethod
    def _coerce_interval(cls, value):
        if isinstance(value, str):
            value = _INTERVAL_ALIASES.get(value.lower(), value)
            if not value:
                raise ValueError("Invalid interval string: empty")
            character_index = 0
            while character_index < len(value) and value[character_index].isdigit():
                character_index += 1
            count = int(value[:character_index]) if character_index else 1
            offset = value[character_index:]
            if not offset:
                raise ValueError(f"Invalid interval string: {value}")
            return {"offset": offset, "n": count}
        return value

    @classmethod
    def model_validate(cls, obj: Any, *args: Any, **kwargs: Any):
        return super().model_validate(cls._coerce_interval(obj), *args, **kwargs)

    @model_validator(mode="before")
    @classmethod
    def validate_n(cls, v, info):
        return cls._coerce_interval(v)

    @model_validator(mode="after")
    def validate_positive_count(self):
        if self.n <= 0:
            raise ValueError("interval count must be positive")
        return self

    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        start_datetime = _coerce_datetime(start)
        end_datetime = _coerce_datetime(end)
        if start_datetime > end_datetime:
            return []
        if self.offset in _UNSUPPORTED_OFFSETS:
            raise ValueError(f"Offset {self.offset!r} is not supported: {_UNSUPPORTED_OFFSETS[self.offset]}")
        if self.offset in _FIXED_TIMEDELTAS:
            return self._fixed_steps(start_datetime, end_datetime)
        if self.offset == "M":
            return self._month_steps(start_datetime, end_datetime)
        if self.offset in _CALENDAR_OFFSETS:
            return self._calendar_steps(start_datetime, end_datetime, _CALENDAR_OFFSETS[self.offset])
        raise ValueError(f"Offset {self.offset!r} is not supported")

    def _fixed_steps(self, start: datetime, end: datetime) -> list[datetime]:
        delta = _FIXED_TIMEDELTAS[self.offset](self.n)
        steps = []
        current = start
        while current <= end:
            steps.append(current)
            current += delta
        return steps

    def _month_steps(self, start: datetime, end: datetime) -> list[datetime]:
        steps = []
        step = 0
        while True:
            current = _add_months(start, self.n * step, start.day)
            if current > end:
                return steps
            steps.append(current)
            step += 1

    def _calendar_steps(self, start: datetime, end: datetime, predicate: Callable[[date], bool]) -> list[datetime]:
        steps = []
        current_date = start.date()
        end_date = end.date()
        while current_date <= end_date:
            if predicate(current_date):
                current = _combine_with_start_time(current_date, start)
                if start <= current <= end:
                    steps.append(current)
            current_date += timedelta(days=1)
        return steps[:: self.n]
