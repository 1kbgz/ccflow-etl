from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional

from ccflow import BaseModel, ModelRegistry
from pydantic import Field, SerializeAsAny

from .calendar import BaseCalendar

__all__ = (
    "DateUtility",
    "DateUtilityRegistry",
    "LatestSessionDate",
)


def _end_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.max)


class DateUtility(BaseModel):
    def resolve(self, *, calendar: BaseCalendar | str | None = None) -> date | datetime:
        raise NotImplementedError


class LatestSessionDate(DateUtility):
    calendar: Optional[SerializeAsAny[BaseCalendar]] = None
    as_of: date | datetime | None = None
    lookback_days: int = Field(default=14, gt=0)

    def resolve(self, *, calendar: BaseCalendar | str | None = None) -> date | datetime:
        selected_calendar = self.calendar or (BaseCalendar.model_validate(calendar) if calendar is not None else None)
        if selected_calendar is None:
            raise ValueError("latest-session requires a selected calendar")

        end = _end_datetime(self.as_of or date.today())
        start = end - timedelta(days=self.lookback_days)
        steps = selected_calendar.steps(start, end)
        if not steps:
            raise ValueError(f"No calendar sessions found in the {self.lookback_days} days before {end.date().isoformat()}")
        return steps[-1]


class DateUtilityRegistry(ModelRegistry):
    name: str = "dates"
    calendar: Optional[SerializeAsAny[BaseCalendar]] = None
    as_of: date | datetime | None = None
    lookback_days: int = Field(default=14, gt=0)

    def __getitem__(self, item) -> DateUtility:
        normalized = str(item).replace("_", "-")
        if normalized != "latest-session":
            return super().__getitem__(item)
        if normalized in self.models:
            return self.models[normalized]
        return self.add(
            normalized,
            LatestSessionDate(calendar=self.calendar, as_of=self.as_of, lookback_days=self.lookback_days),
            overwrite=True,
        )
