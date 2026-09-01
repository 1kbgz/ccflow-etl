from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import ClassVar, Generic, Literal, TypeVar

from ccflow import (
    CallableModel,
    CallableModelGenericType,
    ContextBase,
    ContextType,
    DatetimeRangeContext,
    Flow,
    GenericContext,
    GenericResult,
    ModelRegistry,
    NDArray,
    ResultBase,
    ResultType,
)
from numpy import datetime64
from pydantic import Field, PrivateAttr, SerializeAsAny, model_validator

from .calendar import BaseCalendar, IntervalCalendar
from .dates import DateUtility
from .interval import Interval

__all__ = (
    "BackfillContext",
    "BackfillModel",
    "BackfillRegistry",
)


C = TypeVar("C", bound=ContextBase)
R = TypeVar("R", bound=ResultBase)


class BackfillContext(DatetimeRangeContext, Generic[C]):
    template: SerializeAsAny[C] = Field(default_factory=GenericContext)
    direction: Literal["forward", "backward"] = "forward"
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1D"), description="Interval between each backfill step")
    calendar: SerializeAsAny[BaseCalendar] | None = Field(default=None, description="Calendar that provides backfill steps")
    calendar_from_default: bool = Field(default=False, exclude=True)

    @classmethod
    def _is_calendar_value(cls, value) -> bool:
        return (
            isinstance(value, BaseCalendar)
            or (isinstance(value, str) and value.startswith(("/", "./", "../")))
            or (isinstance(value, Mapping) and ("_target_" in value or "type_" in value or "interval" in value))
        )

    @classmethod
    def _coerce_calendar(cls, value):
        if value is None:
            return None
        return BaseCalendar._coerce_calendar(value)

    @classmethod
    def _resolve_datetime_value(cls, value, calendar):
        if not (isinstance(value, str) and value.startswith(("/", "./", "../"))):
            return value
        utility = DateUtility.model_validate(value)
        return utility.resolve(calendar=calendar)

    @classmethod
    def _coerce_cli_context(cls, v):
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes, bytearray)):
            items = list(v)
            if len(items) not in {2, 3, 4, 5}:
                raise ValueError(
                    "backfill context list must be [start, end], [start, end, template], or [start, end, template, direction, interval_or_calendar]"
                )
            start_datetime = items[0]
            end_datetime = items[1]
            template = items[2] if len(items) >= 3 else None
            direction = items[3] if len(items) >= 4 else "forward"
            interval_or_calendar = items[4] if len(items) >= 5 else None
            if template is None or (isinstance(template, Sequence) and not isinstance(template, (str, bytes, bytearray)) and len(template) == 0):
                template = {"date": start_datetime}
            elif isinstance(template, Mapping):
                template = dict(template)
                template.setdefault("date", start_datetime)
            backfill_context = {
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "template": template,
                "direction": direction,
            }
            if cls._is_calendar_value(interval_or_calendar):
                backfill_context["calendar"] = interval_or_calendar
            elif interval_or_calendar is not None:
                backfill_context["interval"] = interval_or_calendar
            return backfill_context
        return v

    @model_validator(mode="wrap")
    @classmethod
    def validate_cli_sequence(cls, v, handler, info):
        return handler(cls._coerce_cli_context(v))

    @model_validator(mode="before")
    @classmethod
    def validate_cli_context(cls, v):
        v = cls._coerce_cli_context(v)
        if not isinstance(v, Mapping):
            return v
        v = dict(v)
        if "context" in v:
            raise ValueError("BackfillContext.context was renamed to BackfillContext.template")
        if v.get("direction") not in (None, "forward", "backward"):
            raise ValueError("direction must be either 'forward' or 'backward'")
        has_calendar = "calendar" in v and v["calendar"] is not None
        has_interval = "interval" in v and v["interval"] is not None
        # Validate interval to not confuse ccflow
        if "interval" in v and v["interval"] is not None:
            interval = v["interval"]
            if isinstance(interval, str):
                v["interval"] = Interval.model_validate(interval)
        if "calendar" in v and v["calendar"] is not None:
            v["calendar"] = cls._coerce_calendar(v["calendar"])
        v["start_datetime"] = cls._resolve_datetime_value(v["start_datetime"], v.get("calendar"))
        v["end_datetime"] = cls._resolve_datetime_value(v["end_datetime"], v.get("calendar"))
        if v.get("calendar") is None:
            v["calendar"] = IntervalCalendar(interval=v.get("interval", Interval.model_validate("1D")))
            v["calendar_from_default"] = not has_calendar and not has_interval
        elif isinstance(v["calendar"], IntervalCalendar):
            v["interval"] = v["calendar"].interval
        return v

    def steps(self, as_array: bool = False) -> list[datetime] | NDArray[datetime64]:
        date_range = self.calendar.steps(self.start_datetime, self.end_datetime)

        # Adjust for direction
        if self.direction == "backward":
            date_range.reverse()

        # Convert to numpy array if requested
        if as_array:
            import numpy as np

            return np.array(date_range, dtype="datetime64")

        return date_range

    def step_contexts(self) -> list[C]:
        return [self.template.model_copy(update={"datetime": step, "dt": step, "date": step.date()}) for step in self.steps(as_array=False)]


class BackfillResult(GenericResult): ...


class BackfillModel(CallableModel, Generic[C, R]):
    model: CallableModelGenericType[C, R]
    interval: Interval | None = None
    calendar: SerializeAsAny[BaseCalendar] | None = None

    _steps: list[ContextType] = PrivateAttr(default_factory=list)

    @property
    def context_type(self) -> type[ContextType]:
        return BackfillContext[self.model.context_type]

    @property
    def result_type(self) -> type[ResultType]:
        return BackfillResult

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, v):
        if not isinstance(v, dict):
            raise TypeError("model must be a dict representing a CallableModelGenericType")
        v = dict(v)
        if isinstance(v.get("interval"), str):
            v["interval"] = Interval.model_validate(v["interval"])
        if "calendar" in v and v["calendar"] is not None:
            v["calendar"] = BaseCalendar._coerce_calendar(v["calendar"])
        return v

    def _context_with_defaults(self, context: BackfillContext[C]) -> BackfillContext[C]:
        if self.calendar is None and self.interval is None:
            return context
        if not context.calendar_from_default:
            return context
        if self.calendar is not None:
            updates = {"calendar": self.calendar}
            if isinstance(self.calendar, IntervalCalendar):
                updates["interval"] = self.calendar.interval
            return context.model_copy(update=updates)
        interval = self.interval or Interval.model_validate("1D")
        return context.model_copy(update={"interval": interval, "calendar": IntervalCalendar(interval=interval)})

    @Flow.deps
    def __deps__(self, context: BackfillContext[C]) -> list[tuple[CallableModelGenericType[C, R], list[ContextType]]]:
        context = self._context_with_defaults(context)
        self._steps = context.step_contexts()
        return [(self.model, self._steps)]

    @Flow.call
    def __call__(self, context: BackfillContext[C]) -> BackfillResult:
        context = self._context_with_defaults(context)
        outputs = []
        for step in self._steps or context.step_contexts():
            result = self.model(context=step)
            value = result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")
            outputs.append({"context": step.model_dump(mode="json"), "value": value})
        return BackfillResult(value={"steps": len(outputs), "outputs": outputs})


class BackfillRegistry(ModelRegistry):
    intervals: ClassVar[Mapping[str, str]] = {
        "hourly": "1h",
        "first_day_of_month": "1MS",
        "last_day_of_month": "1ME",
    }

    name: str = "backfills"
    model: str = "/task"
    calendar: str | None = None

    def __contains__(self, item: object) -> bool:
        normalized = str(item).replace("-", "_")
        return normalized in {"default", "daily", *self.intervals} or super().__contains__(item)

    def __getitem__(self, item) -> BackfillModel:
        normalized = str(item).replace("-", "_")
        if normalized in self.models:
            return self.models[normalized]

        if normalized not in {"default", "daily", *self.intervals}:
            return super().__getitem__(item)

        kwargs = {"model": self.model}
        if normalized in self.intervals:
            kwargs["interval"] = self.intervals[normalized]
        elif self.calendar is not None:
            kwargs["calendar"] = self.calendar
        return self.add(normalized, BackfillModel(**kwargs), overwrite=True)
