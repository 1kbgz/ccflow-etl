from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Generic, List, Literal, Optional, Tuple, Type, TypeVar, Union

from ccflow import (
    CallableModel,
    CallableModelGenericType,
    ContextBase,
    ContextType,
    DatetimeRangeContext,
    Flow,
    GenericContext,
    GenericResult,
    NDArray,
    ResultBase,
    ResultType,
)
from numpy import datetime64
from pydantic import Field, PrivateAttr, SerializeAsAny, model_validator

from .calendar import BaseCalendar, IntervalCalendar
from .interval import Interval

__all__ = (
    "BackfillContext",
    "BackfillModel",
)


C = TypeVar("C", bound=ContextBase)
R = TypeVar("R", bound=ResultBase)


class BackfillContext(DatetimeRangeContext, Generic[C]):
    context: SerializeAsAny[C] = Field(default_factory=GenericContext)
    direction: Literal["forward", "backward"] = "forward"
    interval: Interval = Field(default_factory=lambda: Interval.model_validate("1D"), description="Interval between each backfill step")
    calendar: Optional[SerializeAsAny[BaseCalendar]] = Field(default=None, description="Calendar that provides backfill steps")

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
    def _coerce_cli_context(cls, v):
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes, bytearray)):
            items = list(v)
            if len(items) not in {2, 3, 4, 5}:
                raise ValueError(
                    "backfill context list must be [start, end], [start, end, context], or [start, end, context, direction, interval_or_calendar]"
                )
            start_datetime = items[0]
            end_datetime = items[1]
            child_context = items[2] if len(items) >= 3 else None
            direction = items[3] if len(items) >= 4 else "forward"
            interval_or_calendar = items[4] if len(items) >= 5 else None
            if child_context is None or (
                isinstance(child_context, Sequence) and not isinstance(child_context, (str, bytes, bytearray)) and len(child_context) == 0
            ):
                child_context = {"date": start_datetime}
            elif isinstance(child_context, Mapping):
                child_context = dict(child_context)
                child_context.setdefault("date", start_datetime)
            backfill_context = {
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "context": child_context,
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
        if v.get("direction") not in (None, "forward", "backward"):
            raise ValueError("direction must be either 'forward' or 'backward'")
        # Validate interval to not confuse ccflow
        if "interval" in v and v["interval"] is not None:
            interval = v["interval"]
            if isinstance(interval, str):
                v["interval"] = Interval.model_validate(interval)
        if "calendar" in v and v["calendar"] is not None:
            v["calendar"] = cls._coerce_calendar(v["calendar"])
        if v.get("calendar") is None:
            v["calendar"] = IntervalCalendar(interval=v.get("interval", Interval.model_validate("1D")))
        elif isinstance(v["calendar"], IntervalCalendar):
            v["interval"] = v["calendar"].interval
        return v

    def steps(self, as_array: bool = False) -> Union[List[datetime], NDArray[datetime64]]:
        date_range = self.calendar.steps(self.start_datetime, self.end_datetime)

        # Adjust for direction
        if self.direction == "backward":
            date_range.reverse()

        # Convert to numpy array if requested
        if as_array:
            import numpy as np

            return np.array(date_range, dtype="datetime64")

        return date_range

    def step_contexts(self) -> List[C]:
        return [self.context.model_copy(update={"datetime": step, "dt": step, "date": step.date()}) for step in self.steps(as_array=False)]


class BackfillResult(GenericResult): ...


class BackfillModel(CallableModel, Generic[C, R]):
    model: CallableModelGenericType[C, R]

    _steps: List[ContextType] = PrivateAttr(default_factory=list)

    @property
    def context_type(self) -> Type[ContextType]:
        return BackfillContext[self.model.context_type]

    @property
    def result_type(self) -> Type[ResultType]:
        return BackfillResult

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, v):
        if not isinstance(v, dict):
            raise ValueError("model must be a dict representing a CallableModelGenericType")
        return v

    @Flow.deps
    def __deps__(self, context: BackfillContext[C]) -> List[Tuple[CallableModelGenericType[C, R], List[ContextType]]]:
        self._steps = context.step_contexts()
        return [(self.model, self._steps)]

    @Flow.call
    def __call__(self, context: BackfillContext[C]) -> BackfillResult:
        outputs = []
        for step in self._steps or context.step_contexts():
            result = self.model(context=step)
            value = result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")
            outputs.append({"context": step.model_dump(mode="json"), "value": value})
        return BackfillResult(value={"steps": len(outputs), "outputs": outputs})
