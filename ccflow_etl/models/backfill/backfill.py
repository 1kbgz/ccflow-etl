from datetime import datetime
from typing import Generic, List, Literal, Tuple, Type, TypeVar, Union

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
    interval: Interval = Field(description="Interval between each backfill step")

    @model_validator(mode="before")
    @classmethod
    def validate_direction(cls, v):
        if v.get("direction") not in (None, "forward", "backward"):
            raise ValueError("direction must be either 'forward' or 'backward'")
        # Validate interval to not confuse ccflow
        if "interval" in v:
            interval = v["interval"]
            if isinstance(interval, str):
                v["interval"] = Interval.validate_n(interval, None)
        return v

    def steps(self, as_array: bool = False) -> Union[List[datetime], NDArray[datetime64]]:
        # Generate steps with pandas
        import pandas as pd

        # reassemble interval string post-validation
        date_range = pd.date_range(
            start=self.start_datetime,
            end=self.end_datetime,
            freq=f"{self.interval.n}{self.interval.offset}",
        )

        # Adjust for direction
        if self.direction == "backward":
            date_range = date_range.reverse()

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
        return BackfillResult[self.model.result_type]

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
    def __call__(self, context: BackfillContext[C]) -> R:
        result = {}
        print("This should happen after all calls and be cached")
        print("You should not see any further executes if using caching evaluator!")
        for step in self._steps or context.step_contexts():
            self.model(context=step)
        return BackfillResult(value=result)
