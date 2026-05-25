from __future__ import annotations

from datetime import date
from typing import Type

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType

from ccflow_etl import BackfillContext, BackfillModel, WeekdayCalendar


def test_backfill_context_builds_business_day_contexts():
    context = BackfillContext(
        start_datetime="2024-01-05",
        end_datetime="2024-01-09",
        interval="1B",
        context=DateContext(date="2024-01-01"),
    )

    step_contexts = context.step_contexts()

    assert [step.date for step in step_contexts] == [date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9)]


def test_backfill_context_accepts_cli_date_range_list_for_daily_contexts():
    context = BackfillContext[DateContext].model_validate(["2024-01-02", "2024-01-04"])

    assert context.start_datetime.date() == date(2024, 1, 2)
    assert context.end_datetime.date() == date(2024, 1, 4)
    assert context.interval.offset == "D"
    assert context.interval.n == 1
    assert context.context.date == date(2024, 1, 2)
    assert [step.date for step in context.step_contexts()] == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


def test_backfill_context_returns_steps_in_backward_direction():
    context = BackfillContext[DateContext].model_validate(["2024-01-02", "2024-01-04", {}, "backward", "1D"])

    assert [step.date() for step in context.steps()] == [date(2024, 1, 4), date(2024, 1, 3), date(2024, 1, 2)]


def test_backfill_context_uses_explicit_calendar_for_steps():
    context = BackfillContext[DateContext].model_validate(
        {
            "start_datetime": "2024-01-05",
            "end_datetime": "2024-01-09",
            "calendar": WeekdayCalendar(),
            "context": {"date": "2024-01-01"},
        }
    )

    assert [step.date for step in context.step_contexts()] == [date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9)]


def test_backfill_context_accepts_calendar_in_compact_cli_list():
    context = BackfillContext[DateContext].model_validate(["2024-01-05", "2024-01-09", {}, "forward", WeekdayCalendar()])

    assert [step.date for step in context.step_contexts()] == [date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9)]


class EchoDateModel(CallableModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return DateContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context):
        return GenericResult(value={"date": context.date.isoformat()})


def test_backfill_model_returns_step_outputs_for_cli_date_range_list():
    result = BackfillModel(model=EchoDateModel())(["2024-01-02", "2024-01-03"])

    assert result.value == {
        "steps": 2,
        "outputs": [
            {"context": {"date": "2024-01-02", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-02"}},
            {"context": {"date": "2024-01-03", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-03"}},
        ],
    }
