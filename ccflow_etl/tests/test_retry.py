from typing import Type

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType
from ccflow.models import RetryModel as CoreRetryModel
from ccflow.utils.retry import RetryError, RetryPolicy as CoreRetryPolicy

from ccflow_etl import RetryExecutionError, RetryModel, RetryPolicy


def test_retry_exports_core_ccflow_symbols():
    assert RetryModel is CoreRetryModel
    assert RetryPolicy is CoreRetryPolicy
    assert RetryExecutionError is RetryError


class FlakyDateModel(CallableModel):
    attempts: int = 0
    failures_before_success: int = 0

    @property
    def context_type(self) -> Type[ContextType]:
        return DateContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context):
        self.attempts += 1
        if self.attempts <= self.failures_before_success:
            raise TimeoutError(f"timed out on attempt {self.attempts}")
        return GenericResult(value={"date": context.date.isoformat(), "attempts": self.attempts})


def test_retry_model_uses_ccflow_retry_model(monkeypatch):
    sleeps = []
    model = RetryModel(
        model=FlakyDateModel(failures_before_success=2),
        max_attempts=3,
        retry_exceptions=[TimeoutError],
        wait_initial=1.0,
        wait_multiplier=2.0,
    )
    monkeypatch.setattr("ccflow.utils.retry.time.sleep", sleeps.append)

    result = model(DateContext(date="2024-01-03"))

    assert sleeps == [1.0, 2.0]
    assert result.value == {"date": "2024-01-03", "attempts": 3}
