from typing import Type

import pytest
from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType

from ccflow_etl import RetryExecutionError, RetryModel, RetryPolicy


def test_retry_policy_classifies_status_and_exceptions():
    policy = RetryPolicy(max_attempts=3, retry_status_codes=[429], retry_exception_types=["TimeoutError"])

    assert policy.should_retry_status(status_code=429, attempt=1) is True
    assert policy.should_retry_status(status_code=500, attempt=1) is False
    assert policy.should_retry_status(status_code=429, attempt=3) is False
    assert policy.should_retry_exception(TimeoutError("timed out"), attempt=2) is True
    assert policy.should_retry_exception(ValueError("bad"), attempt=1) is False


def test_retry_policy_computes_exponential_backoff_with_cap_and_jitter():
    policy = RetryPolicy(initial_delay_seconds=1.0, backoff_multiplier=2.0, max_delay_seconds=3.0, jitter_ratio=0.25)

    assert policy.delay_seconds(attempt=1, jitter_value=0.5) == 1.0
    assert policy.delay_seconds(attempt=2, jitter_value=0.5) == 2.0
    assert policy.delay_seconds(attempt=3, jitter_value=0.5) == 3.0
    assert policy.delay_seconds(attempt=2, jitter_value=0.0) == 1.5
    assert policy.delay_seconds(attempt=2, jitter_value=1.0) == 2.5


def test_retry_policy_classifies_timeout_exception_categories():
    policy = RetryPolicy(retry_exception_types=["TimeoutError"])

    assert policy.exception_category(TimeoutError("timed out")) == "timeout"
    assert policy.exception_category(ValueError("bad")) == "exception"


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


def test_retry_model_retries_exceptions_with_backoff_and_event_summary(monkeypatch):
    sleeps = []
    model = RetryModel(
        model=FlakyDateModel(failures_before_success=2),
        policy=RetryPolicy(max_attempts=3, retry_exception_types=["TimeoutError"], initial_delay_seconds=1.0, backoff_multiplier=2.0),
    )
    monkeypatch.setattr(model, "_sleep", sleeps.append)

    result = model(DateContext(date="2024-01-03"))

    assert sleeps == [1.0, 2.0]
    assert result.value["attempts"] == 3
    assert result.value["retried"] == 2
    assert result.value["failed"] == 0
    assert result.value["result"] == {"date": "2024-01-03", "attempts": 3}
    assert [event["outcome"] for event in result.value["events"]] == ["retry", "retry", "success"]
    assert [event["category"] for event in result.value["events"][:2]] == ["timeout", "timeout"]
    assert [event["delay_seconds"] for event in result.value["events"][:2]] == [1.0, 2.0]


def test_retry_model_raises_failure_with_event_summary_after_exhaustion(monkeypatch):
    model = RetryModel(
        model=FlakyDateModel(failures_before_success=3),
        policy=RetryPolicy(max_attempts=2, retry_exception_types=["TimeoutError"], initial_delay_seconds=0.5),
    )
    monkeypatch.setattr(model, "_sleep", lambda delay: None)

    with pytest.raises(RetryExecutionError) as exc_info:
        model(DateContext(date="2024-01-03"))

    assert exc_info.value.summary["attempts"] == 2
    assert exc_info.value.summary["retried"] == 1
    assert exc_info.value.summary["failed"] == 1
    assert [event["outcome"] for event in exc_info.value.events] == ["retry", "failed"]
