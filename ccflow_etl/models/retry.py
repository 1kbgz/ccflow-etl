from random import random
from time import sleep
from typing import Any, Dict, Generic, List, Literal, Optional, Type, TypeVar

from ccflow import BaseModel, CallableModel, CallableModelGenericType, ContextBase, ContextType, Flow, GenericResult, ResultBase, ResultType
from pydantic import Field

__all__ = (
    "RetryEvent",
    "RetryExecutionError",
    "RetryModel",
    "RetryPolicy",
    "RetryResult",
)


C = TypeVar("C", bound=ContextBase)
R = TypeVar("R", bound=ResultBase)
RetryOutcome = Literal["retry", "success", "failed"]


class RetryEvent(BaseModel):
    attempt: int
    outcome: RetryOutcome
    delay_seconds: float = 0.0
    status_code: Optional[int] = None
    exception_type: Optional[str] = None
    category: Optional[str] = None
    message: Optional[str] = None


class RetryResult(GenericResult): ...


class RetryExecutionError(RuntimeError):
    def __init__(self, message: str, events: List[Dict[str, Any]], summary: Dict[str, Any]):
        super().__init__(message)
        self.events = events
        self.summary = summary


class RetryPolicy(BaseModel):
    max_attempts: int = 1
    retry_status_codes: List[int] = Field(default_factory=lambda: [429, 500, 502, 503, 504])
    retry_exception_types: List[str] = Field(default_factory=lambda: ["TimeoutException", "ConnectError"])
    timeout_exception_types: List[str] = Field(
        default_factory=lambda: ["TimeoutError", "TimeoutException", "ConnectTimeout", "ReadTimeout", "WriteTimeout", "PoolTimeout"]
    )
    initial_delay_seconds: float = Field(default=0.0, ge=0.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)
    max_delay_seconds: Optional[float] = Field(default=None, ge=0.0)
    jitter_ratio: float = Field(default=0.0, ge=0.0)

    def should_retry_status(self, status_code: Optional[int], attempt: int) -> bool:
        return status_code in self.retry_status_codes and attempt < self.max_attempts

    def should_retry_exception(self, exception: BaseException, attempt: int) -> bool:
        exception_names = {type(exception).__name__}
        exception_names.update(base.__name__ for base in type(exception).__mro__)
        return bool(exception_names.intersection(self.retry_exception_types)) and attempt < self.max_attempts

    def delay_seconds(self, attempt: int, jitter_value: Optional[float] = None) -> float:
        if attempt < 1:
            raise ValueError("attempt must be greater than or equal to 1")
        delay = self.initial_delay_seconds * (self.backoff_multiplier ** (attempt - 1))
        if self.jitter_ratio:
            jitter_value = random() if jitter_value is None else jitter_value
            delay += delay * self.jitter_ratio * ((jitter_value * 2.0) - 1.0)
        delay = max(delay, 0.0)
        if self.max_delay_seconds is not None:
            delay = min(delay, self.max_delay_seconds)
        return delay

    def exception_category(self, exception: BaseException) -> str:
        exception_names = {type(exception).__name__}
        exception_names.update(base.__name__ for base in type(exception).__mro__)
        if exception_names.intersection(self.timeout_exception_types) or any("Timeout" in name for name in exception_names):
            return "timeout"
        if any("Connect" in name or "Connection" in name for name in exception_names):
            return "connection"
        return "exception"

    def status_category(self, status_code: Optional[int]) -> str:
        if status_code == 429:
            return "rate_limit"
        if status_code == 408:
            return "timeout"
        if status_code is not None and 500 <= status_code <= 599:
            return "server_error"
        return "status"


class RetryModel(CallableModel, Generic[C, R]):
    model: CallableModelGenericType[C, R]
    policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @property
    def context_type(self) -> Type[ContextType]:
        return self.model.context_type

    @property
    def result_type(self) -> Type[ResultType]:
        return RetryResult

    def _sleep(self, delay_seconds: float) -> None:
        if delay_seconds > 0:
            sleep(delay_seconds)

    def _event(self, **values: Any) -> Dict[str, Any]:
        return RetryEvent(**values).model_dump(exclude_none=True)

    def _summary(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "attempts": max((event["attempt"] for event in events), default=0),
            "retried": sum(1 for event in events if event["outcome"] == "retry"),
            "succeeded": sum(1 for event in events if event["outcome"] == "success"),
            "failed": sum(1 for event in events if event["outcome"] == "failed"),
        }

    def _result_value(self, result: R) -> Any:
        return result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")

    def _retry_status_event(self, status_code: int, attempt: int) -> Dict[str, Any]:
        delay_seconds = self.policy.delay_seconds(attempt)
        return self._event(
            attempt=attempt,
            outcome="retry",
            delay_seconds=delay_seconds,
            status_code=status_code,
            category=self.policy.status_category(status_code),
            message=f"retryable status code {status_code}",
        )

    def _retry_exception_event(self, exception: Exception, attempt: int) -> Dict[str, Any]:
        delay_seconds = self.policy.delay_seconds(attempt)
        return self._event(
            attempt=attempt,
            outcome="retry",
            delay_seconds=delay_seconds,
            exception_type=type(exception).__name__,
            category=self.policy.exception_category(exception),
            message=str(exception),
        )

    def _raise_failure(self, message: str, events: List[Dict[str, Any]], exception: Optional[Exception] = None) -> None:
        summary = self._summary(events)
        error = RetryExecutionError(message, events=events, summary=summary)
        if exception is not None:
            raise error from exception
        raise error

    @Flow.call
    def __call__(self, context: C) -> RetryResult:
        events: List[Dict[str, Any]] = []
        for attempt in range(1, self.policy.max_attempts + 1):
            try:
                result = self.model(context=context)
            except Exception as exc:
                if self.policy.should_retry_exception(exc, attempt):
                    event = self._retry_exception_event(exception=exc, attempt=attempt)
                    events.append(event)
                    self._sleep(event["delay_seconds"])
                    continue
                events.append(
                    self._event(
                        attempt=attempt,
                        outcome="failed",
                        exception_type=type(exc).__name__,
                        category=self.policy.exception_category(exc),
                        message=str(exc),
                    )
                )
                self._raise_failure(f"Retry attempts exhausted after {attempt} attempt(s).", events=events, exception=exc)

            status_code = getattr(result, "status_code", None)
            if self.policy.should_retry_status(status_code, attempt):
                event = self._retry_status_event(status_code=status_code, attempt=attempt)
                events.append(event)
                self._sleep(event["delay_seconds"])
                continue
            if status_code in self.policy.retry_status_codes:
                events.append(
                    self._event(
                        attempt=attempt,
                        outcome="failed",
                        status_code=status_code,
                        category=self.policy.status_category(status_code),
                        message=f"retryable status code {status_code}",
                    )
                )
                self._raise_failure(f"Retry attempts exhausted after {attempt} attempt(s).", events=events)

            events.append(self._event(attempt=attempt, outcome="success"))
            summary = self._summary(events)
            return RetryResult(value={**summary, "events": events, "result": self._result_value(result)})

        self._raise_failure(f"Retry attempts exhausted after {self.policy.max_attempts} attempt(s).", events=events)
