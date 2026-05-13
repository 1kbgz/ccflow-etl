from typing import List, Optional

from ccflow import BaseModel
from pydantic import Field

__all__ = ("RetryPolicy",)


class RetryPolicy(BaseModel):
    max_attempts: int = 1
    retry_status_codes: List[int] = Field(default_factory=lambda: [429, 500, 502, 503, 504])
    retry_exception_types: List[str] = Field(default_factory=lambda: ["TimeoutException", "ConnectError"])

    def should_retry_status(self, status_code: Optional[int], attempt: int) -> bool:
        return status_code in self.retry_status_codes and attempt < self.max_attempts

    def should_retry_exception(self, exception: BaseException, attempt: int) -> bool:
        exception_names = {type(exception).__name__}
        exception_names.update(base.__name__ for base in type(exception).__mro__)
        return bool(exception_names.intersection(self.retry_exception_types)) and attempt < self.max_attempts
