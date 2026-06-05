from typing import Optional

from ccflow import BaseModel
from pydantic import Field

__all__ = ("ExecutionPolicy",)


class ExecutionPolicy(BaseModel):
    max_concurrency: Optional[int] = Field(default=None, ge=1)
    requests_per_interval: Optional[int] = Field(default=None, ge=1)
    interval_seconds: float = Field(default=1.0, gt=0.0)
    min_interval_seconds: float = Field(default=0.0, ge=0.0)

    def effective_max_concurrency(self, default: int = 1) -> int:
        if default < 1:
            raise ValueError("default concurrency must be greater than or equal to 1")
        return self.max_concurrency or default

    def spacing_seconds(self) -> float:
        interval_spacing = self.interval_seconds / self.requests_per_interval if self.requests_per_interval else 0.0
        return max(self.min_interval_seconds, interval_spacing)

    def rate_delay_seconds(self, previous_started_at: Optional[float], now: float) -> float:
        if previous_started_at is None:
            return 0.0
        delay = previous_started_at + self.spacing_seconds() - now
        return max(round(delay, 12), 0.0)
