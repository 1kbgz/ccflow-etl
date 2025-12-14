from typing import List

from ccflow import ContextBase, ResultBase

__all__ = (
    "ETLContext",
    "ETLResult",
)


class ETLContext(ContextBase):
    extract: List["ETLContext"] = []
    transform: List["ETLContext"] = []
    load: List["ETLContext"] = []


class ETLResult(ResultBase):
    step: str = "unknown"
    status: str = "unknown"

    extract: List["ETLResult"] = []
    transform: List["ETLResult"] = []
    load: List["ETLResult"] = []
