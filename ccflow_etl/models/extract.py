from logging import getLogger
from typing import List, Tuple

from ccflow import CallableModel, CallableModelGenericType, Flow

from .common import ETLContext, ETLResult

__all__ = ("ExtractModel",)

_log = getLogger(__name__)


class ExtractModel(CallableModel):
    @Flow.deps
    def __deps__(self, context: ETLContext) -> List[Tuple[CallableModelGenericType[ETLContext, ETLResult], List[ETLContext]]]:
        deps = []
        if context.extract:
            for step in context.extract:
                deps.append((self, [step]))
        return deps

    @Flow.call
    def __call__(self, context: ETLContext) -> ETLResult:
        _log.info("Extract step started for context: %s", context)
        return ETLResult(step="extract", status="success")
