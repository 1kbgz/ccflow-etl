from logging import getLogger
from typing import List, Optional, Tuple

from ccflow import CallableModel, CallableModelGenericType, Flow

from .common import ETLContext, ETLResult
from .extract import ExtractModel

__all__ = ("TransformModel",)

_log = getLogger(__name__)


class TransformModel(CallableModel):
    _extract: Optional[ExtractModel] = None

    @Flow.deps
    def __deps__(self, context: ETLContext) -> List[Tuple[CallableModelGenericType[ETLContext, ETLResult], List[ETLContext]]]:
        deps = []
        if context.transform:
            deps.append((self, context.transform))
        if self._extract:
            # Force ordering
            deps.append((self._extract, [context]))
        return deps

    @Flow.call
    def __call__(self, context: ETLContext) -> ETLResult:
        _log.info("Transform step started for context: %s", context)
        return ETLResult(step="transform", status="success")
