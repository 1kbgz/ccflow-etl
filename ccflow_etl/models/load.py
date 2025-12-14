from logging import getLogger
from typing import List, Optional, Tuple

from ccflow import CallableModel, CallableModelGenericType, Flow

from .common import ETLContext, ETLResult
from .extract import ExtractModel
from .transform import TransformModel

__all__ = ("LoadModel",)

_log = getLogger(__name__)


class LoadModel(CallableModel):
    _extract: Optional[ExtractModel] = None
    _transform: Optional[TransformModel] = None

    @Flow.deps
    def __deps__(self, context: ETLContext) -> List[Tuple[CallableModelGenericType[ETLContext, ETLResult], List[ETLContext]]]:
        deps = []
        if context.load:
            deps.append((self, context.load))
        if self._extract:
            # Force ordering
            deps.append((self._extract, [context]))
        if self._transform:
            # Force ordering
            deps.append((self._transform, [context]))
        return deps

    @Flow.call
    def __call__(self, context: ETLContext) -> ETLResult:
        _log.info("Load step started for context: %s", context)
        return ETLResult(step="load", status="success")
