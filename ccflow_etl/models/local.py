import json
from pathlib import Path
from typing import Any, Literal, Type
from uuid import uuid4

from ccflow import CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType

__all__ = (
    "LocalJSONWriteContext",
    "LocalJSONWriteModel",
    "LocalJSONWriteResult",
)


class LocalJSONWriteContext(ContextBase):
    path: Path
    payload: Any
    overwrite: bool = False


class LocalJSONWriteResult(ResultBase):
    path: str
    status: Literal["written", "exists"]


class LocalJSONWriteModel(CallableModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return LocalJSONWriteContext

    @property
    def result_type(self) -> Type[ResultType]:
        return LocalJSONWriteResult

    @Flow.call
    def __call__(self, context: LocalJSONWriteContext) -> LocalJSONWriteResult:
        if context.path.exists() and not context.overwrite:
            return LocalJSONWriteResult(path=str(context.path), status="exists")

        context.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = context.path.with_name(f".{context.path.name}.{uuid4().hex}.tmp")
        try:
            temp_path.write_text(json.dumps(context.payload, indent=2, sort_keys=True) + "\n")
            temp_path.replace(context.path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        return LocalJSONWriteResult(path=str(context.path), status="written")
