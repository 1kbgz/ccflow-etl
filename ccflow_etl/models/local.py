from pathlib import Path
from typing import Any, Literal, Type
from uuid import uuid4

from ccflow import CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType

from .formats import CacheFormat, PayloadCodec

__all__ = (
    "LocalWriteContext",
    "LocalWriteModel",
    "LocalWriteResult",
    "WriteContext",
    "WriteModel",
    "WriteResult",
)


class WriteContext(ContextBase):
    payload: Any
    overwrite: bool = False


class WriteResult(ResultBase):
    status: Literal["written", "exists"]


class WriteModel(CallableModel):
    format: CacheFormat = "json"

    @property
    def codec(self) -> PayloadCodec:
        return PayloadCodec(format=self.format)


class LocalWriteContext(WriteContext):
    path: Path


class LocalWriteResult(WriteResult):
    path: str


class LocalWriteModel(WriteModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return LocalWriteContext

    @property
    def result_type(self) -> Type[ResultType]:
        return LocalWriteResult

    @Flow.call
    def __call__(self, context: LocalWriteContext) -> LocalWriteResult:
        if context.path.exists() and not context.overwrite:
            return LocalWriteResult(path=str(context.path), status="exists")

        context.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = context.path.with_name(f".{context.path.name}.{uuid4().hex}.tmp")
        try:
            temp_path.write_bytes(self.codec.encode(context.payload))
            temp_path.replace(context.path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        return LocalWriteResult(path=str(context.path), status="written")
