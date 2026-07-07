from pathlib import Path
from shutil import copyfile
from typing import Any, Dict, Literal, Optional, Type
from urllib.parse import urlparse
from uuid import uuid4

from ccflow import CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType

from ..formats import CacheFormat, PayloadCodec

__all__ = (
    "LocalWriteContext",
    "LocalFileOutput",
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


class LocalFileOutput(WriteModel):
    path: Path = Path("cc-etl-output")
    prefix: str = ""

    @property
    def context_type(self) -> Type[ContextType]:
        return LocalWriteContext

    @property
    def result_type(self) -> Type[ResultType]:
        return LocalWriteResult

    @Flow.call
    def __call__(self, context: LocalWriteContext) -> LocalWriteResult:
        result = self.write(str(context.path), self.codec.encode(context.payload) if not isinstance(context.payload, bytes) else context.payload)
        return LocalWriteResult(path=result["path"], status=result["status"])

    def object_key(self, key: str) -> str:
        clean_prefix = self.prefix.strip("/")
        clean_key = key.lstrip("/")
        return f"{clean_prefix}/{clean_key}" if clean_prefix else clean_key

    def file_path(self, key: str) -> Path:
        return self.path / self.object_key(key)

    def artifact_uri(self, key: str) -> str:
        return self.file_path(key).resolve().as_uri()

    def exists(self, key: str) -> bool:
        return self.file_path(key).exists()

    def read(self, key: str) -> bytes:
        return self.file_path(key).read_bytes()

    def write(self, key: str, payload: bytes, media_type: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        output_path = self.file_path(key)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_name(f".{output_path.name}.{uuid4().hex}.tmp")
        try:
            temp_path.write_bytes(payload)
            temp_path.replace(output_path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        return {"path": str(output_path), "media_type": media_type, "status": "written", **(metadata or {})}

    def publish(
        self, key: str, source_key: Optional[str] = None, source_uri: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if source_key:
            source_path = self.file_path(source_key)
        elif source_uri:
            parsed = urlparse(source_uri)
            if parsed.scheme != "file":
                raise ValueError("LocalFileOutput.publish only supports file:// source_uri values.")
            source_path = Path(parsed.path)
        else:
            raise ValueError("LocalFileOutput.publish requires source_key or source_uri.")
        output_path = self.file_path(key)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        copyfile(source_path, output_path)
        return {"path": str(output_path), "source_key": source_key, "source_uri": source_uri, "status": "published", **(metadata or {})}
