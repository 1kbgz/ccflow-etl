import json
from pathlib import Path
from typing import Any, Optional, Type

from ccflow import BaseModel, CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType

from .common import ETLArtifact, ETLStage
from .local import LocalJSONWriteContext, LocalJSONWriteModel

__all__ = (
    "LocalJSONCacheGetContext",
    "LocalJSONCacheGetModel",
    "LocalJSONCacheGetResult",
    "LocalJSONCachePutContext",
    "LocalJSONCachePutModel",
    "LocalJSONCachePutResult",
    "LocalJSONCacheStore",
)


class LocalJSONCacheStore(BaseModel):
    root: Optional[Path] = None

    def resolve_path(self, path: Optional[Path] = None, key: Optional[str] = None) -> Path:
        if path is not None:
            return path
        if self.root is None or not key:
            raise ValueError("LocalJSONCacheStore requires either a path or both root and key")
        suffix = "" if key.endswith(".json") else ".json"
        return self.root / f"{key}{suffix}"

    def artifact(self, path: Path, key: Optional[str], dataset: Optional[str], stage: ETLStage, status: str) -> ETLArtifact:
        artifact_key = key or str(path)
        return ETLArtifact(key=artifact_key, dataset=dataset, stage=stage, uri=str(path), media_type="application/json", status=status)

    def put(self, context: "LocalJSONCachePutContext") -> "LocalJSONCachePutResult":
        path = self.resolve_path(path=context.path, key=context.key)
        result = LocalJSONWriteModel()(LocalJSONWriteContext(path=path, payload=context.payload, overwrite=context.overwrite))
        artifact = self.artifact(path=path, key=context.key, dataset=context.dataset, stage=context.stage, status=result.status)
        return LocalJSONCachePutResult(key=artifact.key, path=result.path, status=result.status, artifact=artifact)

    def get(self, context: "LocalJSONCacheGetContext") -> "LocalJSONCacheGetResult":
        path = self.resolve_path(path=context.path, key=context.key)
        if not path.exists():
            if not context.missing_ok:
                raise FileNotFoundError(path)
            artifact = self.artifact(path=path, key=context.key, dataset=context.dataset, stage=context.stage, status="miss")
            return LocalJSONCacheGetResult(key=artifact.key, path=str(path), status="miss", payload=None, artifact=artifact)
        payload = json.loads(path.read_text())
        artifact = self.artifact(path=path, key=context.key, dataset=context.dataset, stage=context.stage, status="hit")
        return LocalJSONCacheGetResult(key=artifact.key, path=str(path), status="hit", payload=payload, artifact=artifact)


class LocalJSONCachePutContext(ContextBase):
    path: Optional[Path] = None
    key: Optional[str] = None
    payload: Any
    dataset: Optional[str] = None
    stage: ETLStage = "load"
    overwrite: bool = False


class LocalJSONCachePutResult(ResultBase):
    key: str
    path: str
    status: str
    artifact: ETLArtifact


class LocalJSONCacheGetContext(ContextBase):
    path: Optional[Path] = None
    key: Optional[str] = None
    dataset: Optional[str] = None
    stage: ETLStage = "load"
    missing_ok: bool = True


class LocalJSONCacheGetResult(ResultBase):
    key: str
    path: str
    status: str
    payload: Optional[Any] = None
    artifact: ETLArtifact


class LocalJSONCachePutModel(CallableModel):
    store: LocalJSONCacheStore = LocalJSONCacheStore()

    @property
    def context_type(self) -> Type[ContextType]:
        return LocalJSONCachePutContext

    @property
    def result_type(self) -> Type[ResultType]:
        return LocalJSONCachePutResult

    @Flow.call
    def __call__(self, context: LocalJSONCachePutContext) -> LocalJSONCachePutResult:
        return self.store.put(context)


class LocalJSONCacheGetModel(CallableModel):
    store: LocalJSONCacheStore = LocalJSONCacheStore()

    @property
    def context_type(self) -> Type[ContextType]:
        return LocalJSONCacheGetContext

    @property
    def result_type(self) -> Type[ResultType]:
        return LocalJSONCacheGetResult

    @Flow.call
    def __call__(self, context: LocalJSONCacheGetContext) -> LocalJSONCacheGetResult:
        return self.store.get(context)
