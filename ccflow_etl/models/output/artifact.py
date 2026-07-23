from typing import Any

from ccflow import BaseModel, CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType
from pydantic import Field

from ..common import ETLArtifact, ETLStage

__all__ = (
    "ArtifactExistsContext",
    "ArtifactExistsModel",
    "ArtifactExistsResult",
    "ArtifactPublishContext",
    "ArtifactPublishModel",
    "ArtifactPublishResult",
    "ArtifactReadContext",
    "ArtifactReadModel",
    "ArtifactReadResult",
    "ArtifactWriteContext",
    "ArtifactWriteModel",
    "ArtifactWriteResult",
    "NoOpArtifactStore",
)


def _artifact_uri(store: Any, key: str) -> str:
    if hasattr(store, "artifact_uri"):
        return store.artifact_uri(key)
    if hasattr(store, "uri"):
        return store.uri(key)
    return key


def _artifact_exists(store: Any, key: str) -> bool:
    if hasattr(store, "exists"):
        return store.exists(key)
    return False


def _artifact_read(store: Any, key: str) -> bytes:
    if hasattr(store, "read"):
        payload = store.read(key)
    elif hasattr(store, "get_bytes"):
        payload = store.get_bytes(key)
    else:
        raise ValueError("artifact store does not support reading")
    return payload.encode() if isinstance(payload, str) else payload


def _response_metadata(response: Any) -> dict[str, Any]:
    return response if isinstance(response, dict) else {}


class ArtifactExistsContext(ContextBase):
    key: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactExistsResult(ResultBase):
    key: str
    uri: str
    exists: bool
    status: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactReadContext(ContextBase):
    key: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactReadResult(ResultBase):
    key: str
    uri: str
    payload: bytes
    status: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactWriteContext(ContextBase):
    key: str
    payload: bytes = b""
    media_type: str | None = None
    dataset: str | None = None
    stage: ETLStage = "load"
    overwrite: bool = False
    dry_run: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactWriteResult(ResultBase):
    key: str
    uri: str
    status: str
    artifact: ETLArtifact
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactPublishContext(ContextBase):
    key: str
    source_key: str | None = None
    source_uri: str | None = None
    media_type: str | None = None
    dataset: str | None = None
    stage: ETLStage = "load"
    overwrite: bool = False
    dry_run: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactPublishResult(ResultBase):
    key: str
    uri: str
    status: str
    source_key: str | None = None
    source_uri: str | None = None
    artifact: ETLArtifact
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactExistsModel(CallableModel):
    store: Any

    @property
    def context_type(self) -> type[ContextType]:
        return ArtifactExistsContext

    @property
    def result_type(self) -> type[ResultType]:
        return ArtifactExistsResult

    @Flow.call
    def __call__(self, context: ArtifactExistsContext) -> ArtifactExistsResult:
        exists = _artifact_exists(self.store, context.key)
        return ArtifactExistsResult(
            key=context.key,
            uri=_artifact_uri(self.store, context.key),
            exists=exists,
            status="exists" if exists else "missing",
            metadata=context.metadata,
        )


class ArtifactWriteModel(CallableModel):
    store: Any

    @property
    def context_type(self) -> type[ContextType]:
        return ArtifactWriteContext

    @property
    def result_type(self) -> type[ResultType]:
        return ArtifactWriteResult

    @Flow.call
    def __call__(self, context: ArtifactWriteContext) -> ArtifactWriteResult:
        uri = _artifact_uri(self.store, context.key)
        if context.dry_run:
            status = "planned"
            metadata = dict(context.metadata)
        elif not context.overwrite and _artifact_exists(self.store, context.key):
            status = "exists"
            metadata = dict(context.metadata)
        else:
            response = self.store.write(context.key, context.payload, media_type=context.media_type, metadata=context.metadata)
            metadata = {**context.metadata, **_response_metadata(response)}
            status = str(metadata.pop("status", "written"))
        artifact = ETLArtifact(key=context.key, stage=context.stage, dataset=context.dataset, uri=uri, media_type=context.media_type, status=status)
        return ArtifactWriteResult(key=context.key, uri=uri, status=status, artifact=artifact, metadata=metadata)


class ArtifactReadModel(CallableModel):
    """Read a full artifact payload, propagating backend errors for missing keys."""

    store: Any

    @property
    def context_type(self) -> type[ContextType]:
        return ArtifactReadContext

    @property
    def result_type(self) -> type[ResultType]:
        return ArtifactReadResult

    @Flow.call
    def __call__(self, context: ArtifactReadContext) -> ArtifactReadResult:
        return ArtifactReadResult(
            key=context.key,
            uri=_artifact_uri(self.store, context.key),
            payload=_artifact_read(self.store, context.key),
            status="read",
            metadata=context.metadata,
        )


class ArtifactPublishModel(CallableModel):
    store: Any

    @property
    def context_type(self) -> type[ContextType]:
        return ArtifactPublishContext

    @property
    def result_type(self) -> type[ResultType]:
        return ArtifactPublishResult

    @Flow.call
    def __call__(self, context: ArtifactPublishContext) -> ArtifactPublishResult:
        uri = _artifact_uri(self.store, context.key)
        if context.dry_run:
            status = "planned"
            metadata = dict(context.metadata)
        elif not context.overwrite and _artifact_exists(self.store, context.key):
            status = "exists"
            metadata = dict(context.metadata)
        else:
            response = self.store.publish(context.key, source_key=context.source_key, source_uri=context.source_uri, metadata=context.metadata)
            metadata = {**context.metadata, **_response_metadata(response)}
            status = str(metadata.pop("status", "published"))
        artifact = ETLArtifact(key=context.key, stage=context.stage, dataset=context.dataset, uri=uri, media_type=context.media_type, status=status)
        return ArtifactPublishResult(
            key=context.key,
            uri=uri,
            status=status,
            source_key=context.source_key,
            source_uri=context.source_uri,
            artifact=artifact,
            metadata=metadata,
        )


class NoOpArtifactStore(BaseModel):
    uri_prefix: str = "noop://artifact"

    def artifact_uri(self, key: str) -> str:
        return f"{self.uri_prefix}/{key.lstrip('/')}"

    def exists(self, key: str) -> bool:
        return False

    def list_keys(self, prefix: str = "") -> list[str]:
        return []

    def write(self, key: str, payload: bytes, media_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"key": key, "media_type": media_type, "status": "noop", **(metadata or {})}

    def write_file(self, key: str, path: Any, media_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"key": key, "path": str(path), "media_type": media_type, "status": "noop", **(metadata or {})}

    def publish(
        self, key: str, source_key: str | None = None, source_uri: str | None = None, metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return {"key": key, "source_key": source_key, "source_uri": source_uri, "status": "noop", **(metadata or {})}
