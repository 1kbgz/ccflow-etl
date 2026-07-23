from pathlib import Path
from typing import Any
from uuid import uuid4

from ccflow import BaseModel, CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType

from .common import ETLArtifact, ETLStage
from .formats import CacheFormat, PayloadCodec

__all__ = (
    "CacheGetContext",
    "CacheGetModel",
    "CacheGetResult",
    "CachePutContext",
    "CachePutModel",
    "CachePutResult",
    "LocalCacheStore",
    "NoOpCacheStore",
)


def _cache_key(key: str, suffix: str) -> str:
    if suffix and not key.endswith(suffix):
        return f"{key}{suffix}"
    return key


def _cache_uri(store: Any, key: str) -> str:
    if hasattr(store, "uri"):
        return store.uri(key)
    return key


def _artifact(key: str, uri: str, dataset: str | None, stage: ETLStage, status: str, codec: PayloadCodec) -> ETLArtifact:
    return ETLArtifact(key=key, dataset=dataset, stage=stage, uri=uri, media_type=codec.media_type, status=status)


def _logical_key(key: str | None, path: Path | None) -> str:
    if key is not None:
        return key
    if path is not None:
        return str(path)
    raise ValueError("Cache contexts require either key or path.")


def _resolve_cache_key(store: Any, key: str | None, path: Path | None, suffix: str) -> str:
    if hasattr(store, "resolve_key"):
        return store.resolve_key(key=key, path=path, suffix=suffix)
    if path is not None:
        raise ValueError("Path-based cache contexts require a store with resolve_key().")
    if key is None:
        raise ValueError("Cache contexts require either key or path.")
    return _cache_key(key.lstrip("/"), suffix)


class CachePutContext(ContextBase):
    key: str | None = None
    path: Path | None = None
    payload: Any
    dataset: str | None = None
    stage: ETLStage = "load"
    overwrite: bool = False


class CachePutResult(ResultBase):
    key: str
    cache_key: str
    uri: str
    format: CacheFormat
    media_type: str | None = None
    status: str
    artifact: ETLArtifact


class CacheGetContext(ContextBase):
    key: str | None = None
    path: Path | None = None
    dataset: str | None = None
    stage: ETLStage = "load"
    missing_ok: bool = True


class CacheGetResult(ResultBase):
    key: str
    cache_key: str
    uri: str
    format: CacheFormat
    media_type: str | None = None
    status: str
    payload: Any | None = None
    artifact: ETLArtifact


class CachePutModel(CallableModel):
    store: Any
    format: CacheFormat = "json"

    @property
    def codec(self) -> PayloadCodec:
        return PayloadCodec(format=self.format)

    @property
    def context_type(self) -> type[ContextType]:
        return CachePutContext

    @property
    def result_type(self) -> type[ResultType]:
        return CachePutResult

    def resolve_key(self, context: CachePutContext) -> str:
        return _resolve_cache_key(self.store, key=context.key, path=context.path, suffix=self.codec.suffix)

    @Flow.call
    def __call__(self, context: CachePutContext) -> CachePutResult:
        codec = self.codec
        logical_key = _logical_key(context.key, context.path)
        cache_key = self.resolve_key(context)
        if self.store.exists(cache_key) and not context.overwrite:
            status = "exists"
        else:
            self.store.put_bytes(cache_key, codec.encode(context.payload), content_type=codec.media_type)
            status = "written"
        uri = _cache_uri(self.store, cache_key)
        artifact = _artifact(key=logical_key, uri=uri, dataset=context.dataset, stage=context.stage, status=status, codec=codec)
        return CachePutResult(
            key=logical_key,
            cache_key=cache_key,
            uri=uri,
            format=self.format,
            media_type=codec.media_type,
            status=status,
            artifact=artifact,
        )


class CacheGetModel(CallableModel):
    store: Any
    format: CacheFormat = "json"

    @property
    def codec(self) -> PayloadCodec:
        return PayloadCodec(format=self.format)

    @property
    def context_type(self) -> type[ContextType]:
        return CacheGetContext

    @property
    def result_type(self) -> type[ResultType]:
        return CacheGetResult

    def resolve_key(self, context: CacheGetContext) -> str:
        return _resolve_cache_key(self.store, key=context.key, path=context.path, suffix=self.codec.suffix)

    @Flow.call
    def __call__(self, context: CacheGetContext) -> CacheGetResult:
        codec = self.codec
        logical_key = _logical_key(context.key, context.path)
        cache_key = self.resolve_key(context)
        uri = _cache_uri(self.store, cache_key)
        if not self.store.exists(cache_key):
            if not context.missing_ok:
                raise FileNotFoundError(uri)
            artifact = _artifact(key=logical_key, uri=uri, dataset=context.dataset, stage=context.stage, status="miss", codec=codec)
            return CacheGetResult(
                key=logical_key,
                cache_key=cache_key,
                uri=uri,
                format=self.format,
                media_type=codec.media_type,
                status="miss",
                payload=None,
                artifact=artifact,
            )
        payload = codec.decode(self.store.get_bytes(cache_key))
        artifact = _artifact(key=logical_key, uri=uri, dataset=context.dataset, stage=context.stage, status="hit", codec=codec)
        return CacheGetResult(
            key=logical_key,
            cache_key=cache_key,
            uri=uri,
            format=self.format,
            media_type=codec.media_type,
            status="hit",
            payload=payload,
            artifact=artifact,
        )


class LocalCacheStore(BaseModel):
    root: Path | None = None

    def resolve_path(self, path: Path | None = None, key: str | None = None, suffix: str = "") -> Path:
        if path is not None:
            return path
        if self.root is None or not key:
            raise ValueError("LocalCacheStore requires either a path or both root and key")
        return self.root / _cache_key(key, suffix)

    def resolve_key(self, key: str | None = None, path: Path | None = None, suffix: str = "") -> str:
        return str(self.resolve_path(path=path, key=key, suffix=suffix))

    def uri(self, key: str) -> str:
        return str(Path(key))

    def exists(self, key: str) -> bool:
        return Path(key).exists()

    def put_bytes(self, key: str, value: bytes, content_type: str | None = None) -> str:
        path = Path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            temp_path.write_bytes(value)
            temp_path.replace(path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        return str(path)

    def get_bytes(self, key: str) -> bytes:
        return Path(key).read_bytes()


class NoOpCacheStore(BaseModel):
    def uri(self, key: str) -> str:
        return f"noop://cache/{key}"

    def exists(self, key: str) -> bool:
        return False

    def put_bytes(self, key: str, value: bytes, content_type: str | None = None) -> dict[str, str]:
        return {"key": key, "status": "noop"}

    def get_bytes(self, key: str) -> bytes:
        raise FileNotFoundError(self.uri(key))
