from pathlib import Path
from typing import Any

from ccflow import BaseModel
from pydantic import Field

__all__ = ("FsspecArtifactStore",)

_POLARS_S3_OPTIONS = {"key": "aws_access_key_id", "secret": "aws_secret_access_key", "token": "aws_session_token"}
_POLARS_S3_CLIENT_OPTIONS = {"endpoint_url": "aws_endpoint_url", "region_name": "aws_region"}


class FsspecArtifactStore(BaseModel):
    """An artifact store under any fsspec URL, such as ``s3://bucket/prefix`` or a local directory."""

    url: str
    options: dict[str, Any] = Field(default_factory=dict, description="fsspec storage options for the filesystem behind ``url``.")
    polars_options: dict[str, Any] | None = Field(
        default=None, description="Polars storage options; derived from ``options`` for s3 URLs when not given."
    )

    @property
    def fs(self) -> Any:
        from fsspec.core import url_to_fs

        return url_to_fs(self.url, **self.options)[0]

    def fsspec_url(self, key: str) -> str:
        return f"{self.url.rstrip('/')}/{key.lstrip('/')}"

    def artifact_uri(self, key: str) -> str:
        return self.fsspec_url(key)

    def storage_options(self) -> dict[str, Any]:
        return dict(self.options)

    def polars_storage_options(self) -> dict[str, Any]:
        if self.polars_options is not None:
            return dict(self.polars_options)
        if not self.url.startswith(("s3://", "s3a://")):
            return {}
        options = {_POLARS_S3_OPTIONS[name]: value for name, value in self.options.items() if name in _POLARS_S3_OPTIONS}
        client = self.options.get("client_kwargs") or {}
        options.update({_POLARS_S3_CLIENT_OPTIONS[name]: value for name, value in client.items() if name in _POLARS_S3_CLIENT_OPTIONS})
        if self.options.get("anon"):
            options["aws_skip_signature"] = "true"
        return options

    def exists(self, key: str) -> bool:
        return self.fs.exists(self.fsspec_url(key))

    def list_keys(self, prefix: str = "") -> list[str]:
        fs = self.fs
        root = fs._strip_protocol(self.url).rstrip("/")
        target = self.fsspec_url(prefix.strip("/")) if prefix.strip("/") else self.url
        if not fs.exists(target):
            return []
        return sorted(path[len(root) :].lstrip("/") for path in fs.find(target))

    def read(self, key: str) -> bytes:
        return self.fs.cat_file(self.fsspec_url(key))

    def read_file(self, key: str, path: str | Path) -> dict[str, Any]:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.fs.get_file(self.fsspec_url(key), str(output_path))
        return {"path": str(output_path), "size": output_path.stat().st_size, "status": "materialized"}

    def write(self, key: str, payload: bytes, media_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self.fsspec_url(key)
        fs = self.fs
        fs.makedirs(url.rsplit("/", 1)[0], exist_ok=True)
        fs.pipe_file(url, payload)
        return {"path": url, "media_type": media_type, "status": "written", **(metadata or {})}

    def write_file(self, key: str, path: str | Path, media_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self.fsspec_url(key)
        fs = self.fs
        fs.makedirs(url.rsplit("/", 1)[0], exist_ok=True)
        fs.put_file(str(path), url)
        return {"path": url, "size": Path(path).stat().st_size, "media_type": media_type, "status": "written", **(metadata or {})}
