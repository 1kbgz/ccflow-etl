import io
import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal

from ccflow import BaseModel, CallableModel, ContextBase, Flow, ModelRegistry
from ccflow.result.narwhals import NarwhalsFrameResult
from pydantic import Field

from .common import ETLArtifact, ETLStage
from .output.artifact import _artifact_exists, _artifact_read, _artifact_uri

__all__ = (
    "DatasetDefinition",
    "DatasetFormat",
    "DatasetReadContext",
    "DatasetReadModel",
)

DatasetFormat = Literal["parquet", "json", "jsonl", "jsonl.gz", "csv", "csv.gz", "bytes"]

_MEDIA_TYPES: dict[str, str] = {
    "parquet": "application/vnd.apache.parquet",
    "json": "application/json",
    "jsonl": "application/x-ndjson",
    "jsonl.gz": "application/gzip",
    "csv": "text/csv",
    "csv.gz": "application/gzip",
    "bytes": "application/octet-stream",
}


def _resolve(value: Any) -> Any:
    return ModelRegistry.root()[value] if isinstance(value, str) else value


class DatasetDefinition(BaseModel):
    """A dataset partitioned by date in an artifact store: where each partition lives and how to read and write it.

    ``store`` and ``calendar`` may be objects or registry paths (such as ``/output``), resolved when used. When
    ``local_store`` is set, writes go to both stores and reads try the local store first, copying partitions read from
    ``store`` into it.
    """

    name: str
    key_template: str = Field(description="Partition key with {date}, {year}, {month}, {day}, and {ymd} placeholders.")
    store: Any = "/output"
    local_store: Any | None = None
    format: DatasetFormat = "parquet"
    media_type: str | None = None
    schema_name: str | None = None
    schema_version: str | None = None
    calendar: Any | None = Field(default=None, description="Calendar of expected partitions; every calendar day when unset.")
    first_date: date | None = None
    sidecar: bool = Field(default=True, description="Write a lineage sidecar next to each partition written from a frame.")
    partition_column: str | None = Field(default="date", description="Column holding the partition date in frames; added on read when missing.")

    def key(self, value: date) -> str:
        return self.key_template.format(
            date=value.isoformat(), year=f"{value.year:04d}", month=f"{value.month:02d}", day=f"{value.day:02d}", ymd=value.strftime("%Y%m%d")
        )

    def sidecar_key(self, value: date) -> str:
        key = self.key(value)
        suffix = f".{self.format}"
        return f"{key.removesuffix(suffix) if key.endswith(suffix) else key}.metadata.json"

    def uri(self, value: date) -> str:
        return _artifact_uri(_resolve(self.store), self.key(value))

    def resolved_media_type(self) -> str:
        return self.media_type or _MEDIA_TYPES[self.format]

    def exists(self, value: date) -> bool:
        return _artifact_exists(_resolve(self.store), self.key(value))

    def partitions(self, start: date, end: date) -> list[date]:
        start = max(start, self.first_date) if self.first_date else start
        if start > end:
            return []
        if self.calendar is None:
            return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
        calendar = _resolve(self.calendar)
        if hasattr(calendar, "business_days"):
            return list(calendar.business_days(start, end))
        return [step.date() if isinstance(step, datetime) else step for step in calendar.steps(start, end)]

    def missing(self, start: date, end: date) -> list[date]:
        return [value for value in self.partitions(start, end) if not self.exists(value)]

    def plan(self, value: date) -> dict[str, Any]:
        """Explain record for one partition, without reading or writing it."""
        return {
            "dataset": self.name,
            "date": value.isoformat(),
            "key": self.key(value),
            "uri": self.uri(value),
            "format": self.format,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
        }

    def _stores(self) -> list[tuple[Any, ETLStage]]:
        stores: list[tuple[Any, ETLStage]] = [(_resolve(self.store), "load")]
        if self.local_store is not None:
            stores.insert(0, (_resolve(self.local_store), "transform"))
        return stores

    def _write(self, key: str, payload: bytes, media_type: str, metadata: dict[str, Any], overwrite: bool) -> list[dict[str, Any]]:
        writes = []
        for store, stage in self._stores():
            if not overwrite and _artifact_exists(store, key):
                status = "exists"
            else:
                response = store.write(key, payload, media_type=media_type, metadata=metadata)
                status = str(response.get("status", "written")) if isinstance(response, dict) else "written"
            artifact = ETLArtifact(
                key=key, stage=stage, status=status, dataset=self.name, uri=_artifact_uri(store, key), media_type=media_type, metadata=metadata
            )
            writes.append(artifact.model_dump(mode="json"))
        return writes

    def write_bytes(self, value: date, payload: bytes, metadata: dict[str, Any] | None = None, overwrite: bool = False) -> list[dict[str, Any]]:
        """Write one partition's payload to every store, skipping stores that already hold it unless ``overwrite``."""
        return self._write(self.key(value), payload, self.resolved_media_type(), {"date": value.isoformat(), **(metadata or {})}, overwrite)

    def write_frame(self, value: date, frame: Any, lineage: dict[str, Any] | None = None, overwrite: bool = False) -> list[dict[str, Any]]:
        """Write a Polars or PyArrow frame as one Parquet partition, followed by its lineage sidecar."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        if self.format != "parquet":
            raise ValueError(f"write_frame requires a parquet dataset, not {self.format}")
        table = frame if isinstance(frame, pa.Table) else frame.to_arrow()
        if self.schema_name or self.schema_version:
            table = table.replace_schema_metadata(
                {
                    **(table.schema.metadata or {}),
                    b"dataset": self.name.encode(),
                    b"schema_name": str(self.schema_name).encode(),
                    b"schema_version": str(self.schema_version).encode(),
                }
            )
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        payload = buffer.getvalue()
        writes = self.write_bytes(value, payload, {"row_count": table.num_rows}, overwrite=overwrite)
        if self.sidecar:
            sidecar = {
                "dataset": self.name,
                "date": value.isoformat(),
                "schema_name": self.schema_name,
                "schema_version": self.schema_version,
                "produced_at": datetime.now(UTC).isoformat(),
                "output": {"key": self.key(value), "row_count": table.num_rows, "size": len(payload), "sha256": sha256(payload).hexdigest()},
                **(lineage or {}),
            }
            sidecar_payload = (json.dumps(sidecar, indent=2, sort_keys=True, default=str) + "\n").encode()
            writes += self._write(self.sidecar_key(value), sidecar_payload, "application/json", {"date": value.isoformat()}, overwrite)
        return writes

    def read_bytes(self, value: date) -> bytes:
        key = self.key(value)
        if self.local_store is not None:
            local = _resolve(self.local_store)
            if _artifact_exists(local, key):
                return _artifact_read(local, key)
            payload = _artifact_read(_resolve(self.store), key)
            local.write(key, payload, media_type=self.resolved_media_type(), metadata={"date": value.isoformat()})
            return payload
        return _artifact_read(_resolve(self.store), key)

    def read_frame(self, value: date) -> Any:
        """Read one partition as a Polars DataFrame."""
        import polars as pl

        payload = self.read_bytes(value)
        if self.format == "parquet":
            frame = pl.read_parquet(io.BytesIO(payload))
        elif self.format in ("jsonl", "jsonl.gz"):
            frame = pl.read_ndjson(io.BytesIO(payload))
        elif self.format in ("csv", "csv.gz"):
            frame = pl.read_csv(io.BytesIO(payload))
        else:
            raise ValueError(f"read_frame does not support {self.format}; use read_bytes")
        if self.partition_column and self.partition_column not in frame.columns:
            frame = frame.with_columns(pl.lit(value).alias(self.partition_column))
        return frame

    def scan(self, start: date, end: date) -> Any:
        """Lazily concatenate the partitions that exist between ``start`` and ``end`` into a Polars LazyFrame."""
        import polars as pl

        local = _resolve(self.local_store) if self.local_store is not None else None
        dates = [
            value for value in self.partitions(start, end) if (local is not None and _artifact_exists(local, self.key(value))) or self.exists(value)
        ]
        if not dates:
            return pl.LazyFrame()
        if self.format == "parquet" and local is not None and hasattr(local, "file_path"):
            for value in dates:
                if not _artifact_exists(local, self.key(value)):
                    self.read_bytes(value)
            frames = []
            for value in dates:
                frame = pl.scan_parquet(str(local.file_path(self.key(value))))
                if self.partition_column and self.partition_column not in frame.collect_schema().names():
                    frame = frame.with_columns(pl.lit(value).alias(self.partition_column))
                frames.append(frame)
            return pl.concat(frames, how="diagonal_relaxed")
        return pl.concat([self.read_frame(value).lazy() for value in dates], how="diagonal_relaxed")


class DatasetReadContext(ContextBase):
    start_date: date
    end_date: date


class DatasetReadModel(CallableModel):
    """Read a dataset's partitions over a date range as a lazy frame."""

    dataset: DatasetDefinition

    @property
    def context_type(self) -> type[DatasetReadContext]:
        return DatasetReadContext

    @property
    def result_type(self) -> type[NarwhalsFrameResult]:
        return NarwhalsFrameResult

    @Flow.call
    def __call__(self, context: DatasetReadContext) -> NarwhalsFrameResult:
        import narwhals.stable.v1 as nw

        return NarwhalsFrameResult(df=nw.from_native(self.dataset.scan(context.start_date, context.end_date)))
