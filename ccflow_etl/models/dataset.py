import io
import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal

from ccflow import BaseModel, CallableModel, ContextBase, Flow, ModelRegistry
from ccflow.result.narwhals import NarwhalsFrameResult
from pydantic import Field, PrivateAttr

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


def _standard_type(data_type: Any) -> Any:
    import pyarrow as pa

    if pa.types.is_large_string(data_type):
        return pa.string()
    if pa.types.is_large_binary(data_type):
        return pa.binary()
    if pa.types.is_large_list(data_type):
        return pa.list_(_standard_type(data_type.value_type))
    if pa.types.is_list(data_type):
        return pa.list_(_standard_type(data_type.value_type))
    if pa.types.is_struct(data_type):
        return pa.struct([field.with_type(_standard_type(field.type)) for field in data_type])
    return data_type


def _standard_types(table: Any) -> Any:
    """Cast 64-bit-offset Arrow types (which Polars writes) to their standard forms before schema checks."""
    import pyarrow as pa

    schema = pa.schema([field.with_type(_standard_type(field.type)) for field in table.schema], metadata=table.schema.metadata)
    return table if schema == table.schema else table.cast(schema)


def _required_columns(table: Any, expected: Any) -> Any:
    """Mark columns the expected schema requires as non-nullable once they are checked to hold no nulls.

    Frames from Polars and most builders declare every column nullable, which schema policies cannot narrow, so the
    check happens here: a required column with nulls is an error, and one without becomes non-nullable.
    """
    import pyarrow as pa

    fields = []
    for field in table.schema:
        index = expected.get_field_index(field.name)
        if index != -1 and not expected.field(index).nullable and field.nullable:
            nulls = table.column(field.name).null_count
            if nulls:
                raise ValueError(f"column {field.name!r} has {nulls} nulls but the expected schema requires values")
            field = field.with_nullable(False)
        fields.append(field)
    schema = pa.schema(fields, metadata=table.schema.metadata)
    return table if schema == table.schema else table.cast(schema)


def _schema_reference(value: Any) -> Any:
    """An fsspec-data schema reference for a URL string or a reference mapping; ``pkg://package/path`` names a package file.

    Files ending in ``.json`` are read as Arrow JSON unless the mapping sets ``format``.
    """
    from fsspec_data.schema import SchemaRef

    if isinstance(value, str):
        value = {"url": value}
    url = value["url"]
    if url.startswith("pkg://"):
        from importlib.resources import files

        package, _, resource = url.removeprefix("pkg://").partition("/")
        url = str(files(package).joinpath(resource))
    # A .json schema file is Arrow JSON unless the reference says otherwise (fsspec-data cannot tell it from JSON Schema).
    schema_format = value.get("format") or ("arrow-json" if url.endswith(".json") else None)
    return SchemaRef(url, format=schema_format, storage_options=value.get("storage_options") or {})


class DatasetDefinition(BaseModel):
    """A dataset partitioned by date in an artifact store: where each partition lives and how to read and write it.

    ``store`` and ``calendar`` may be objects or registry paths (such as ``/output``), resolved when used. When
    ``local_store`` is set, writes go to both stores and reads try the local store first; ``read_bytes`` copies
    partitions it reads from ``store`` into it.

    ``expected_schema`` is an optional Arrow schema for Parquet partitions, given as an fsspec-data reference (a URL to
    a schema file, ``pkg://package/path`` for a file inside an installed package, or a mapping with ``url``,
    ``format``, and ``storage_options``) or inline as a mapping with ``fields``. Frames written with ``write_frame``
    are reconciled to it under ``schema_policy`` and rejected when they do not fit, and ``scan`` reads every partition
    in one lazy scan with that schema. Schemas need the ``fsspec`` extra.
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
    expected_schema: Any | None = Field(default=None, description="Expected Arrow schema: an fsspec-data reference or an inline mapping with fields.")
    schema_policy: Literal["exact", "projection", "compatible", "coerce"] = Field(
        default="compatible", description="How written frames may differ from the expected schema (fsspec-data schema policies)."
    )
    _resolved_schema: Any = PrivateAttr(default=None)

    def resolved_schema(self) -> Any:
        """The expected schema as an fsspec-data ``ResolvedSchema`` (Arrow schema plus provenance), or None."""
        if self.expected_schema is None:
            return None
        if self._resolved_schema is None:
            from fsspec_data.schema import ResolvedSchema, SchemaProvenance, resolve_schema, schema_from_options

            value = self.expected_schema
            if isinstance(value, dict) and "fields" in value:
                self._resolved_schema = ResolvedSchema(
                    schema_from_options(value), SchemaProvenance(source="inline", format="arrow-json", provider="inline")
                )
            else:
                self._resolved_schema = resolve_schema(_schema_reference(value))
        return self._resolved_schema

    def _polars_schema(self) -> Any:
        resolved = self.resolved_schema()
        if resolved is None:
            return None
        import polars as pl

        return pl.DataFrame(resolved.schema.empty_table()).schema

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
        resolved = self.resolved_schema()
        if resolved is not None:
            from fsspec_data import plan_schema

            table = _required_columns(_standard_types(table), resolved.schema)
            table = plan_schema(table.schema, resolved.schema, self.schema_policy).apply_table(table)
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
                **({"expected_schema": {**resolved.provenance.__dict__, "policy": self.schema_policy}} if resolved is not None else {}),
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

    def _frame(self, payload: bytes, value: date) -> Any:
        import polars as pl

        if self.format == "parquet":
            frame = pl.read_parquet(io.BytesIO(payload))
        elif self.format in ("jsonl", "jsonl.gz"):
            frame = pl.read_ndjson(io.BytesIO(payload))
        elif self.format in ("csv", "csv.gz"):
            frame = pl.read_csv(io.BytesIO(payload))
        else:
            raise ValueError(f"reading frames does not support {self.format}; use read_bytes")
        if self.partition_column and self.partition_column not in frame.columns:
            frame = frame.with_columns(pl.lit(value).alias(self.partition_column))
        return frame

    def read_frame(self, value: date) -> Any:
        """Read one partition as a Polars DataFrame."""
        return self._frame(self.read_bytes(value), value)

    def _present(self, store: Any, dates: list[date]) -> list[date]:
        """The dates whose partitions ``store`` holds, from one listing where the store supports it."""
        keys = {value: self.key(value) for value in dates}
        list_keys = getattr(store, "list_keys", None)
        if list_keys is not None:
            available = set(list_keys(self.key_template.split("{", 1)[0]))
        else:
            available = {key for key in keys.values() if _artifact_exists(store, key)}
        return [value for value in dates if keys[value] in available]

    def _scan_store(self, store: Any, dates: list[date]) -> Any:
        import polars as pl

        if self.format != "parquet" or not hasattr(store, "fsspec_url"):
            return pl.concat([self._frame(_artifact_read(store, self.key(value)), value).lazy() for value in dates], how="diagonal_relaxed")
        urls = {store.fsspec_url(self.key(value)): value for value in dates}
        options = store.polars_storage_options() if hasattr(store, "polars_storage_options") else {}
        schema = self._polars_schema()
        if schema is None:
            # Without a schema, files may differ in columns, so each is scanned on its own and aligned.
            frames = []
            for url, value in urls.items():
                frame = pl.scan_parquet(url, storage_options=options or None)
                if self.partition_column and self.partition_column not in frame.collect_schema().names():
                    frame = frame.with_columns(pl.lit(value).alias(self.partition_column))
                frames.append(frame)
            return pl.concat(frames, how="diagonal_relaxed")
        frame = pl.scan_parquet(
            list(urls),
            storage_options=options or None,
            schema=schema,
            missing_columns="insert",
            extra_columns="ignore",
            include_file_paths="__source__" if self.partition_column else None,
        )
        if self.partition_column:
            # Files without the partition column get it from their path.
            from_path = pl.col("__source__").replace_strict(urls, return_dtype=pl.Date)
            value = pl.coalesce(pl.col(self.partition_column), from_path) if self.partition_column in schema else from_path
            frame = frame.with_columns(value.alias(self.partition_column)).drop("__source__")
        return frame

    def scan(self, start: date, end: date) -> Any:
        """Lazily read the partitions that exist between ``start`` and ``end`` as one Polars LazyFrame.

        Partitions come from ``local_store`` where it holds them and from ``store`` otherwise, each found with one
        listing. Parquet partitions are read lazily from their fsspec URLs, from local disk or object storage, so
        filters and column selections reach the files; with an ``expected_schema`` all of a store's partitions are
        one scan with that schema.
        """
        import polars as pl

        remaining = self.partitions(start, end)
        stores = [_resolve(self.local_store)] if self.local_store is not None else []
        stores.append(_resolve(self.store))
        frames = []
        for store in stores:
            if not remaining:
                break
            present = self._present(store, remaining)
            if present:
                frames.append(self._scan_store(store, present))
                taken = set(present)
                remaining = [value for value in remaining if value not in taken]
        if not frames:
            schema = self._polars_schema()
            return pl.LazyFrame(schema=schema) if schema is not None else pl.LazyFrame()
        return frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")


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
