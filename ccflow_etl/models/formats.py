import csv
import json
from gzip import compress, decompress
from io import BytesIO, StringIO
from typing import Any, Dict, List, Literal, Union

from ccflow import BaseModel

__all__ = (
    "CacheFormat",
    "CacheFormatName",
    "PayloadCodec",
)

CacheFormatName = Literal["binary", "text", "json", "csv", "parquet", "gzip"]
CacheFormat = Union[CacheFormatName, List[CacheFormatName]]


class PayloadCodec(BaseModel):
    format: CacheFormat = "json"

    @property
    def formats(self) -> List[CacheFormatName]:
        return [self.format] if isinstance(self.format, str) else list(self.format)

    @property
    def content_format(self) -> CacheFormatName:
        content_formats = [format_name for format_name in self.formats if format_name != "gzip"]
        if not content_formats:
            return "binary"
        if len(content_formats) > 1:
            raise ValueError(f"Cache payloads support one content format plus optional gzip compression, got {self.formats!r}")
        return content_formats[0]

    @property
    def suffix(self) -> str:
        suffixes = {
            "binary": "",
            "text": ".txt",
            "json": ".json",
            "csv": ".csv",
            "parquet": ".parquet",
        }
        suffix = suffixes[self.content_format]
        return f"{suffix}.gz" if "gzip" in self.formats else suffix

    @property
    def media_type(self) -> str | None:
        media_types = {
            "binary": None,
            "text": "text/plain; charset=utf-8",
            "json": "application/json",
            "csv": "text/csv; charset=utf-8",
            "parquet": "application/vnd.apache.parquet",
        }
        return media_types[self.content_format]

    def encode(self, payload: Any) -> bytes:
        match self.content_format:
            case "binary":
                if isinstance(payload, bytes):
                    data = payload
                elif isinstance(payload, str):
                    data = payload.encode("utf-8")
                else:
                    raise TypeError("Binary cache payloads require bytes or str input.")
            case "text":
                data = str(payload).encode("utf-8")
            case "json":
                data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
            case "csv":
                if not isinstance(payload, (dict, list)):
                    raise TypeError("CSV cache payloads require a dict row or list of dict rows.")
                data = self._write_csv(payload)
            case "parquet":
                data = self._write_parquet(payload)
            case _:
                raise ValueError(f"Unsupported cache format: {self.content_format}")
        return compress(data) if "gzip" in self.formats else data

    def decode(self, payload: bytes) -> Any:
        data = decompress(payload) if "gzip" in self.formats else payload
        match self.content_format:
            case "binary":
                return data
            case "text":
                return data.decode("utf-8")
            case "json":
                return json.loads(data)
            case "csv":
                return list(csv.DictReader(StringIO(data.decode("utf-8"))))
            case "parquet":
                return self._read_parquet(data)
            case _:
                raise ValueError(f"Unsupported cache format: {self.content_format}")

    def _write_csv(self, payload: dict | List[Dict[str, Any]]) -> bytes:
        rows = payload if isinstance(payload, list) else [payload]
        if not rows:
            return b""
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue().encode("utf-8")

    def _write_parquet(self, payload: Any) -> bytes:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError("Parquet cache writes require pyarrow.") from exc
        table = self._arrow_table(payload, pa)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()

    def _read_parquet(self, payload: bytes) -> Any:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError("Parquet cache reads require pyarrow.") from exc
        return pq.read_table(BytesIO(payload))

    def _arrow_table(self, payload: Any, pa: Any) -> Any:
        if isinstance(payload, pa.Table):
            return payload
        if isinstance(payload, pa.RecordBatch):
            return pa.Table.from_batches([payload])
        if hasattr(payload, "to_arrow"):
            return self._arrow_table(payload.to_arrow(), pa)
        if isinstance(payload, list):
            return pa.Table.from_pylist(payload)
        if isinstance(payload, dict):
            if all(isinstance(value, (list, tuple)) for value in payload.values()):
                return pa.table(payload)
            return pa.Table.from_pylist([payload])
        raise TypeError("Parquet cache payloads require a pyarrow Table/RecordBatch, a to_arrow() object, a dict row, or rows.")
