from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest
from ccflow import ModelRegistry

from ccflow_etl import DatasetDefinition, DatasetReadContext, DatasetReadModel, LocalFileOutput, WeekdayCalendar


def _dataset(tmp_path, **kwargs) -> DatasetDefinition:
    return DatasetDefinition(
        name="sample-bars",
        key_template="curated/bars/{year}/{month}/{date}.parquet",
        store=LocalFileOutput(path=tmp_path / "remote"),
        schema_name="bar",
        schema_version="1",
        **kwargs,
    )


def test_dataset_keys_and_sidecar_keys(tmp_path):
    dataset = _dataset(tmp_path)

    assert dataset.key(date(2025, 1, 6)) == "curated/bars/2025/01/2025-01-06.parquet"
    assert dataset.sidecar_key(date(2025, 1, 6)) == "curated/bars/2025/01/2025-01-06.metadata.json"
    assert dataset.resolved_media_type() == "application/vnd.apache.parquet"
    assert dataset.plan(date(2025, 1, 6))["key"] == "curated/bars/2025/01/2025-01-06.parquet"


def test_dataset_partitions_follow_calendar_and_first_date(tmp_path):
    every_day = _dataset(tmp_path)
    weekdays = _dataset(tmp_path, calendar=WeekdayCalendar(), first_date=date(2025, 1, 3))

    assert len(every_day.partitions(date(2025, 1, 3), date(2025, 1, 6))) == 4
    assert weekdays.partitions(date(2025, 1, 1), date(2025, 1, 6)) == [date(2025, 1, 3), date(2025, 1, 6)]


def test_dataset_write_frame_writes_partition_and_lineage_sidecar_once(tmp_path):
    dataset = _dataset(tmp_path)
    frame = pl.DataFrame({"ticker": ["AAA", "BBB"], "close": [1.0, 2.0]})

    writes = dataset.write_frame(date(2025, 1, 6), frame, lineage={"source": {"key": "raw/2025-01-06.csv.gz"}})
    sidecar = json.loads((tmp_path / "remote" / dataset.sidecar_key(date(2025, 1, 6))).read_text())

    assert [write["status"] for write in writes] == ["written", "written"]
    assert sidecar["output"]["row_count"] == 2 and len(sidecar["output"]["sha256"]) == 64
    assert sidecar["source"] == {"key": "raw/2025-01-06.csv.gz"}
    assert dataset.exists(date(2025, 1, 6))
    assert dataset.missing(date(2025, 1, 6), date(2025, 1, 7)) == [date(2025, 1, 7)]
    assert [write["status"] for write in dataset.write_frame(date(2025, 1, 6), frame)] == ["exists", "exists"]


def test_dataset_reads_local_first_and_caches_remote_partitions(tmp_path):
    dataset = _dataset(tmp_path)
    dataset.write_frame(date(2025, 1, 6), pl.DataFrame({"ticker": ["AAA"], "close": [1.0]}))
    cached = _dataset(tmp_path, local_store=LocalFileOutput(path=tmp_path / "local"))

    frame = cached.read_frame(date(2025, 1, 6))

    assert frame["ticker"].to_list() == ["AAA"]
    assert frame["date"].to_list() == [date(2025, 1, 6)]
    assert (tmp_path / "local" / cached.key(date(2025, 1, 6))).exists()


def test_dataset_scan_concatenates_existing_partitions_with_partition_column(tmp_path):
    dataset = _dataset(tmp_path, local_store=LocalFileOutput(path=tmp_path / "local"))
    dataset.write_frame(date(2025, 1, 6), pl.DataFrame({"ticker": ["AAA"], "close": [1.0]}))
    dataset.write_frame(date(2025, 1, 7), pl.DataFrame({"ticker": ["AAA", "BBB"], "close": [2.0, 3.0]}))

    lazy = dataset.scan(date(2025, 1, 1), date(2025, 1, 8))
    remote_only = _dataset(tmp_path).scan(date(2025, 1, 1), date(2025, 1, 8))

    assert isinstance(lazy, pl.LazyFrame)
    for frame in (lazy.collect(), remote_only.collect()):
        assert frame.sort("date", "ticker").select("date", "ticker").rows() == [
            (date(2025, 1, 6), "AAA"),
            (date(2025, 1, 7), "AAA"),
            (date(2025, 1, 7), "BBB"),
        ]
    assert dataset.scan(date(2025, 2, 1), date(2025, 2, 2)).collect().is_empty()


def test_dataset_resolves_registry_paths_and_read_model_returns_lazy_frame(tmp_path):
    registry = ModelRegistry.root()
    registry.add("dataset_test_store", LocalFileOutput(path=tmp_path / "remote"), overwrite=True)
    try:
        dataset = DatasetDefinition(name="sample", key_template="sample/{date}.parquet", store="/dataset_test_store")
        dataset.write_frame(date(2025, 1, 6), pl.DataFrame({"value": [1]}))

        result = DatasetReadModel(dataset=dataset)(DatasetReadContext(start_date=date(2025, 1, 6), end_date=date(2025, 1, 6)))

        assert result.df.collect().to_native()["value"].to_list() == [1]
    finally:
        registry.remove("dataset_test_store")


def test_dataset_write_frame_requires_parquet(tmp_path):
    dataset = DatasetDefinition(name="raw", key_template="raw/{date}.json", store=LocalFileOutput(path=tmp_path), format="json")

    with pytest.raises(ValueError, match="parquet"):
        dataset.write_frame(date(2025, 1, 6), pl.DataFrame({"a": [1]}))
    assert dataset.write_bytes(date(2025, 1, 6), b"{}")[0]["status"] == "written"
    assert dataset.read_bytes(date(2025, 1, 6)) == b"{}"


BAR_SCHEMA = {
    "fields": [
        {"name": "date", "type": "date32"},
        {"name": "ticker", "type": "string"},
        {"name": "close", "type": "float64"},
        {"name": "volume", "type": "int64", "nullable": True},
    ]
}


def test_dataset_reconciles_written_frames_to_the_expected_schema(tmp_path):
    dataset = _dataset(tmp_path, expected_schema=BAR_SCHEMA)
    day = date(2025, 1, 6)

    dataset.write_frame(day, pl.DataFrame({"date": [day], "ticker": ["AAA"], "close": [1.5], "volume": [10]}, schema_overrides={"volume": pl.Int32}))
    frame = dataset.read_frame(day)
    sidecar = json.loads((tmp_path / "remote" / dataset.sidecar_key(day)).read_text())

    assert frame.schema["volume"] == pl.Int64
    assert sidecar["expected_schema"]["source"] == "inline" and sidecar["expected_schema"]["policy"] == "compatible"
    with pytest.raises(ValueError):
        dataset.write_frame(date(2025, 1, 7), pl.DataFrame({"date": [date(2025, 1, 7)], "ticker": ["AAA"]}))


def test_dataset_scan_reads_every_partition_in_one_schema_scan(tmp_path):
    loose = _dataset(tmp_path, partition_column="date")
    # An older partition without the volume column, and partitions without a date column of their own.
    loose.write_frame(date(2025, 1, 6), pl.DataFrame({"ticker": ["AAA"], "close": [1.0]}))
    loose.write_frame(date(2025, 1, 7), pl.DataFrame({"ticker": ["AAA"], "close": [2.0], "volume": [5]}))
    typed = loose.model_copy(update={"expected_schema": BAR_SCHEMA})

    frame = typed.scan(date(2025, 1, 6), date(2025, 1, 8)).collect().sort("date")

    assert frame.select("date", "ticker", "close", "volume").rows() == [(date(2025, 1, 6), "AAA", 1.0, None), (date(2025, 1, 7), "AAA", 2.0, 5)]
    assert typed.scan(date(2025, 1, 6), date(2025, 1, 7)).filter(pl.col("close") > 1.5).collect().height == 1
    assert loose.scan(date(2025, 1, 6), date(2025, 1, 7)).collect().height == 2
    assert typed.scan(date(2025, 2, 1), date(2025, 2, 3)).collect_schema().names() == ["date", "ticker", "close", "volume"]


def test_dataset_scan_prefers_local_partitions_and_lists_each_store_once(tmp_path):
    calls = []

    class CountingStore(LocalFileOutput):
        def list_keys(self, prefix=""):
            calls.append(prefix)
            return super().list_keys(prefix)

    remote = CountingStore(path=tmp_path / "remote")
    local = LocalFileOutput(path=tmp_path / "local")
    dataset = DatasetDefinition(
        name="sample-bars", key_template="curated/bars/{year}/{month}/{date}.parquet", store=remote, local_store=local, expected_schema=BAR_SCHEMA
    )
    for day, close in ((date(2025, 1, 6), 1.0), (date(2025, 1, 7), 2.0)):
        dataset.write_frame(day, pl.DataFrame({"date": [day], "ticker": ["AAA"], "close": [close], "volume": [1]}))
    # A local copy that differs from the remote one wins.
    local_only = dataset.model_copy(update={"store": local, "local_store": None})
    local_only.write_frame(
        date(2025, 1, 6), pl.DataFrame({"date": [date(2025, 1, 6)], "ticker": ["AAA"], "close": [9.0], "volume": [1]}), overwrite=True
    )
    (tmp_path / "local" / dataset.key(date(2025, 1, 7))).unlink()

    closes = dataset.scan(date(2025, 1, 6), date(2025, 1, 7)).sort("date").collect()["close"].to_list()

    assert closes == [9.0, 2.0]
    assert calls == ["curated/bars/"]


def test_dataset_resolves_schema_files_by_path_and_package(tmp_path, monkeypatch):
    package = tmp_path / "schemapkg"
    (package / "schemas").mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "schemas" / "bar.json").write_text(json.dumps(BAR_SCHEMA))
    monkeypatch.syspath_prepend(str(tmp_path))

    by_package = _dataset(tmp_path, expected_schema="pkg://schemapkg/schemas/bar.json").resolved_schema()
    by_path = _dataset(tmp_path, expected_schema={"url": str(package / "schemas" / "bar.json"), "format": "arrow-json"}).resolved_schema()

    assert by_package.schema.names == ["date", "ticker", "close", "volume"]
    assert by_package.schema == by_path.schema
    assert _dataset(tmp_path).resolved_schema() is None


def test_standard_types_replace_64_bit_offset_arrow_types():
    import pyarrow as pa

    from ccflow_etl.models.dataset import _standard_types

    table = pa.table(
        {
            "name": pa.array(["a"], pa.large_string()),
            "blob": pa.array([b"x"], pa.large_binary()),
            "tags": pa.array([["t"]], pa.large_list(pa.large_string())),
            "pairs": pa.array([["p"]], pa.list_(pa.large_string())),
            "info": pa.array([{"k": "v"}], pa.struct([("k", pa.large_string())])),
        }
    )

    standard = _standard_types(table)

    assert standard.schema.types == [pa.string(), pa.binary(), pa.list_(pa.string()), pa.list_(pa.string()), pa.struct([("k", pa.string())])]
    assert _standard_types(standard) is standard


def test_dataset_scan_reads_non_parquet_partitions_from_stores_without_listing(tmp_path):
    class PlainStore:
        """A store with only exists, read, and write: no listing and no fsspec URLs."""

        def __init__(self):
            self.objects = {}

        def artifact_uri(self, key):
            return f"memory://{key}"

        def exists(self, key):
            return key in self.objects

        def read(self, key):
            return self.objects[key]

        def write(self, key, payload, media_type=None, metadata=None):
            self.objects[key] = payload
            return {"status": "written"}

    store = PlainStore()
    dataset = DatasetDefinition(name="events", key_template="raw/events/{date}.jsonl", store=store, format="jsonl")
    dataset.write_bytes(date(2025, 1, 6), b'{"id": 1}\n{"id": 2}\n')
    dataset.write_bytes(date(2025, 1, 8), b'{"id": 3, "extra": "x"}\n')

    frame = dataset.scan(date(2025, 1, 6), date(2025, 1, 8)).collect().sort("id")

    assert frame.select("id", "date").rows() == [(1, date(2025, 1, 6)), (2, date(2025, 1, 6)), (3, date(2025, 1, 8))]
    assert frame["extra"].to_list() == [None, None, "x"]
    assert LocalFileOutput(path=tmp_path).storage_options() == {} and LocalFileOutput(path=tmp_path).polars_storage_options() == {}


def test_dataset_checks_required_columns_for_nulls_before_applying_the_policy(tmp_path):
    schema = {
        "fields": [
            {"name": "date", "type": "date32", "nullable": False},
            {"name": "ticker", "type": "string", "nullable": False},
            {"name": "note", "type": "string"},
        ]
    }
    dataset = _dataset(tmp_path, expected_schema=schema)
    day = date(2025, 1, 6)

    # Polars declares every column nullable; required columns without nulls are accepted, and all-null optional columns are cast.
    dataset.write_frame(day, pl.DataFrame({"date": [day], "ticker": ["AAA"], "note": [None]}))
    assert dataset.read_frame(day).schema["note"] == pl.String
    with pytest.raises(ValueError, match="column 'ticker' has 1 nulls"):
        dataset.write_frame(date(2025, 1, 7), pl.DataFrame({"date": [date(2025, 1, 7)] * 2, "ticker": ["AAA", None], "note": ["x", "y"]}))
