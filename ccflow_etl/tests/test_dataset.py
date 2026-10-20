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
