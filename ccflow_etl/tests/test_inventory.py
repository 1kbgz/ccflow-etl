from __future__ import annotations

from datetime import date
from hashlib import md5

import polars as pl
import pytest

from ccflow_etl import DatasetDefinition, DatasetInventory, DatasetPlanModel, LocalFileOutput, WeekdayCalendar


class ETagStore(LocalFileOutput):
    """A local store whose listing reports an MD5 ETag per object, as S3 listings do."""

    def list_objects(self, prefix: str = "") -> dict[str, dict]:
        return {key: {"etag": f'"{md5(self.read(key)).hexdigest()}"'} for key in self.list_keys(prefix)}


DAYS = [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8), date(2025, 1, 9), date(2025, 1, 10)]


@pytest.fixture
def stores(tmp_path):
    return ETagStore(path=tmp_path / "raw"), LocalFileOutput(path=tmp_path / "curated")


def _raw(store) -> DatasetDefinition:
    return DatasetDefinition(name="raw-bars", key_template="raw/bars/{date}.csv", store=store, format="csv", calendar=WeekdayCalendar())


def _curated(store, **kwargs) -> DatasetDefinition:
    return DatasetDefinition(name="bars", key_template="curated/bars/{date}.parquet", store=store, calendar=WeekdayCalendar(), **kwargs)


def _build(raw: DatasetDefinition, curated: DatasetDefinition, value: date, nested: bool = False) -> None:
    """Write a curated partition whose sidecar records the raw object's current identity, as producers do."""
    store = raw.store
    identity = {"uri": raw.uri(value), "etag": store.list_objects(raw.key(value))[raw.key(value)]["etag"]}
    lineage = {"sources": {"bars": {"last": identity}}} if nested else {"source": identity}
    curated.write_frame(value, pl.DataFrame({"close": [1.0]}), lineage=lineage, overwrite=True)


def test_inventory_reports_missing_stale_and_current_partitions(stores):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    for value in DAYS:
        raw_store.write(raw.key(value), f"close\n{value.day}\n".encode())
    for value in DAYS[:3]:
        _build(raw, curated, value)
    # The source for the second day is republished after its partition was built.
    raw_store.write(raw.key(DAYS[1]), b"close\n99\n")

    result = DatasetInventory(dataset=curated, sources=[raw]).inventory(DAYS[0], DAYS[-1])

    assert result["expected_count"] == 5 and result["present_count"] == 3
    assert result["missing_ranges"] == [["2025-01-09", "2025-01-10"]]
    assert result["stale_ranges"] == [["2025-01-07", "2025-01-07"]]
    assert result["stale"] == [{"date": "2025-01-07", "sources": [raw.uri(DAYS[1])]}]
    assert result["unverified_count"] == 0 and result["source_missing_count"] == 0
    assert result["rebuild"] == [
        {"start_date": "2025-01-07", "end_date": "2025-01-07"},
        {"start_date": "2025-01-09", "end_date": "2025-01-10"},
    ]


def test_inventory_finds_nested_sources_and_reads_sidecars_from_local_store(stores, tmp_path):
    raw_store, curated_store = stores
    raw = _raw(raw_store)
    curated = _curated(curated_store, local_store=LocalFileOutput(path=tmp_path / "local"))
    raw_store.write(raw.key(DAYS[0]), b"close\n1\n")
    _build(raw, curated, DAYS[0], nested=True)
    # Only the local copy of the sidecar remains; the inventory reads it rather than the remote one.
    (tmp_path / "curated" / curated.sidecar_key(DAYS[0])).unlink()
    raw_store.write(raw.key(DAYS[0]), b"close\n2\n")

    result = DatasetInventory(dataset=curated, sources=[raw]).inventory(DAYS[0], DAYS[0])

    assert result["stale_count"] == 1 and result["missing_count"] == 0


def test_inventory_reports_deleted_sources_without_planning_a_rebuild(stores):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    raw_store.write(raw.key(DAYS[0]), b"close\n1\n")
    _build(raw, curated, DAYS[0])
    raw_store.file_path(raw.key(DAYS[0])).unlink()

    result = DatasetInventory(dataset=curated, sources=[raw]).inventory(DAYS[0], DAYS[0])

    assert result["source_missing"] == [{"date": "2025-01-06", "sources": [raw.uri(DAYS[0])]}]
    assert result["stale_count"] == 0 and result["rebuild"] == []


def test_inventory_counts_partitions_it_cannot_verify(stores, tmp_path):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    raw_store.write(raw.key(DAYS[0]), b"close\n1\n")
    raw_store.write(raw.key(DAYS[1]), b"close\n2\n")
    _build(raw, curated, DAYS[0])
    # No sidecar for the second day, and a source store without ETags in its listing for the first.
    curated.write_frame(DAYS[1], pl.DataFrame({"close": [2.0]}), overwrite=True)
    curated_store.file_path(curated.sidecar_key(DAYS[1])).unlink()
    plain = _raw(LocalFileOutput(path=tmp_path / "raw"))

    assert DatasetInventory(dataset=curated, sources=[raw]).inventory(DAYS[0], DAYS[1])["unverified_count"] == 1
    assert DatasetInventory(dataset=curated, sources=[plain]).inventory(DAYS[0], DAYS[1])["unverified_count"] == 2
    assert DatasetInventory(dataset=curated).inventory(DAYS[0], DAYS[1])["unverified_count"] == 2


def test_plan_model_explains_without_reading_and_fails_when_asked(stores):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    inventory = DatasetInventory(dataset=curated, sources=[raw])
    context = {"start_date": DAYS[0], "end_date": DAYS[-1]}

    explained = DatasetPlanModel(datasets=[inventory], explain=True)(context).value
    planned = DatasetPlanModel(datasets=[inventory])(context).value

    assert explained["status"] == "planned" and explained["datasets"][0]["expected_count"] == 5
    assert "missing_count" not in explained["datasets"][0]
    assert planned["status"] == "planned" and planned["datasets"][0]["rebuild"] == [{"start_date": "2025-01-06", "end_date": "2025-01-10"}]
    with pytest.raises(RuntimeError, match="bars: 5 missing, 0 stale"):
        DatasetPlanModel(datasets=[inventory], fail_on_missing=True)(context)
    assert DatasetPlanModel(datasets=[inventory], fail_on_stale=True)(context).value["status"] == "planned"
