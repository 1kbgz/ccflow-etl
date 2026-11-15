from __future__ import annotations

from datetime import date
from hashlib import md5

import polars as pl
import pytest
from ccflow import DateRangeContext

from ccflow_etl import DatasetDefinition, DatasetInventory, DatasetPartitionSources, DatasetPlanModel, LocalFileOutput, WeekdayCalendar


class ETagStore(LocalFileOutput):
    """A local store whose listing reports an MD5 ETag per object, as S3 listings do."""

    def list_objects(self, prefix: str = "") -> dict[str, dict]:
        return {key: {"etag": f'"{md5(self.read(key)).hexdigest()}"'} for key in self.list_keys(prefix)}


def _window(start: date | str, end: date | str) -> DateRangeContext:
    return DateRangeContext(start_date=start, end_date=end)


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

    result = DatasetInventory(dataset=curated, sources=[raw])(_window(DAYS[0], DAYS[-1]))

    assert result.expected_count == 5 and result.present_count == 3
    assert result.missing_ranges == [_window("2025-01-09", "2025-01-10")]
    assert result.stale_ranges == [_window("2025-01-07", "2025-01-07")]
    assert result.stale == [DatasetPartitionSources(date=DAYS[1], sources=[raw.uri(DAYS[1])])]
    assert result.unverified_count == 0 and result.source_missing_count == 0
    assert result.rebuild == [
        _window("2025-01-07", "2025-01-07"),
        _window("2025-01-09", "2025-01-10"),
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

    result = DatasetInventory(dataset=curated, sources=[raw])(_window(DAYS[0], DAYS[0]))

    assert result.stale_count == 1 and result.missing_count == 0


def test_inventory_reports_deleted_sources_without_planning_a_rebuild(stores):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    raw_store.write(raw.key(DAYS[0]), b"close\n1\n")
    _build(raw, curated, DAYS[0])
    raw_store.file_path(raw.key(DAYS[0])).unlink()

    result = DatasetInventory(dataset=curated, sources=[raw])(_window(DAYS[0], DAYS[0]))

    assert result.source_missing == [DatasetPartitionSources(date=DAYS[0], sources=[raw.uri(DAYS[0])])]
    assert result.stale_count == 0 and result.rebuild == []


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

    assert DatasetInventory(dataset=curated, sources=[raw])(_window(DAYS[0], DAYS[1])).unverified_count == 1
    assert DatasetInventory(dataset=curated, sources=[plain])(_window(DAYS[0], DAYS[1])).unverified_count == 2
    assert DatasetInventory(dataset=curated)(_window(DAYS[0], DAYS[1])).unverified_count == 2


def test_inventory_checks_only_the_latest_snapshot_when_asked(stores):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    for value in DAYS:
        raw_store.write(raw.key(value), f"close\n{value.day}\n".encode())
    # Only the newest snapshot and one older one exist, and the older one's source has changed since.
    _build(raw, curated, DAYS[1])
    _build(raw, curated, DAYS[-1])
    raw_store.write(raw.key(DAYS[1]), b"close\n99\n")

    latest = DatasetInventory(dataset=curated, sources=[raw], latest_only=True)

    assert latest.plan(_window(DAYS[0], DAYS[-1])).expected_count == 1
    result = latest(_window(DAYS[0], DAYS[-1]))
    assert (result.expected_count, result.missing_count, result.stale_count, result.rebuild) == (1, 0, 0, [])
    assert latest(_window(DAYS[0], DAYS[-2])).rebuild == [_window("2025-01-09", "2025-01-09")]


def test_inventory_plans_partitions_without_required_sidecars(stores, tmp_path):
    raw_store, curated_store = stores
    raw = _raw(raw_store)
    curated = _curated(curated_store, local_store=LocalFileOutput(path=tmp_path / "local"))
    for value in DAYS[:3]:
        raw_store.write(raw.key(value), b"close\n1\n")
        _build(raw, curated, value)
    # The first day keeps only its local sidecar; the second has none anywhere.
    curated_store.file_path(curated.sidecar_key(DAYS[0])).unlink()
    curated_store.file_path(curated.sidecar_key(DAYS[1])).unlink()
    (tmp_path / "local" / curated.sidecar_key(DAYS[1])).unlink()

    required = DatasetInventory(dataset=curated, require_sidecars=True)(_window(DAYS[0], DAYS[2]))

    assert required.sidecar_missing_count == 1 and required.sidecar_missing_ranges == [_window("2025-01-07", "2025-01-07")]
    assert required.rebuild == [_window("2025-01-07", "2025-01-07")]
    assert DatasetInventory(dataset=curated)(_window(DAYS[0], DAYS[2])).sidecar_missing_count == 0
    with pytest.raises(RuntimeError, match="bars: 0 missing, 1 without sidecars, 0 stale"):
        DatasetPlanModel(datasets=[DatasetInventory(dataset=curated, require_sidecars=True)], fail_on_missing=True)(
            {"start_date": DAYS[0], "end_date": DAYS[2]}
        )


def test_plan_model_explains_without_reading_and_fails_when_asked(stores):
    raw_store, curated_store = stores
    raw, curated = _raw(raw_store), _curated(curated_store)
    inventory = DatasetInventory(dataset=curated, sources=[raw])
    context = _window(DAYS[0], DAYS[-1])

    explained = DatasetPlanModel(datasets=[inventory], explain=True)(context)
    planned = DatasetPlanModel(datasets=[inventory])(context)

    assert explained.status == "planned" and explained.explained[0].expected_count == 5 and explained.datasets == []
    assert DatasetPlanModel(datasets=[inventory]).__deps__(context) == [(inventory, [context])]
    assert DatasetPlanModel(datasets=[inventory], explain=True).__deps__(context) == []
    assert planned.status == "planned" and planned.datasets[0].rebuild == [_window("2025-01-06", "2025-01-10")]
    with pytest.raises(RuntimeError, match="bars: 5 missing, 0 without sidecars, 0 stale"):
        DatasetPlanModel(datasets=[inventory], fail_on_missing=True)(context)
    assert DatasetPlanModel(datasets=[inventory], fail_on_stale=True)(context).status == "planned"
