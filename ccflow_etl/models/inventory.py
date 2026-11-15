import json
from datetime import date
from typing import Any

from ccflow import BaseModel, CallableModel, DateRangeContext, Flow, GenericResult
from pydantic import Field

from .dataset import DatasetDefinition, _resolve
from .output.artifact import _artifact_read, _artifact_uri

__all__ = (
    "DatasetInventory",
    "DatasetPlanModel",
)


def _prefix(dataset: DatasetDefinition) -> str:
    return dataset.key_template.split("{", 1)[0]


def _listing(store: Any, prefix: str) -> dict[str, dict[str, Any]]:
    """Every key under ``prefix`` with the identity the listing reports (ETag, size), from one listing."""
    list_objects = getattr(store, "list_objects", None)
    if list_objects is not None:
        return list_objects(prefix)
    return {key: {} for key in store.list_keys(prefix)}


def _recorded_sources(value: Any) -> list[dict[str, Any]]:
    """Every mapping in a sidecar that records a source object by ``uri`` and ``etag``."""
    if isinstance(value, dict):
        found = [value] if value.get("uri") and value.get("etag") else []
        return found + [source for item in value.values() for source in _recorded_sources(item)]
    if isinstance(value, list):
        return [source for item in value for source in _recorded_sources(item)]
    return []


def _ranges(dates: list[date], selected: set[date]) -> list[list[str]]:
    """Runs of ``selected`` dates that are consecutive in ``dates``, as ``[start, end]`` ISO pairs."""
    ranges: list[list[str]] = []
    current: list[date] | None = None
    for value in dates:
        if value in selected:
            current = [value, value] if current is None else [current[0], value]
        elif current is not None:
            ranges.append([current[0].isoformat(), current[1].isoformat()])
            current = None
    if current is not None:
        ranges.append([current[0].isoformat(), current[1].isoformat()])
    return ranges


class DatasetInventory(BaseModel):
    """Which of a dataset's expected partitions are missing, and which are stale because a source changed after they were built.

    A partition is stale when its lineage sidecar records a source object (any mapping with ``uri`` and ``etag``)
    whose URI is a partition of one of ``sources`` and whose ETag differs from that object's current ETag. A recorded
    source that no longer exists is reported as missing rather than stale, since rebuilding would need it. Present
    partitions with no comparable source are counted as unverified. With ``require_sidecars``, a present partition
    without its sidecar is reported as sidecar-missing and planned for rebuild.

    Each store is listed once per prefix. Sidecars are read from ``local_store`` where it holds them and from
    ``store`` otherwise, and only when ``sources`` is set. Current ETags come from the source store's
    ``list_objects`` listing; a store that only offers ``list_keys`` cannot show staleness.
    """

    dataset: DatasetDefinition
    sources: list[DatasetDefinition] = Field(default_factory=list, description="Datasets whose objects the sidecars record as sources.")
    latest_only: bool = Field(
        default=False,
        description="Check only the last expected partition in the range, for datasets whose partitions are snapshots that supersede earlier ones.",
    )
    require_sidecars: bool = Field(default=False, description="Treat a present partition without its lineage sidecar as needing a rebuild.")

    def _dates(self, start: date, end: date) -> list[date]:
        dates = self.dataset.partitions(start, end)
        return dates[-1:] if self.latest_only else dates

    def plan(self, start: date, end: date) -> dict[str, Any]:
        """Explain record: the expected partitions and the listings an inventory would make, without reading anything."""
        return {
            "name": self.dataset.name,
            "expected_count": len(self._dates(start, end)),
            "prefix": _artifact_uri(_resolve(self.dataset.store), _prefix(self.dataset)),
            "sources": [_artifact_uri(_resolve(source.store), _prefix(source)) for source in self.sources],
        }

    def _current_sources(self) -> tuple[dict[str, dict[str, Any]], list[str]]:
        """Current identity by URI for every listed source object, and the URI prefix of each source."""
        current: dict[str, dict[str, Any]] = {}
        prefixes = []
        for source in self.sources:
            store, prefix = _resolve(source.store), _prefix(source)
            prefixes.append(_artifact_uri(store, prefix))
            current.update({_artifact_uri(store, key): identity for key, identity in _listing(store, prefix).items()})
        return current, prefixes

    def _sidecar(self, value: date, stored: set[str], local_keys: set[str]) -> dict[str, Any]:
        key = self.dataset.sidecar_key(value)
        if key in local_keys:
            return json.loads(_artifact_read(_resolve(self.dataset.local_store), key))
        if key in stored:
            return json.loads(_artifact_read(_resolve(self.dataset.store), key))
        return {}

    def inventory(self, start: date, end: date) -> dict[str, Any]:
        dataset = self.dataset
        dates = self._dates(start, end)
        prefix = _prefix(dataset)
        stored = set(_listing(_resolve(dataset.store), prefix))
        local_keys = set(_listing(_resolve(dataset.local_store), prefix)) if dataset.local_store is not None else set()
        missing = {value for value in dates if dataset.key(value) not in stored}
        present = [value for value in dates if value not in missing]
        sidecars = stored | local_keys
        sidecar_missing = {value for value in present if dataset.sidecar_key(value) not in sidecars} if self.require_sidecars else set()

        stale: dict[date, list[str]] = {}
        source_missing: dict[date, list[str]] = {}
        unverified = 0
        if self.sources:
            current, source_prefixes = self._current_sources()
            for value in present:
                sidecar = self._sidecar(value, stored, local_keys) if dataset.sidecar else {}
                tracked = [source for source in _recorded_sources(sidecar) if source["uri"].startswith(tuple(source_prefixes))]
                gone = [source["uri"] for source in tracked if source["uri"] not in current]
                # A listing without ETags (a store with only list_keys) cannot show whether a source changed.
                comparable = [source for source in tracked if current.get(source["uri"], {}).get("etag")]
                changed = [source["uri"] for source in comparable if current[source["uri"]]["etag"] != source["etag"]]
                if gone:
                    source_missing[value] = gone
                if changed:
                    stale[value] = changed
                elif not comparable and not gone:
                    unverified += 1
        else:
            unverified = len(present)

        rebuild = missing | sidecar_missing | set(stale)
        return {
            "name": dataset.name,
            "expected_count": len(dates),
            "present_count": len(present),
            "missing_count": len(missing),
            "missing_ranges": _ranges(dates, missing),
            "sidecar_missing_count": len(sidecar_missing),
            "sidecar_missing_ranges": _ranges(dates, sidecar_missing),
            "stale_count": len(stale),
            "stale_ranges": _ranges(dates, set(stale)),
            "stale": [{"date": value.isoformat(), "sources": stale[value]} for value in dates if value in stale],
            "source_missing_count": len(source_missing),
            "source_missing": [{"date": value.isoformat(), "sources": source_missing[value]} for value in dates if value in source_missing],
            "unverified_count": unverified,
            "rebuild": [{"start_date": first, "end_date": last} for first, last in _ranges(dates, rebuild)],
        }


class DatasetPlanModel(CallableModel):
    """Plan which partitions of each dataset need building: missing ones, ones missing a required sidecar, and ones whose sources changed.

    The result lists, per dataset, missing and stale date ranges and ``rebuild`` contexts (``start_date`` and
    ``end_date``) covering both, for the dataset's producing task to run. Nothing is built or written. Explain mode
    reports the expected partitions without listing or reading any store.
    """

    datasets: list[DatasetInventory] = Field(default_factory=list)
    explain: bool = False
    fail_on_missing: bool = False
    fail_on_stale: bool = False
    max_listed: int = Field(default=20, ge=0, description="Stale and source-missing partitions listed per dataset.")

    @property
    def context_type(self) -> type[DateRangeContext]:
        return DateRangeContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    @Flow.call
    def __call__(self, context: DateRangeContext) -> GenericResult:
        window = {"start_date": context.start_date.isoformat(), "end_date": context.end_date.isoformat()}
        if self.explain:
            return GenericResult(
                value={**window, "status": "planned", "datasets": [item.plan(context.start_date, context.end_date) for item in self.datasets]}
            )
        results = []
        for item in self.datasets:
            result = item.inventory(context.start_date, context.end_date)
            result["stale"] = result["stale"][: self.max_listed]
            result["source_missing"] = result["source_missing"][: self.max_listed]
            results.append(result)
        failures = [
            f"{result['name']}: {result['missing_count']} missing, {result['sidecar_missing_count']} without sidecars, {result['stale_count']} stale"
            for result in results
            if (self.fail_on_missing and (result["missing_count"] or result["sidecar_missing_count"]))
            or (self.fail_on_stale and result["stale_count"])
        ]
        if failures:
            raise RuntimeError(f"dataset plan {window['start_date']} to {window['end_date']} found partitions to rebuild: {'; '.join(failures)}")
        return GenericResult(
            value={**window, "status": "planned" if any(result["rebuild"] for result in results) else "current", "datasets": results}
        )
