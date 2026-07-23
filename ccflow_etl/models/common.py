from collections import Counter
from collections.abc import Iterable
from typing import Any, Literal

from ccflow import BaseModel
from pydantic import Field

__all__ = (
    "ETLArtifact",
    "ETLStage",
    "RunSummary",
)


ETLStage = Literal["plan", "extract", "transform", "load"]


class ETLArtifact(BaseModel):
    key: str
    stage: ETLStage
    status: str = "planned"
    dataset: str | None = None
    uri: str | None = None
    media_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunSummary(BaseModel):
    total: int = 0
    planned: int = 0
    skipped: int = 0
    succeeded: int = 0
    failed: int = 0
    retried: int = 0
    cancelled: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)
    by_stage: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def from_statuses(cls, statuses: Iterable[str | None], artifacts: Iterable[ETLArtifact | dict] = ()) -> "RunSummary":
        normalized_statuses = [status for status in statuses if status]
        by_status = dict(sorted(Counter(normalized_statuses).items()))
        artifact_stages = [artifact.stage if isinstance(artifact, ETLArtifact) else artifact.get("stage") for artifact in artifacts]
        by_stage = dict(sorted(Counter(stage for stage in artifact_stages if stage).items()))
        return cls(
            total=len(normalized_statuses),
            planned=by_status.get("planned", 0),
            skipped=sum(by_status.get(status, 0) for status in ("database", "exists", "skipped")),
            succeeded=sum(by_status.get(status, 0) for status in ("published", "succeeded", "success", "updated", "upserted", "written")),
            failed=by_status.get("failed", 0),
            retried=by_status.get("retried", 0),
            cancelled=by_status.get("cancelled", 0),
            by_status=by_status,
            by_stage=by_stage,
        )

    @classmethod
    def from_items(cls, items: Iterable[dict], artifacts: Iterable[ETLArtifact | dict] = ()) -> "RunSummary":
        return cls.from_statuses((item.get("status") for item in items), artifacts=artifacts)

    def legacy_counts(self) -> dict[str, int]:
        return {
            "planned": self.planned,
            "skipped": self.skipped,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "retried": self.retried,
            "cancelled": self.cancelled,
        }
