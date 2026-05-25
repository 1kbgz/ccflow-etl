from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Type

from ccflow import BaseModel, CallableModel, ContextBase, ContextType, Flow, ResultBase, ResultType
from pydantic import Field

__all__ = (
    "CheckpointDecision",
    "CheckpointDecisionContext",
    "CheckpointDecisionModel",
    "CheckpointDecisionResult",
    "CheckpointDecisionUnit",
    "CheckpointRecord",
    "CheckpointStatus",
)

CheckpointStatus = Literal["planned", "running", "succeeded", "failed", "skipped"]


class CheckpointRecord(BaseModel):
    key: str
    status: CheckpointStatus
    updated_at: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


CheckpointDecisionStatus = Literal["runnable", "checkpoint", "exists"]


class CheckpointDecisionUnit(BaseModel):
    key: str
    output_path: Optional[Path] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CheckpointDecision(BaseModel):
    key: str
    status: CheckpointDecisionStatus
    output_path: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CheckpointDecisionContext(ContextBase):
    units: List[CheckpointDecisionUnit] = Field(default_factory=list)
    checkpoint_store: Optional[Any] = None
    overwrite: bool = False
    skip_existing: bool = True


class CheckpointDecisionResult(ResultBase):
    decisions: List[CheckpointDecision] = Field(default_factory=list)
    runnable_keys: List[str] = Field(default_factory=list)


class CheckpointDecisionModel(CallableModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return CheckpointDecisionContext

    @property
    def result_type(self) -> Type[ResultType]:
        return CheckpointDecisionResult

    @Flow.call
    def __call__(self, context: CheckpointDecisionContext) -> CheckpointDecisionResult:
        decisions = []
        runnable_keys = []
        for unit in context.units:
            output_path = str(unit.output_path) if unit.output_path is not None else None
            if not context.overwrite and context.checkpoint_store is not None and context.checkpoint_store.should_skip(unit.key):
                decisions.append(CheckpointDecision(key=unit.key, status="checkpoint", output_path=output_path, metadata=unit.metadata))
            elif not context.overwrite and context.skip_existing and unit.output_path is not None and unit.output_path.exists():
                decisions.append(CheckpointDecision(key=unit.key, status="exists", output_path=output_path, metadata=unit.metadata))
            else:
                decisions.append(CheckpointDecision(key=unit.key, status="runnable", output_path=output_path, metadata=unit.metadata))
                runnable_keys.append(unit.key)
        return CheckpointDecisionResult(decisions=decisions, runnable_keys=runnable_keys)
