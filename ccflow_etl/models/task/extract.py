from json import dumps
from typing import Any, Dict, List, Optional, Type

from ccflow import CallableModel, ContextBase, ContextType, Flow, GenericResult, ResultType
from pydantic import Field

from ..formats import PayloadCodec
from ..output import ArtifactWriteContext, ArtifactWriteModel

__all__ = ("ExtractTaskModel",)


class ExtractTaskModel(CallableModel):
    dataset: Any
    output: Optional[Any] = None
    transforms: Any = Field(default_factory=list)
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return self.dataset.context_type if hasattr(self.dataset, "context_type") else ContextBase

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    def _dataset_for_mode(self) -> Any:
        model_fields = getattr(type(self.dataset), "model_fields", {})
        if "explain" in model_fields:
            return self.dataset.model_copy(update={"explain": self.explain or bool(getattr(self.dataset, "explain", False))})
        return self.dataset

    def _return_type(self) -> str:
        return str(getattr(self.dataset, "return_type", None) or getattr(getattr(self.dataset, "dataset", None), "return_type", None) or "json")

    def _codec(self, return_type: str) -> PayloadCodec:
        return PayloadCodec(format=return_type)

    def _payload_bytes(self, payload: Any, return_type: str) -> bytes:
        if isinstance(payload, bytes):
            return payload
        if return_type == "json":
            return dumps(payload, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")
        return self._codec(return_type).encode(payload)

    def _result_payloads(self, payload: Dict[str, Any], keys: List[str]) -> List[Any]:
        if "payloads" in payload:
            return list(payload["payloads"])
        if "results" in payload:
            return list(payload["results"])
        return [{} for _ in keys]

    def _write_outputs(self, payload: Dict[str, Any], *, dry_run: bool) -> List[Any]:
        if self.output is None:
            return []
        return_type = self._return_type()
        keys = list(payload.get("output_keys") or [])
        payloads = self._result_payloads(payload, keys)
        writer = ArtifactWriteModel(store=self.output)
        codec = self._codec(return_type)
        return [
            writer(
                ArtifactWriteContext(
                    key=key,
                    payload=b"" if dry_run else self._payload_bytes(payload_item, return_type),
                    media_type=codec.media_type,
                    dataset=payload.get("dataset"),
                    stage="extract",
                    dry_run=dry_run,
                )
            )
            for key, payload_item in zip(keys, payloads)
        ]

    @Flow.call
    def __call__(self, context: ContextBase) -> GenericResult:
        dataset = self._dataset_for_mode()
        dataset_result = dataset(context)
        payload = dict(dataset_result.value if isinstance(dataset_result.value, dict) else {"value": dataset_result.value})
        output_results = self._write_outputs(payload, dry_run=self.explain)
        return GenericResult(
            value={
                **payload,
                "transforms": self.transforms.model_dump(mode="json") if hasattr(self.transforms, "model_dump") else self.transforms,
                "output_writes": [result.model_dump(mode="json") for result in output_results],
            }
        )
