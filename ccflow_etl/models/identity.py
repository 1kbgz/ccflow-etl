import hashlib
import json
from typing import Any, Dict, Optional

from ccflow import BaseModel
from pydantic import Field, field_validator

__all__ = ("ETLUnitIdentity",)


class ETLUnitIdentity(BaseModel):
    provider: str
    dataset: str
    partition: Dict[str, str] = Field(default_factory=dict)
    transform_version: Optional[str] = None
    destination: Optional[str] = None
    schema_version: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider", "dataset")
    @classmethod
    def _validate_non_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("identity fields must not be empty")
        return value

    def identity_payload(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "dataset": self.dataset,
            "partition": dict(sorted(self.partition.items())),
            "transform_version": self.transform_version,
            "destination": self.destination,
            "schema_version": self.schema_version,
        }

    def digest(self) -> str:
        payload = json.dumps(self.identity_payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def key(self, prefix: Optional[str] = None) -> str:
        parts = [self.provider, self.dataset]
        if self.schema_version:
            parts.append(f"schema={self.schema_version}")
        if self.transform_version:
            parts.append(f"transform={self.transform_version}")
        if self.destination:
            parts.append(f"destination={self.destination}")
        parts.append(self.digest()[:16])
        key = "/".join(parts)
        return f"{prefix.rstrip('/')}/{key}" if prefix else key
