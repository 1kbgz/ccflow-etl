from typing import Any, Dict, List, Optional

from ccflow import BaseModel
from pydantic import Field

__all__ = (
    "DatasetDefinition",
    "ProviderDefinition",
)


class DatasetDefinition(BaseModel):
    name: str
    description: Optional[str] = None
    schema_name: Optional[str] = None
    schema_version: Optional[str] = None
    partition_keys: List[str] = Field(default_factory=list)
    cadence: Optional[str] = None
    media_types: List[str] = Field(default_factory=list)
    quality_expectations: List[str] = Field(default_factory=list)
    destination_hints: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProviderDefinition(BaseModel):
    name: str
    description: Optional[str] = None
    provider_type: Optional[str] = None
    dataset_refs: List[str] = Field(default_factory=list)
    credentials_ref: Optional[str] = None
    capabilities: List[str] = Field(default_factory=list)
    rate_limit: Dict[str, Any] = Field(default_factory=dict)
    retry: Dict[str, Any] = Field(default_factory=dict)
    request_templates: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
