import pytest

from ccflow_etl import DatasetDefinition, ETLUnitIdentity, ProviderDefinition


def test_dataset_and_provider_definitions_are_json_serializable():
    dataset = DatasetDefinition(
        name="sample_records",
        schema_name="sample_record",
        schema_version="1",
        partition_keys=["date", "item_id"],
        cadence="1D",
        media_types=["application/json"],
        quality_expectations=["date/item unique"],
        destination_hints={"object_prefix": "sample/records"},
    )
    provider = ProviderDefinition(
        name="example_provider",
        provider_type="http",
        dataset_refs=["/datasets/sample_records"],
        credentials_ref="/credentials/example_provider",
        capabilities=["pagination", "rate_limit_headers"],
        rate_limit={"requests_per_minute": 5},
        request_templates={"sample_records": "/v1/records/{item_id}/{date}"},
    )

    assert dataset.model_dump(mode="json")["partition_keys"] == ["date", "item_id"]
    assert provider.model_dump(mode="json")["dataset_refs"] == ["/datasets/sample_records"]
    assert provider.credentials_ref == "/credentials/example_provider"


def test_etl_unit_identity_key_is_stable_and_metadata_free():
    left = ETLUnitIdentity(
        provider="example_provider",
        dataset="sample_records",
        partition={"item_id": "item-001", "date": "2025-01-02"},
        schema_version="1",
        transform_version="raw",
        destination="object_store",
        metadata={"attempt": 1},
    )
    right = ETLUnitIdentity(
        provider="example_provider",
        dataset="sample_records",
        partition={"date": "2025-01-02", "item_id": "item-001"},
        schema_version="1",
        transform_version="raw",
        destination="object_store",
        metadata={"attempt": 2},
    )
    changed_destination = left.model_copy(update={"destination": "warehouse"})

    assert left.identity_payload() == right.identity_payload()
    assert left.digest() == right.digest()
    assert left.key(prefix="checkpoint").startswith("checkpoint/example_provider/sample_records/schema=1/transform=raw/destination=object_store/")
    assert changed_destination.digest() != left.digest()


def test_etl_unit_identity_requires_provider_and_dataset():
    with pytest.raises(ValueError, match="identity fields must not be empty"):
        ETLUnitIdentity(provider="", dataset="sample_records")
