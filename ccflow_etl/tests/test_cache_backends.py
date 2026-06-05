from ccflow_etl import CacheGetContext, CacheGetModel, CachePutContext, CachePutModel, ETLArtifact


class InMemoryCacheStore:
    def __init__(self):
        self.values = {}
        self.content_types = {}

    def exists(self, key):
        return key in self.values

    def put_bytes(self, key, value, content_type=None):
        self.values[key] = value
        self.content_types[key] = content_type
        return {"key": key}

    def get_bytes(self, key):
        return self.values[key]

    def uri(self, key):
        return f"memory://{key}"


def test_cache_put_and_get_formats_json_with_typed_artifacts():
    store = InMemoryCacheStore()

    put_model = CachePutModel(store=store, format="json")
    get_model = CacheGetModel(store=store, format="json")
    key = "example_provider/sample_records/raw/2024-01-03/item-001"
    put_result = put_model(CachePutContext(key=key, payload={"item_id": "item-001"}, dataset="sample_records", stage="extract"))
    second_put = put_model(CachePutContext(key=key, payload={"item_id": "item-002"}, dataset="sample_records", stage="extract"))
    get_result = get_model(CacheGetContext(key=key, dataset="sample_records", stage="extract"))

    assert put_result.status == "written"
    assert second_put.status == "exists"
    assert get_result.status == "hit"
    assert get_result.payload == {"item_id": "item-001"}
    assert store.values["example_provider/sample_records/raw/2024-01-03/item-001.json"] == b'{"item_id":"item-001"}'
    assert store.content_types["example_provider/sample_records/raw/2024-01-03/item-001.json"] == "application/json"
    assert put_result.artifact == ETLArtifact(
        key=key,
        dataset="sample_records",
        stage="extract",
        uri="memory://example_provider/sample_records/raw/2024-01-03/item-001.json",
        media_type="application/json",
        status="written",
    )


def test_cache_format_csv_encodes_and_decodes_rows_without_new_models():
    store = InMemoryCacheStore()
    rows = [{"item_id": "item-001", "quantity": 10}, {"item_id": "item-002", "quantity": 20}]

    put_result = CachePutModel(store=store, format="csv")(CachePutContext(key="sample/records", payload=rows, dataset="sample_records", stage="load"))
    get_result = CacheGetModel(store=store, format="csv")(CacheGetContext(key="sample/records", dataset="sample_records", stage="load"))

    assert put_result.cache_key == "sample/records.csv"
    assert put_result.artifact.media_type == "text/csv; charset=utf-8"
    assert get_result.payload == [{"item_id": "item-001", "quantity": "10"}, {"item_id": "item-002", "quantity": "20"}]


def test_cache_format_parquet_uses_pyarrow_without_pandas():
    import pyarrow as pa

    store = InMemoryCacheStore()
    rows = [{"item_id": "item-001", "quantity": 10}, {"item_id": "item-002", "quantity": 20}]

    put_result = CachePutModel(store=store, format="parquet")(
        CachePutContext(key="sample/records", payload=rows, dataset="sample_records", stage="load")
    )
    get_result = CacheGetModel(store=store, format="parquet")(CacheGetContext(key="sample/records", dataset="sample_records", stage="load"))

    assert put_result.cache_key == "sample/records.parquet"
    assert put_result.artifact.media_type == "application/vnd.apache.parquet"
    assert store.content_types["sample/records.parquet"] == "application/vnd.apache.parquet"
    assert isinstance(get_result.payload, pa.Table)
    assert get_result.payload.to_pylist() == rows
