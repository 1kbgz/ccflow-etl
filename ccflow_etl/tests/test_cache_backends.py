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
    put_result = put_model(CachePutContext(key="massive/stocks/raw/2024-01-03/AAA", payload={"ticker": "AAA"}, dataset="stocks", stage="extract"))
    second_put = put_model(CachePutContext(key="massive/stocks/raw/2024-01-03/AAA", payload={"ticker": "BBB"}, dataset="stocks", stage="extract"))
    get_result = get_model(CacheGetContext(key="massive/stocks/raw/2024-01-03/AAA", dataset="stocks", stage="extract"))

    assert put_result.status == "written"
    assert second_put.status == "exists"
    assert get_result.status == "hit"
    assert get_result.payload == {"ticker": "AAA"}
    assert store.values["massive/stocks/raw/2024-01-03/AAA.json"] == b'{"ticker":"AAA"}'
    assert store.content_types["massive/stocks/raw/2024-01-03/AAA.json"] == "application/json"
    assert put_result.artifact == ETLArtifact(
        key="massive/stocks/raw/2024-01-03/AAA",
        dataset="stocks",
        stage="extract",
        uri="memory://massive/stocks/raw/2024-01-03/AAA.json",
        media_type="application/json",
        status="written",
    )


def test_cache_format_csv_encodes_and_decodes_rows_without_new_models():
    store = InMemoryCacheStore()
    rows = [{"ticker": "AAA", "volume": 10}, {"ticker": "BBB", "volume": 20}]

    put_result = CachePutModel(store=store, format="csv")(CachePutContext(key="daily/bars", payload=rows, dataset="stocks", stage="load"))
    get_result = CacheGetModel(store=store, format="csv")(CacheGetContext(key="daily/bars", dataset="stocks", stage="load"))

    assert put_result.cache_key == "daily/bars.csv"
    assert put_result.artifact.media_type == "text/csv; charset=utf-8"
    assert get_result.payload == [{"ticker": "AAA", "volume": "10"}, {"ticker": "BBB", "volume": "20"}]


def test_cache_format_parquet_uses_pyarrow_without_pandas():
    import pyarrow as pa

    store = InMemoryCacheStore()
    rows = [{"ticker": "AAA", "volume": 10}, {"ticker": "BBB", "volume": 20}]

    put_result = CachePutModel(store=store, format="parquet")(CachePutContext(key="daily/bars", payload=rows, dataset="stocks", stage="load"))
    get_result = CacheGetModel(store=store, format="parquet")(CacheGetContext(key="daily/bars", dataset="stocks", stage="load"))

    assert put_result.cache_key == "daily/bars.parquet"
    assert put_result.artifact.media_type == "application/vnd.apache.parquet"
    assert store.content_types["daily/bars.parquet"] == "application/vnd.apache.parquet"
    assert isinstance(get_result.payload, pa.Table)
    assert get_result.payload.to_pylist() == rows
