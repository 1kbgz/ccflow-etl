from datetime import date

import polars as pl
import pytest

from ccflow_etl import DatasetDefinition, FsspecArtifactStore

pytest.importorskip("fsspec")


def test_fsspec_store_reads_writes_and_lists_memory_filesystem(tmp_path):
    store = FsspecArtifactStore(url="memory://ccflow-etl-test/root")

    assert store.write("a/one.json", b"{}")["status"] == "written"
    source = tmp_path / "two.bin"
    source.write_bytes(b"22")
    assert store.write_file("a/b/two.bin", source)["size"] == 2

    assert store.exists("a/one.json")
    assert not store.exists("a/missing.json")
    assert store.read("a/b/two.bin") == b"22"
    assert store.list_keys() == ["a/b/two.bin", "a/one.json"]
    assert store.list_keys("a/b") == ["a/b/two.bin"]
    assert store.list_keys("missing") == []
    assert store.fsspec_url("/a/one.json") == "memory://ccflow-etl-test/root/a/one.json"
    assert store.read_file("a/one.json", tmp_path / "out" / "one.json")["size"] == 2


def test_fsspec_store_translates_s3_options_for_polars():
    store = FsspecArtifactStore(
        url="s3://bucket/prefix",
        options={
            "key": "id",
            "secret": "secret",
            "token": "token",
            "client_kwargs": {"endpoint_url": "https://s3.example", "region_name": "us-west-1"},
        },
    )

    assert store.polars_storage_options() == {
        "aws_access_key_id": "id",
        "aws_secret_access_key": "secret",
        "aws_session_token": "token",
        "aws_endpoint_url": "https://s3.example",
        "aws_region": "us-west-1",
    }
    assert FsspecArtifactStore(url="s3://bucket", options={"anon": True}).polars_storage_options() == {"aws_skip_signature": "true"}
    assert FsspecArtifactStore(url="s3://bucket", polars_options={"aws_region": "x"}).polars_storage_options() == {"aws_region": "x"}
    assert FsspecArtifactStore(url="gs://bucket", options={"token": "t"}).polars_storage_options() == {}
    assert store.storage_options()["key"] == "id"


def test_dataset_scans_partitions_from_fsspec_store(tmp_path):
    dataset = DatasetDefinition(
        name="sample-bars",
        key_template="curated/bars/{year}/{month}/{date}.parquet",
        store=FsspecArtifactStore(url=f"file://{tmp_path}/remote"),
    )
    dataset.write_frame(date(2025, 1, 6), pl.DataFrame({"ticker": ["AAA"], "close": [1.0]}))
    dataset.write_frame(date(2025, 1, 7), pl.DataFrame({"ticker": ["BBB"], "close": [2.0]}))

    frame = dataset.scan(date(2025, 1, 1), date(2025, 1, 8)).collect().sort("date")

    assert frame["ticker"].to_list() == ["AAA", "BBB"]
    assert (tmp_path / "remote" / "curated/bars/2025/01/2025-01-06.parquet").exists()
