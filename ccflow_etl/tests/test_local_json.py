import json
from pathlib import Path

import pytest

from ccflow_etl import LocalFileOutput, LocalWriteContext, LocalWriteModel, WriteModel


def test_local_write_model_writes_json_payload_with_generic_write_base(tmp_path):
    output_path = tmp_path / "raw" / "AAA" / "2024-01-03.json"

    model = LocalWriteModel(format="json")
    result = model(LocalWriteContext(path=output_path, payload={"ticker": "AAA", "close": 103.6}))

    assert isinstance(model, WriteModel)
    assert result.status == "written"
    assert result.path == str(output_path)
    assert json.loads(output_path.read_text()) == {"ticker": "AAA", "close": 103.6}
    assert output_path.read_bytes() == b'{"close":103.6,"ticker":"AAA"}'


def test_local_write_model_skips_existing_payload_without_overwrite(tmp_path):
    output_path = tmp_path / "normalized" / "AAA" / "2024-01-03.json"
    output_path.parent.mkdir(parents=True)
    output_path.write_text('{"sentinel": true}\n')

    result = LocalWriteModel(format="json")(LocalWriteContext(path=output_path, payload={"ticker": "AAA"}))

    assert result.status == "exists"
    assert result.path == str(output_path)
    assert output_path.read_text() == '{"sentinel": true}\n'


def test_local_write_model_preserves_existing_payload_when_replace_fails(tmp_path, monkeypatch):
    output_path = tmp_path / "normalized" / "AAA" / "2024-01-03.json"
    output_path.parent.mkdir(parents=True)
    output_path.write_text('{"sentinel": true}\n')

    original_replace = Path.replace

    def fail_target_replace(self, target):
        if Path(target) == output_path:
            raise OSError("simulated replace failure")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", fail_target_replace)

    with pytest.raises(OSError, match="simulated replace failure"):
        LocalWriteModel(format="json")(LocalWriteContext(path=output_path, payload={"ticker": "AAA"}, overwrite=True))

    assert output_path.read_text() == '{"sentinel": true}\n'


def test_local_write_model_supports_non_json_formats(tmp_path):
    output_path = tmp_path / "notes.txt"

    result = LocalWriteModel(format="text")(LocalWriteContext(path=output_path, payload="hello"))

    assert result.status == "written"
    assert output_path.read_text() == "hello"


def test_local_file_output_implements_artifact_store_protocol(tmp_path):
    output = LocalFileOutput(path=tmp_path, prefix="extracts")

    result = output.write("sample/output.json", b'{"ok":true}', media_type="application/json")

    assert result["status"] == "written"
    assert output.exists("sample/output.json") is True
    assert output.read("sample/output.json") == b'{"ok":true}'
    assert output.artifact_uri("sample/output.json").startswith("file://")
    assert (tmp_path / "extracts" / "sample" / "output.json").read_text() == '{"ok":true}'
    assert output.list_keys("sample") == ["sample/output.json"]
    assert output.list_keys("missing") == []
