from pathlib import Path

import pytest

from data_bridge import api as api_module
from tests.helpers import DummyPipeline, DummyUploader, write_config


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    return write_config(tmp_path)


def test_run_from_file_returns_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config_file: Path) -> None:
    output_dir = tmp_path / "staging" / "output"
    output_dir.mkdir(parents=True)
    artifact_path = output_dir / "shard-00000.tar"
    artifact_path.write_bytes(b"payload")

    pipeline = DummyPipeline([artifact_path])
    monkeypatch.setattr(api_module, "PipelineFactory", type("Factory", (), {"create": staticmethod(lambda _: pipeline)}))
    monkeypatch.setattr(api_module, "R2Uploader", DummyUploader)

    manifest_uri = api_module.run_from_file(config_file, prefix="unit/test")

    assert manifest_uri == "s3://bucket/unit/test/manifest.json"
    assert not artifact_path.exists()
