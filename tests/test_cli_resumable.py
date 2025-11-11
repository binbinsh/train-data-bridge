from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from data_bridge import cli
from data_bridge import api as api_module
from tests.helpers import DummyPipeline, DummyUploader, write_config

@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    return write_config(tmp_path)


def test_cli_run_uploads_and_cleans(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config_file: Path) -> None:
    output_dir = tmp_path / "staging" / "output"
    output_dir.mkdir(parents=True)
    artifact_path = output_dir / "shard-00000.tar"
    artifact_path.write_bytes(b"test")
    prefix_value = "local/test-prefix"
    manifest_stub = Path(prefix_value.replace("/", "_")).with_suffix(".json")

    pipeline = DummyPipeline([artifact_path])

    monkeypatch.setattr(api_module, "PipelineFactory", type("Factory", (), {"create": staticmethod(lambda _: pipeline)}))
    monkeypatch.setattr(api_module, "R2Uploader", DummyUploader)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["run", f"--config={config_file}", f"--prefix={prefix_value}"])

    assert result.exit_code == 0, result.output
    assert not artifact_path.exists()
    assert pipeline.marked and pipeline.marked[-1] == [artifact_path]
    assert manifest_stub.exists()
    manifest_stub.unlink()
