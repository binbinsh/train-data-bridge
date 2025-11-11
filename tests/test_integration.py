from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from data_bridge import api as api_module
from data_bridge import cli
from data_bridge.storage import r2 as r2_module
from tests.helpers import write_config


class FakeS3Client:
    def __init__(self, log):
        self.log = log

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.log.append((Path(filename).name, bucket, key))


class FakeBotoSession:
    def __init__(self, log):
        self.log = log

    def client(self, *_args, **_kwargs):
        return FakeS3Client(self.log)


def test_run_pipeline_uploads_with_mocked_r2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact_dir = tmp_path / "staging" / "output"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "shard-00000.tar"
    artifact_path.write_bytes(b"payload")

    upload_log: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        r2_module.boto3.session,
        "Session",
        lambda: FakeBotoSession(upload_log),
    )

    pipeline = type(
        "DummyPipeline",
        (),
        {
            "process": lambda self: [artifact_path],
            "mark_uploaded": lambda self, artifacts: None,
        },
    )()

    monkeypatch.setattr(
        api_module.PipelineFactory,
        "create",
        staticmethod(lambda _settings: pipeline),
    )

    manifest_uri = api_module.run_from_file(write_config(tmp_path), prefix="integration/test")

    assert manifest_uri == "s3://bucket/datasets/integration/test/manifest-integration_test.json"
    assert any(entry[0] == "shard-00000.tar" for entry in upload_log)
    assert not artifact_path.exists()


def test_cli_run_with_use_vast_invokes_remote_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = write_config(tmp_path)
    runner_calls = {}

    class DummyClient:
        pass

    class DummySession:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            runner_calls["closed"] = True

        def provision(self, *_args, **_kwargs):
            runner_calls["provisioned"] = True
            return {"id": 123}

        def wait_for_ready(self):
            runner_calls["ready"] = True

    class DummyRunner:
        def __init__(self, *_):
            runner_calls["runner_init"] = True

        def run(self):
            runner_calls["runner_run"] = True
            return "REMOTE DONE"

    monkeypatch.setattr(cli, "VastClient", lambda *_: DummyClient())
    monkeypatch.setattr(cli, "VastSession", lambda *_: DummySession())
    monkeypatch.setattr(cli, "VastRemoteRunner", DummyRunner)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["run", f"--config={config_path}", "--use-vast"])

    assert result.exit_code == 0
    assert "REMOTE DONE" in result.output
    assert "runner_run" in runner_calls
