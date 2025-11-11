from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from data_bridge.storage import r2 as r2_module


class DummyPipeline:
    def __init__(self, artifacts: List[Path]) -> None:
        self._artifacts = artifacts
        self.marked: List[List[Path]] = []

    def process(self):
        for artifact in self._artifacts:
            yield artifact

    def mark_uploaded(self, artifacts: List[Path]) -> None:
        self.marked.append(list(artifacts))


class DummyUploader:
    def __init__(self, settings) -> None:  # noqa: D401 - mimic boto client signature
        self.settings = settings
        self.uploaded: List[Path] = []

    def upload_files(self, artifacts: List[Path], prefix: str) -> List[r2_module.Artifact]:
        self.uploaded.extend(artifacts)
        return [
            r2_module.Artifact(path=artifact, key=f"{prefix}/{artifact.name}", size=artifact.stat().st_size, sha256="hash")
            for artifact in artifacts
        ]

    def write_manifest(self, artifacts: List[r2_module.Artifact], prefix: str) -> r2_module.Artifact:
        manifest_path = Path(prefix.replace("/", "_")).with_suffix(".json")
        manifest_path.write_text("{}")
        return r2_module.Artifact(path=manifest_path, key=f"{prefix}/manifest.json", size=manifest_path.stat().st_size, sha256="hash")


def write_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        yaml.safe_dump(
            {
                "vast": {
                    "api_key": "dummy",
                    "offer_id": 0,
                    "image": "docker.io/library/python:3.11",
                    "storage_gb": 1,
                    "repo_url": "https://example.com/unused.git",
                },
                "r2": {
                    "account_id": "acct",
                    "access_key_id": "key",
                    "secret_access_key": "secret",
                    "bucket": "bucket",
                    "region": "auto",
                },
                "pipeline": {
                    "name": "webdataset",
                    "input_uri": "file://data.jsonl",
                    "input_format": "jsonl",
                    "samples_per_shard": 2,
                    "staging_dir": str(tmp_path / "staging"),
                },
            },
            sort_keys=False,
        )
    )
    return cfg
