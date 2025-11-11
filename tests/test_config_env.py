from __future__ import annotations

from pathlib import Path

import pytest

from data_bridge.config import Settings, load_settings


def write_env_config(tmp_path: Path) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(
        """
vast:
  api_key: ${VAST_API_KEY}
  offer_id: 123
  image: docker.io/library/pytorch:2.3.0
  storage_gb: 10
r2:
  account_id: ${R2_ACCOUNT}
  access_key_id: key
  secret_access_key: secret
  bucket: data
pipeline:
  name: webdataset
  input_uri: hf://dataset
  input_format: huggingface
  samples_per_shard: 2
  staging_dir: ${STAGING_PATH}
"""
    )
    return path


def test_load_settings_expands_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = write_env_config(tmp_path)
    monkeypatch.setenv("VAST_API_KEY", "abc123")
    monkeypatch.setenv("R2_ACCOUNT", "acct-42")
    monkeypatch.setenv("STAGING_PATH", str(tmp_path / "stage"))

    settings = load_settings(config_path)
    assert isinstance(settings, Settings)
    assert settings.vast.api_key == "abc123"
    assert settings.r2.account_id == "acct-42"
    assert settings.pipeline.staging_dir.endswith("stage")


def test_missing_environment_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = write_env_config(tmp_path)
    monkeypatch.delenv("VAST_API_KEY", raising=False)
    monkeypatch.setenv("R2_ACCOUNT", "acct-42")
    monkeypatch.setenv("STAGING_PATH", str(tmp_path / "stage"))

    with pytest.raises(ValueError):
        load_settings(config_path)
