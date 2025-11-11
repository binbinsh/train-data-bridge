from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import AliasChoices, BaseModel, Field, field_validator

ENV_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


class VastSettings(BaseModel):
    api_key: str = Field(..., description="Vast.ai API key")
    offer_id: int = Field(..., description="Preferred offer ID for GPU rental")
    image: str = Field(..., description="Container image used on the GPU host")
    storage_gb: int = Field(50, description="Ephemeral storage to attach")
    max_price_usd_hour: float = Field(2.5, description="Ceiling price per hour")
    repo_url: Optional[str] = Field(
        default=None,
        description="Git repo cloned on the Vast.ai host when --use-vast is enabled",
    )
    repo_ref: str = Field(
        "main",
        description="Git ref (branch/tag/commit) to checkout on the Vast.ai host",
    )
    workdir: str = Field(
        "/root/train-data-bridge",
        description="Workspace path on the Vast.ai host for cloning and caching",
    )


class R2Settings(BaseModel):
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    region: str = "auto"
    prefix: str = Field("datasets", description="Bucket prefix for processed shards")


class PipelineSettings(BaseModel):
    name: str = Field(..., description="webdataset or litdata")
    input_uri: str
    input_format: str = Field("jsonl", description="jsonl or huggingface")
    samples_per_shard: int = Field(
        2048,
        description="Number of samples per shard/chunk",
        validation_alias=AliasChoices("samples_per_shard", "shards"),
    )
    staging_dir: str = "/tmp/train-data-bridge"
    output_format: Optional[str] = None
    source_options: Dict[str, Any] = Field(default_factory=dict, description="Extra kwargs for the source adapter")

    @field_validator("name")
    @classmethod
    def _lower(cls, value: str) -> str:
        value = value.lower()
        if value not in {"webdataset", "litdata"}:
            raise ValueError("pipeline.name must be either 'webdataset' or 'litdata'")
        return value

    @field_validator("input_format")
    @classmethod
    def _input_format(cls, value: str) -> str:
        value = value.lower()
        if value not in {"jsonl", "huggingface"}:
            raise ValueError("pipeline.input_format must be 'jsonl' or 'huggingface'")
        return value

    @field_validator("samples_per_shard")
    @classmethod
    def _shard_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("samples_per_shard must be positive")
        return value


class Settings(BaseModel):
    vast: VastSettings
    r2: R2Settings
    pipeline: PipelineSettings


def load_settings(path: Path) -> Settings:
    """Load YAML/TOML file into Settings."""
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(path.read_text())
    elif path.suffix == ".toml":
        import tomllib

        data = tomllib.loads(path.read_text())
    else:
        raise ValueError(f"Unsupported config extension: {path.suffix}")
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping")
    expanded = _expand_env_variables(data)
    return Settings(**expanded)


def ensure_config(path: Path, template: Settings) -> None:
    """Write a template config if path is missing."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(template.model_dump(), sort_keys=False))


def _expand_env_variables(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env_variables(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_expand_env_variables(item) for item in value]
    if isinstance(value, str):
        def replacer(match: re.Match) -> str:
            name = match.group(1)
            env_value = os.environ.get(name)
            if env_value is None:
                raise ValueError(f"Environment variable '{name}' referenced in config but not set")
            return env_value

        return ENV_PATTERN.sub(replacer, value)
    return value
