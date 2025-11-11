from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import List, Optional

from data_bridge.config import Settings, load_settings
from data_bridge.pipelines.base import PipelineFactory
from data_bridge.storage.r2 import Artifact, R2Uploader


def run_pipeline(settings: Settings, prefix: Optional[str] = None) -> Optional[str]:
    """Process data according to ``settings`` and upload the shards to R2.

    Args:
        settings: Fully parsed configuration object.
        prefix: Optional override for the R2 object prefix.

    Returns:
        The manifest URI (``s3://bucket/key``) if new artifacts were uploaded, otherwise ``None``.
    """
    pipeline = PipelineFactory.create(settings.pipeline)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    resolved_prefix = prefix or f"{settings.pipeline.name}/{timestamp}"
    uploader = R2Uploader(settings.r2)
    uploaded: List[Artifact] = []

    for artifact_path in pipeline.process():
        result = uploader.upload_files([artifact_path], resolved_prefix)[0]
        uploaded.append(result)
        pipeline.mark_uploaded([artifact_path])
        artifact_path.unlink(missing_ok=True)

    if not uploaded:
        return None

    manifest = uploader.write_manifest(uploaded, resolved_prefix)
    return f"s3://{settings.r2.bucket}/{manifest.key}"


def run_from_file(config_path: Path | str, prefix: Optional[str] = None) -> Optional[str]:
    """Load a YAML/TOML config from disk and execute the pipeline."""
    settings = load_settings(Path(config_path))
    return run_pipeline(settings, prefix=prefix)
