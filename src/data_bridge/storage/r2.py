from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import boto3

from data_bridge.config import R2Settings


def _hash_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


@dataclass
class Artifact:
    path: Path
    key: str
    size: int
    sha256: str


class R2Uploader:
    def __init__(self, settings: R2Settings) -> None:
        endpoint = f"https://{settings.account_id}.r2.cloudflarestorage.com"
        session = boto3.session.Session()
        self.s3 = session.client(
            "s3",
            region_name=settings.region,
            endpoint_url=endpoint,
            aws_access_key_id=settings.access_key_id,
            aws_secret_access_key=settings.secret_access_key,
        )
        self.settings = settings

    def upload_files(self, files: Iterable[Path], prefix: str) -> List[Artifact]:
        artifacts: List[Artifact] = []
        for path in files:
            key = f"{self.settings.prefix}/{prefix}/{path.name}"
            self.s3.upload_file(str(path), self.settings.bucket, key)
            artifacts.append(Artifact(path=path, key=key, size=path.stat().st_size, sha256=_hash_file(path)))
        return artifacts

    def write_manifest(self, artifacts: List[Artifact], prefix: str) -> Artifact:
        safe_prefix = prefix.replace("/", "_")
        manifest_path = Path(f"manifest-{safe_prefix}.json")
        payload = [
            {"key": art.key, "size": art.size, "sha256": art.sha256, "filename": art.path.name}
            for art in artifacts
        ]
        manifest_path.write_text(json.dumps(payload, indent=2))
        manifest_art = self.upload_files([manifest_path], prefix)[0]
        manifest_path.unlink(missing_ok=True)
        return manifest_art
