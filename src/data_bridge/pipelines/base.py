from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List

from data_bridge.config import PipelineSettings
from data_bridge.io.sources import SampleStream
from data_bridge.utils.progress import ProgressTracker


@dataclass
class PipelineContext:
    settings: PipelineSettings
    staging_dir: Path
    output_dir: Path


class Pipeline(ABC):
    def __init__(self, settings: PipelineSettings) -> None:
        self.settings = settings
        staging = Path(settings.staging_dir)
        self.ctx = PipelineContext(
            settings=settings,
            staging_dir=staging,
            output_dir=staging / "output",
        )
        self.ctx.staging_dir.mkdir(parents=True, exist_ok=True)
        self.ctx.output_dir.mkdir(parents=True, exist_ok=True)
        self.progress = ProgressTracker(self.ctx.staging_dir / f"{settings.name}-progress.json")

    @abstractmethod
    def process(self) -> Iterator[Path]:
        """Yield artifacts as soon as shard/chunk writers finish them."""

    def stream(self) -> SampleStream:
        return SampleStream(self.settings, start_index=self.progress.next_index)

    def pending_artifacts(self) -> List[Path]:
        return self.progress.pending_artifacts(self.ctx.output_dir)

    def mark_uploaded(self, artifacts: Iterable[Path]) -> None:
        names = [artifact.name for artifact in artifacts]
        if names:
            self.progress.mark_uploaded(names)
            self.progress.flush()

    def cleanup_untracked_outputs(self) -> None:
        recorded = set(self.progress.completed_outputs)
        for path in self.ctx.output_dir.iterdir():
            if path.is_file() and path.name not in recorded:
                path.unlink(missing_ok=True)


class PipelineFactory:
    @staticmethod
    def create(settings: PipelineSettings) -> Pipeline:
        if settings.name == "webdataset":
            from data_bridge.pipelines.webdataset import WebdatasetPipeline

            return WebdatasetPipeline(settings)
        if settings.name == "litdata":
            from data_bridge.pipelines.litdata import LitDataPipeline

            return LitDataPipeline(settings)
        raise ValueError(f"Unknown pipeline: {settings.name}")
