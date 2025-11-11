from __future__ import annotations

from pathlib import Path
from typing import Iterator

from data_bridge.pipelines.base import Pipeline
from data_bridge.utils.vendors import ensure_vendor

ensure_vendor("litdata")
from litdata.streaming.writer import BinaryWriter  # type: ignore


class LitDataPipeline(Pipeline):
    def process(self) -> Iterator[Path]:
        self.cleanup_untracked_outputs()
        for pending in self.pending_artifacts():
            yield pending

        chunk_size = max(1, self.settings.samples_per_shard)
        chunk_index = len(self.progress.completed_outputs)
        writer = BinaryWriter(
            cache_dir=str(self.ctx.output_dir),
            chunk_size=chunk_size,
            chunk_index=chunk_index,
        )
        for index, sample in self.stream():
            chunk_path = writer.add_item(index, sample)
            self.progress.mark_sample(index)
            if chunk_path:
                path = Path(chunk_path)
                self.progress.record_output(path.name)
                yield path
        for chunk_file in writer.done():
            path = Path(chunk_file)
            self.progress.record_output(path.name)
            yield path
        self.progress.flush()
