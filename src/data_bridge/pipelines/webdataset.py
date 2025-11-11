from __future__ import annotations

from pathlib import Path
from typing import Iterator, Optional

from data_bridge.pipelines.base import Pipeline
from data_bridge.utils.vendors import ensure_vendor

ensure_vendor("webdataset")
import webdataset as wds  # type: ignore


class WebdatasetPipeline(Pipeline):
    def process(self) -> Iterator[Path]:
        self.cleanup_untracked_outputs()
        for pending in self.pending_artifacts():
            yield pending

        shard_size = max(1, self.settings.samples_per_shard)
        output_template = self.ctx.output_dir / "shard-{shard:05d}.tar"
        writer = None
        writer_shard = None
        current_path: Optional[Path] = None

        def finalize_writer() -> Optional[Path]:
            nonlocal writer, current_path
            if not writer or not current_path:
                return None
            writer.close()
            path = current_path
            self.progress.record_output(path.name)
            writer = None
            current_path = None
            return path

        for index, sample in self.stream():
            shard_number = index // shard_size
            if writer_shard != shard_number:
                finished = finalize_writer()
                if finished:
                    yield finished
                shard_path = Path(str(output_template).format(shard=shard_number))
                if shard_path.exists() and not self.progress.has_record(shard_path.name):
                    shard_path.unlink()
                writer = wds.TarWriter(str(shard_path))
                writer_shard = shard_number
                current_path = shard_path
            key = sample.get("__key__", f"{index:08d}")
            writer.write({"__key__": key, **sample})
            self.progress.mark_sample(index)

        finished = finalize_writer()
        if finished:
            yield finished
        self.progress.flush()
