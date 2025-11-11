from __future__ import annotations

import json
from typing import Dict, Iterable, Iterator, Tuple

from smart_open import open as smart_open  # type: ignore

from data_bridge.config import PipelineSettings


class SampleStream:
    """Unified streaming reader that yields (global_index, sample_dict)."""

    def __init__(self, settings: PipelineSettings, start_index: int = 0) -> None:
        self.settings = settings
        self.start_index = max(0, start_index)

    def __iter__(self) -> Iterator[Tuple[int, Dict]]:
        if self.settings.input_format == "jsonl":
            yield from self._iter_jsonl()
        elif self.settings.input_format == "huggingface":
            yield from self._iter_huggingface()
        else:  # pragma: no cover (validator should guard)
            raise ValueError(f"Unsupported input_format {self.settings.input_format}")

    # helpers -----------------------------------------------------------------

    def _iter_jsonl(self) -> Iterator[Tuple[int, Dict]]:
        kwargs = {}
        transport_params = self.settings.source_options.get("transport_params")
        if transport_params:
            kwargs["transport_params"] = transport_params
        with smart_open(self.settings.input_uri, "r", encoding="utf-8", **kwargs) as handle:
            for idx, line in enumerate(handle):
                if idx < self.start_index:
                    continue
                line = line.strip()
                if not line:
                    continue
                yield idx, json.loads(line)

    def _iter_huggingface(self) -> Iterator[Tuple[int, Dict]]:
        from datasets import load_dataset  # lazy import to avoid heavy startup when unused

        dataset_id = self.settings.input_uri.replace("hf://", "")
        config_name = self.settings.source_options.get("config")
        split = self.settings.source_options.get("split", "train")
        streaming_kwargs = self.settings.source_options.get("load_kwargs", {})
        iterable: Iterable[Dict] = load_dataset(
            dataset_id,
            config_name,
            split=split,
            streaming=True,
            **streaming_kwargs,
        )
        for idx, sample in enumerate(iterable):
            if idx < self.start_index:
                continue
            yield idx, dict(sample)
