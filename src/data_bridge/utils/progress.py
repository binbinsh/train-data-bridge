from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable, List


class ProgressTracker:
    """Lightweight progress/state persistence for resumable pipelines."""

    def __init__(self, path: Path, checkpoint_every: int = 500) -> None:
        self.path = path
        self.checkpoint_every = checkpoint_every
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            self.state = json.loads(path.read_text())
        else:
            self.state = {
                "next_index": 0,
                "completed_outputs": [],
                "pending_outputs": [],
                "updated_at": time.time(),
            }
        self._sample_dirty = 0

    # --------------------------------------------------------------------- API
    @property
    def next_index(self) -> int:
        return int(self.state.get("next_index", 0))

    @property
    def completed_outputs(self) -> List[str]:
        return list(self.state.get("completed_outputs", []))

    def pending_artifacts(self, output_dir: Path) -> List[Path]:
        """Return existing local artifacts that still need uploading."""
        pending = []
        missing = []
        for name in self.state.get("pending_outputs", []):
            path = output_dir / name
            if path.exists():
                pending.append(path)
            else:
                missing.append(name)
        if missing:
            for name in missing:
                self.state["pending_outputs"].remove(name)
            self._save(force=True)
        return pending

    def mark_sample(self, index: int) -> None:
        """Persist the next sample index periodically."""
        next_index = index + 1
        if next_index <= self.state.get("next_index", 0):
            return
        self.state["next_index"] = next_index
        self._sample_dirty += 1
        if self._sample_dirty >= self.checkpoint_every:
            self._save()

    def record_output(self, filename: str) -> None:
        completed = self.state.setdefault("completed_outputs", [])
        pending = self.state.setdefault("pending_outputs", [])
        if filename not in completed:
            completed.append(filename)
        if filename not in pending:
            pending.append(filename)
        self._save(force=True)

    def mark_uploaded(self, filenames: Iterable[str]) -> None:
        pending = self.state.setdefault("pending_outputs", [])
        before = len(pending)
        to_remove = set(filenames)
        pending[:] = [name for name in pending if name not in to_remove]
        if len(pending) != before:
            self._save(force=True)

    def has_record(self, filename: str) -> bool:
        return filename in self.state.get("completed_outputs", [])

    def flush(self) -> None:
        self._save(force=True)

    # ------------------------------------------------------------------ intern
    def _save(self, force: bool = False) -> None:
        if not force and self._sample_dirty == 0:
            return
        self.state["updated_at"] = time.time()
        self.path.write_text(json.dumps(self.state, indent=2))
        self._sample_dirty = 0
