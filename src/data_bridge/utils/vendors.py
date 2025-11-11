from __future__ import annotations

import sys
from pathlib import Path


def ensure_vendor(name: str) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    vendor_path = repo_root / "externals" / name
    if vendor_path.exists() and str(vendor_path) not in sys.path:
        sys.path.append(str(vendor_path))
