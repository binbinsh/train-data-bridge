"""Train Data Bridge package."""

from importlib import metadata

from data_bridge.api import run_from_file, run_pipeline


def __getattr__(name: str) -> str:
    if name == "__version__":
        return metadata.version("train-data-bridge")
    raise AttributeError(name)


__all__ = ["__version__", "run_pipeline", "run_from_file"]
