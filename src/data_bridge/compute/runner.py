from __future__ import annotations

import shlex
import textwrap
from pathlib import Path

from data_bridge.compute.vast import VastSession
from data_bridge.config import Settings


class VastRemoteRunner:
    """Bootstrap the project on a Vast.ai instance and invoke the pipeline remotely."""

    def __init__(self, session: VastSession, settings: Settings, config_path: Path) -> None:
        self.session = session
        self.settings = settings
        self.config_path = config_path

    def run(self) -> str:
        command = self._build_command()
        return self.session.run(command)

    # ------------------------------------------------------------------ private
    def _build_command(self) -> str:
        config_text = self.config_path.read_text()
        vast = self.settings.vast
        repo_url = vast.repo_url
        if not repo_url:
            raise ValueError("vast.repo_url must be configured to use --use-vast")
        repo_ref = vast.repo_ref
        workdir = vast.workdir.rstrip("/")
        script = textwrap.dedent(
            f"""
            set -eo pipefail
            WORKDIR={shlex.quote(workdir)}
            REPO_DIR="$WORKDIR/repo"
            CONFIG_PATH="$WORKDIR/remote-config.yaml"
            mkdir -p "$WORKDIR"
            if [ ! -d "$REPO_DIR/.git" ]; then
              git clone {shlex.quote(repo_url)} "$REPO_DIR"
            fi
            cd "$REPO_DIR"
            git fetch --all --tags
            git checkout {shlex.quote(repo_ref)}
            export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
            if ! command -v uv >/dev/null 2>&1; then
              curl -LsSf https://astral.sh/uv/install.sh | sh
              export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
            fi
            uv sync
            cat <<'EOF' > "$CONFIG_PATH"
            {config_text}
            EOF
            uv run train-data-bridge run --config "$CONFIG_PATH" --remote-child
            """
        ).strip()
        return f"bash -lc {shlex.quote(script)}"
