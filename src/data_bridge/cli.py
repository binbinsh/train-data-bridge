from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Optional

import typer

from data_bridge.api import run_from_file, run_pipeline
from data_bridge.compute.runner import VastRemoteRunner
from data_bridge.compute.vast import VastClient, VastSession
from data_bridge.config import PipelineSettings, R2Settings, Settings, VastSettings, ensure_config, load_settings

app = typer.Typer(help="Process datasets on Vast.ai and publish shards to Cloudflare R2.")

DEFAULT_CONFIG = Settings(
    vast=VastSettings(
        api_key="VAST_API_KEY",
        offer_id=0,
        image="docker.io/library/pytorch:2.3.0-cuda12.1-cudnn8-runtime",
        storage_gb=50,
        max_price_usd_hour=2.5,
    ),
    r2=R2Settings(
        account_id="ACCOUNT_ID",
        access_key_id="R2_ACCESS_KEY",
        secret_access_key="R2_SECRET",
        bucket="train-data-bridge",
        region="auto",
        prefix="datasets",
    ),
    pipeline=PipelineSettings(
        name="webdataset",
        input_uri="file://data/raw/sample.jsonl",
        input_format="jsonl",
        samples_per_shard=2048,
        staging_dir=".train-data-bridge",
        output_format=None,
        source_options={},
    ),
)


@app.command()
def init(config: Path = typer.Option(Path("configs/example.yaml"), help="Config path to create")) -> None:
    """Create a starter YAML config."""
    ensure_config(config, DEFAULT_CONFIG)
    typer.echo(f"Template written to {config}")


@app.command()
def plan(config: Path = typer.Option(Path("configs/example.yaml"), help="Config to inspect")) -> None:
    """Print a textual execution plan."""
    settings = load_settings(config)
    typer.echo(f"Pipeline: {settings.pipeline.name}")
    typer.echo(f"Input URI: {settings.pipeline.input_uri}")
    typer.echo(f"Input format: {settings.pipeline.input_format}")
    typer.echo(f"Samples/shard: {settings.pipeline.samples_per_shard}")
    typer.echo(f"Upload prefix: {settings.r2.prefix}")
    typer.echo(f"Vast offer: {settings.vast.offer_id} @ ${settings.vast.max_price_usd_hour}/h")


@app.command()
def run(
    config: Path = typer.Option(Path("configs/example.yaml"), help="Config to execute"),
    prefix: Optional[str] = typer.Option(None, help="Override upload prefix inside the R2 bucket"),
    use_vast: bool = typer.Option(False, help="Actually provision Vast.ai resources"),
    remote_child: bool = typer.Option(
        False,
        "--remote-child",
        hidden=True,
        help="Internal flag indicating execution inside a Vast.ai child process.",
    ),
) -> None:
    """Execute the configured pipeline and upload shards to R2."""
    settings = load_settings(config)
    if remote_child and use_vast:
        typer.echo("--use-vast ignored inside remote child invocation.")
        use_vast = False
    if use_vast:
        vast_settings = settings.vast
        if not vast_settings.repo_url:
            raise typer.BadParameter("Set vast.repo_url in the config before using --use-vast.")
        client = VastClient(vast_settings.api_key)
        with VastSession(client) as session:
            info = session.provision(vast_settings.offer_id, vast_settings.image, vast_settings.storage_gb)
            typer.echo(f"Provisioned Vast.ai instance #{info.get('id')} (waiting for readiness)...")
            session.wait_for_ready()
            runner = VastRemoteRunner(session, settings, config)
            typer.echo("Running train-data-bridge remotely on Vast.ai...")
            output = runner.run()
            typer.echo(output)
        return

    manifest_uri = run_pipeline(settings, prefix=prefix)
    if manifest_uri:
        typer.echo(f"Manifest stored at {manifest_uri}")
    else:
        typer.echo("No pending artifacts detected; dataset already processed.")


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":
    main()
