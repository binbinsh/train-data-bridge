# Development Guide

## High-level flow

1. **Source acquisition**  
   - `SampleStream` lazily reads from HTTPS/S3/R2 (`smart-open`) or Hugging Face Hub (`datasets` streaming).  
   - No dataset is staged locally in full; lines/samples are decoded on-the-fly inside the Vast.ai container.
2. **Pipeline execution**  
   - Choose between WebDataset or LitData pipeline.  
   - Apply the configured sharding strategy.  
   - Emit tar shards (`.tar`, `.tar.gz`) or LitData parquet/tensor blocks.
3. **Artifact promotion**  
   - Upload shards to Cloudflare R2 using the S3-compatible API.  
   - Generate a manifest with metadata (hashes, sizes, schema, provenance).
4. **Consumption**  
   - Training jobs (e.g., PyTorch DDP) stream data directly from R2 via presigned URLs.

## Module breakdown

| Module | Responsibility |
| --- | --- |
| `data_bridge.config` | Load YAML/TOML configs, expose strongly typed settings for Vast, R2, and pipeline parameters. |
| `data_bridge.compute.vast` | Minimal Vast.ai REST wrapper for listings, machine reservation, command execution, and teardown. |
| `data_bridge.compute.runner` | Generates bootstrap scripts and replays the CLI remotely when `--use-vast` is enabled. |
| `data_bridge.storage.r2` | Thin client around boto3 for uploading shards, generating manifests, and presigning download URLs. |
| `data_bridge.pipelines.base` | Abstract Pipeline interface (prepare → process → finalize) plus utilities for staging dirs, resumable progress, and source streaming. |
| `data_bridge.pipelines.webdataset` | Glue layer that invokes `externals/webdataset` helpers to build shard writers. |
| `data_bridge.pipelines.litdata` | Similar glue for `externals/litdata`, focuses on partitioned parquet/tensor exports. |
| `data_bridge.cli` | Typer CLI with `init`, `plan`, and `run` commands. |
| `data_bridge.api` | Programmatic entry points (`run_pipeline`, `run_from_file`) for embedding into other projects. |
| `data_bridge.io.sources` | Streaming adapters for JSONL over smart-open and Hugging Face datasets. |
| `data_bridge.utils.progress` | Durable progress tracker (next sample index, completed shards, pending uploads). |

## Configuration

Example YAML (referenced by CLI):

```yaml
vast:
  api_key: "..."
  offer_id: 12345
  image: "docker.io/library/pytorch:2.3.0-cuda12.1-cudnn8-runtime"
  storage_gb: 50

r2:
  account_id: "xxxx"
  access_key_id: "AKIA..."
  secret_access_key: "..."
  bucket: "train-data-bridge"
  region: "auto"

pipeline:
  name: "webdataset"
  input_uri: "hf://laion/laion2B-en"
  input_format: "huggingface"
  samples_per_shard: 2048
  source_options:
    split: "train"
    config: "default"
    load_kwargs:
      token: ${HF_TOKEN}
```

## Execution lifecycle

1. CLI/API loads config → instantiates `PipelineSettings`.
2. `PipelineFactory` creates WebDataset or LitData pipeline (local mode).
3. When `--use-vast` is passed, `VastRemoteRunner` provisions a Vast.ai instance, clones the repo (based on `vast.repo_url/ref`), recreates the config remotely, and re-invokes `train-data-bridge run` (which delegates to the shared API) inside the container.
4. `SampleStream` opens the remote dataset and skips to `ProgressTracker.next_index` to resume mid-stream.
5. Pipeline streams data, writes shards/chunks, and records each successful file into `ProgressTracker`.
6. CLI/API first uploads any `pending` local artifacts from previous crashes, then the new ones, removing the files after confirmation.
7. Manifest stored at `r2://bucket/path/manifest.json`.

## Scaling considerations

- **Chunking**: WebDataset uses `TarWriter` rolling buckets, LitData leverages `BinaryWriter` chunks; both keyed by `samples_per_shard`.
- **Resume**: `ProgressTracker` persists `next_index` + pending shard filenames, so a rerun only streams yet-to-be-processed samples and re-uploads incomplete shards.
- **Cost-awareness**: `VastSession` polls GPU prices and suggests cheaper instances before provisioning.
- **Monitoring**: CLI prints progress, optionally pushes metrics to Prometheus pushgateway (future).

## WebDataset / LitData support notes

- **WebDataset**: `TarWriter` supports appending shards sequentially. By recomputing shard IDs from the global sample index (`idx // samples_per_shard`) and deleting any untracked tar files on startup, the pipeline can resume safely even if the previous run crashed mid-write. Tar shards are small (≈1–2 GB) and optimized for Cloudflare R2 streaming.
- **LitData**: `BinaryWriter` already provides chunk numbering, optional compression, and metadata sidecars. We pass `chunk_index=len(completed_outputs)` on resume so new chunk filenames continue monotonically. LitData readers (`StreamingDataset`) can pull directly from R2.
- **Both pipelines** expose pending outputs so the CLI/API can retry uploads without duplicating compute, ensuring Vast.ai time is spent on new work only.

## Dev Principles
- Prefer comments in English.
- Prefer to add a concise user manual to README.md in the project root.
- Try to avoid using try-except whenever possible.
- Do not use environment toggles unless explicitly asked.
- Always query context7 for the most recent docs and best practices.
- Prefer using Typer instead of argparse.
- Always use uv for python package manager. The uv venv is located in the project root. I always prefer to use uv run from the project root.
- After modifying the project, promptly update all documentation.

## Testing
- Run `uv run python -m pytest` from the repo root; pytest is scoped to the `tests/` directory.
- If you need to execute a single test, append the module path (e.g., `uv run python -m pytest tests/test_api.py`).
