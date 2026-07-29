# Demo: Open-Source LLM Serving

> Take an open-source model, package it with MLflow, register it in Unity Catalog, and deploy it as
> a real-time Model Serving endpoint — designed to run unchanged in connected and air-gapped
> workspaces.

## Overview

This demo shows the canonical Databricks pattern for bringing your *own* open-source (or fine-tuned)
model to a governed, served endpoint:

```
Model in a UC Volume → MLflow (transformers flavor) → Unity Catalog (registered model) → Model Serving endpoint → REST queries
```

**The Unity Catalog Volume is the handoff interface.** The main notebook always loads the model
*from a Volume* — it doesn't care how the files got there. That keeps *acquisition* (pull from
Hugging Face, an approved air-gapped transfer, or a fine-tuning job) cleanly separate from
*packaging / governance / serving*. Because it loads from the Volume, the packaging notebook runs
**unchanged** in a connected workspace and an air-gapped one — only how the files land in the Volume
differs, and packages resolve through whatever index the workspace is configured with (PyPI or a
private mirror).

**Demo model:** `microsoft/Phi-4-mini-instruct` — MIT-licensed, not gated, ~3.8B parameters. A clean
default: small enough to run almost anywhere, permissive license, no access friction. Swap in any
causal-LM/chat model by changing `MODEL_NAME`.

## Prerequisites

- Databricks workspace with **Unity Catalog** and **Model Serving** enabled
- A cluster running **Databricks Runtime 15.4 LTS ML** or later (single-user access mode)
- Permission to create (or write to) a catalog/schema/volume, and to create a serving endpoint
- The config values in the notebooks (`CATALOG`, `SCHEMA`, `ENDPOINT_NAME`, region) are **examples —
  change them for your workspace**

## Running the Demo

Run the notebooks in order (they're Databricks source-format notebooks — import them into your
workspace, or run from a Git folder):

1. **`notebooks/00_pull_from_huggingface.py`** *(optional)* — pulls a model from Hugging Face into a
   UC Volume. Skip it if the Volume is already populated (by a prior run, an approved air-gapped
   transfer, or a fine-tuned model you staged yourself).
2. **`notebooks/01_package_and_serve.py`** — loads the model from the Volume, logs it with MLflow,
   registers it into Unity Catalog, and deploys a Model Serving endpoint.
3. **`notebooks/02_query_endpoint.py`** — queries the live endpoint (SDK + raw REST). No model load
   or packaging, so it runs on any small compute — the "show the payoff" notebook.

### CPU vs. GPU serving

The endpoint tries `GPU_SMALL` first and automatically falls back to CPU if the workspace has no GPU
serving. A ~3.8B model runs on CPU (`CPU_LARGE`, 16 GB) with a few seconds per response; on GPU it's
snappier (~2–4s). GPU/CPU availability varies by region and workspace.

### Cost

The endpoint uses `scale_to_zero_enabled=True`, so it costs nothing while idle (the first request
after idle pays a one-time cold start). A teardown cell at the end of `01` removes the endpoint.

## Structure

```
open-source-llm-serving/
├── notebooks/
│   ├── 00_pull_from_huggingface.py   # optional: HF → UC Volume (acquisition)
│   ├── 01_package_and_serve.py       # Volume → MLflow → UC → Model Serving
│   └── 02_query_endpoint.py          # query the live endpoint (SDK + REST)
├── pyproject.toml
└── README.md
```

## Notes

- **Weights are baked into the MLflow artifact** (`save_pretrained=True`), so the serving container
  never needs to reach Hugging Face at serve time — the key to the air-gapped path.
- **Load with explicit device placement**, not `device_map="auto"` — on CPU-only / single-GPU nodes
  `"auto"` can trigger a `torch.distributed` init that fails.
- **Pin `mlflow>=3.1`** — the notebook uses `log_model(name=...)`, which requires MLflow 3.x.
- The MLflow `llm/v1/chat` endpoint returns its completion wrapped in a top-level JSON list; the
  query helper unwraps it defensively.
