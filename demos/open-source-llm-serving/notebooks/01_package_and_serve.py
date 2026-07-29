# Databricks notebook source
# MAGIC %md
# MAGIC # Model in a Volume → MLflow → Unity Catalog → Model Serving
# MAGIC ### A reusable, end-to-end open-source-model serving starter
# MAGIC
# MAGIC This notebook takes a model that has been **staged into a Unity Catalog Volume**, packages it
# MAGIC with **MLflow**, registers it into **Unity Catalog**, and deploys it as a real-time
# MAGIC **Model Serving** endpoint you can query over REST.
# MAGIC
# MAGIC **The Volume is the handoff interface.** This notebook doesn't care how the model got into
# MAGIC the Volume — that's a separate, pluggable *acquisition* step:
# MAGIC - **Hugging Face access?** Run `00_pull_from_huggingface.py` first.
# MAGIC - **Air-gapped workspace** (no HF egress)? Move the files in via your approved transfer path.
# MAGIC - **Fine-tuned / internal model?** Write its files to the Volume however you like.
# MAGIC
# MAGIC Because it always loads from the Volume, this notebook runs **unchanged** in a connected
# MAGIC workspace and in an air-gapped one (as long as `%pip` resolves — e.g. through a private PyPI
# MAGIC mirror). It's also **deterministic** — no live network dependency at packaging time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Demo model:** `microsoft/Phi-4-mini-instruct` — MIT-licensed and **not gated** on Hugging
# MAGIC Face (no token / license click required), ~3.8B parameters, 128K context. A clean default for
# MAGIC a starter: small enough to run anywhere, permissive license, no access friction. To use a
# MAGIC different model, stage it into the Volume and change `MODEL_NAME` in the config cell.
# MAGIC
# MAGIC **Runtime:** Databricks Runtime **15.4 LTS ML** or later (recent PyTorch; ships MLflow 2.x,
# MAGIC which Stage 0 upgrades to 3.x). We pin `transformers>=4.49.0` because Phi-4-mini's modeling
# MAGIC code was upstreamed into transformers in that release.
# MAGIC
# MAGIC **What you get at the end:** a live endpoint that accepts OpenAI-style chat requests
# MAGIC (`{"messages": [...]}`) and returns generated text.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### New to Databricks? Four things this notebook uses that aren't standard Python:
# MAGIC - **Cells.** The `# COMMAND ----------` lines split this file into runnable cells; run them
# MAGIC   top to bottom. `# MAGIC %md` cells are formatted notes like this one.
# MAGIC - **`spark` and `dbutils`** are provided automatically in every notebook — no import needed.
# MAGIC   `spark.sql(...)` runs SQL; `dbutils` is a Databricks utility helper.
# MAGIC - **`%pip install` + `dbutils.library.restartPython()`** installs libraries for this notebook,
# MAGIC   then restarts the Python process so the new versions load cleanly. Run that cell first.
# MAGIC - **A Unity Catalog Volume** is just a governed folder that shows up as a normal file path
# MAGIC   (`/Volumes/<catalog>/<schema>/<name>/...`). We read the model's files from one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 0 — Install packages
# MAGIC
# MAGIC These installs are **identical in a connected and an air-gapped workspace**. In an air-gapped
# MAGIC environment, `%pip` resolves against the workspace's configured private package index (e.g. a
# MAGIC Nexus/Artifactory mirror set at the workspace level), so there is **no `--index-url` flag to
# MAGIC add** — the same line just works.
# MAGIC
# MAGIC We pin `transformers>=4.49.0` (Phi-4-mini requirement) and `accelerate` (efficient model
# MAGIC loading). We pin **`mlflow>=3.1`** deliberately: Stage 3 calls `log_model(..., name="model")`,
# MAGIC and the `name=` argument only exists in MLflow 3.x — on MLflow 2.x that call requires the
# MAGIC older `artifact_path=` and would raise `TypeError: missing 'artifact_path'`. Pinning the 3.x
# MAGIC floor keeps the notebook and the logged environment consistent.

# COMMAND ----------

# MAGIC %pip install --upgrade "transformers>=4.49.0" "accelerate>=0.34.0" "mlflow>=3.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1 — Configuration
# MAGIC
# MAGIC Set catalog / schema / model name / endpoint name in the **input fields (widgets) at the top
# MAGIC of the notebook** — no code editing needed. `catalog` has no default (set it to a catalog you
# MAGIC can write to); the rest have working defaults. Keep catalog / schema / model_name in sync with
# MAGIC `00_pull_from_huggingface.py`.
# MAGIC
# MAGIC ### This notebook loads the model **from a Unity Catalog Volume** — always.
# MAGIC The Volume is the handoff interface: `01` doesn't care *how* the model got there, only that
# MAGIC the files are present. That keeps "where the model comes from" a separate, pluggable concern:
# MAGIC - **Hugging Face access?** Run `00_pull_from_huggingface.py` first to stage the files.
# MAGIC - **Air-gapped workspace?** Move the files into the Volume via your approved transfer path.
# MAGIC - **Fine-tuned / internal model?** Write its files to the Volume by whatever means.
# MAGIC
# MAGIC In every case, `01` below is identical. Loading from the Volume also makes this run
# MAGIC **deterministic** — no live network dependency at packaging time.

# COMMAND ----------

# ---- Parameters (widgets render as a form at the top of the notebook) -------
# Edit these in the input fields at the top instead of hunting through code. The registered model
# lands at {catalog}.{schema}.{model_name}; the model is loaded from a Volume under the same
# catalog/schema. Keep catalog / schema / model_name in sync with 00_pull_from_huggingface.py.
#
# IMPORTANT: pick a catalog whose managed storage bucket actually permits writes from the compute
# network. A freshly-created MANAGED catalog with no storage_root can fall back to the *metastore
# default* bucket, whose network/endpoint policy may block object writes — MLflow artifact upload
# then fails with AccessDenied. Prefer a catalog backed by its own external location / bucket.
dbutils.widgets.text("catalog", "", "Catalog (writable) — required")
dbutils.widgets.text("schema", "os_llm_serving", "Schema")
dbutils.widgets.text("model_name", "phi4_mini_instruct", "Model name (Volume folder + registered model)")
dbutils.widgets.text("endpoint_name", "os-llm-serving-demo", "Serving endpoint name")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
MODEL_NAME = dbutils.widgets.get("model_name").strip()
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
if not (CATALOG and SCHEMA and MODEL_NAME):
    raise ValueError("Set the catalog / schema / model_name widgets at the top of the notebook.")

REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"
# UC Volume path the model was staged to (must match 00_pull_from_huggingface.py).
VOLUME_MODEL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/models/{MODEL_NAME}"

# ---- Fixed knobs (edit in code if needed) ----------------------------------
# Leave False for models supported natively by your transformers version (Phi-4-mini has been
# since 4.49.0). True forces the model's bundled modeling code, which can break against a newer
# transformers — only set it True if a model genuinely requires custom code, and pin transformers
# to match.
TRUST_REMOTE_CODE = False

# Phi-4-mini (~3.8B) is ~7.6GB in bf16. Model Serving CPU memory is per-concurrency:
#   CPU = 4GB, CPU_MEDIUM = 8GB, CPU_LARGE = 16GB. So a ~7.6GB model needs CPU_LARGE
#   (the default "CPU"/Small would OOM). If the workspace has GPU serving, GPU_SMALL is
#   far snappier. Stage 4 tries PREFERRED_WORKLOAD_TYPE and falls back to FALLBACK on error.
# GPU serving availability varies by region/workspace; the deploy step auto-falls back to CPU
# if GPU is rejected.
PREFERRED_WORKLOAD_TYPE = "GPU_SMALL"   # try GPU first (if the workspace offers GPU serving)...
FALLBACK_WORKLOAD_TYPE = "CPU"          # ...fall back to CPU (with CPU_LARGE sizing) if GPU unavailable
WORKLOAD_SIZE = "Large"                 # CPU_LARGE (16GB) — required to fit ~7.6GB weights on CPU
SCALE_TO_ZERO = True                    # cost-friendly for a demo endpoint

print(f"Model source:     {VOLUME_MODEL_PATH}")
print(f"Registered as:    {REGISTERED_MODEL_NAME}")
print(f"Endpoint:         {ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2 — Load the model & tokenizer from the Volume
# MAGIC
# MAGIC We load the model and tokenizer as separate components (rather than a `pipeline`) because
# MAGIC MLflow's `transformers` flavor logs a `{"model", "tokenizer"}` dict cleanly and, with
# MAGIC `task="llm/v1/chat"`, gives us a standard OpenAI-style chat interface on the endpoint.
# MAGIC
# MAGIC We point `from_pretrained` at the **local Volume path** and set Hugging Face's offline flags
# MAGIC so `transformers` never reaches the network — the files are already local, and this makes the
# MAGIC load behave identically in a connected workspace and in an air-gapped one.

# COMMAND ----------

import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# Files are local in the Volume — forbid any network access so a stray code path fails fast
# (a safe error) instead of trying to reach huggingface.co. Same behavior everywhere.
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

# Fail early with a clear message if the Volume isn't populated yet.
if not os.path.isdir(VOLUME_MODEL_PATH) or not os.listdir(VOLUME_MODEL_PATH):
    raise FileNotFoundError(
        f"No model files found at {VOLUME_MODEL_PATH}. "
        f"Stage the model first (see 00_pull_from_huggingface.py) or move files into the Volume."
    )

print(f"Loading from UC Volume: {VOLUME_MODEL_PATH}")

tokenizer = AutoTokenizer.from_pretrained(
    VOLUME_MODEL_PATH,
    trust_remote_code=TRUST_REMOTE_CODE,
)

model = AutoModelForCausalLM.from_pretrained(
    VOLUME_MODEL_PATH,
    trust_remote_code=TRUST_REMOTE_CODE,
    torch_dtype=torch.bfloat16,   # half precision: smaller artifact, faster load
)

# Place the model explicitly instead of device_map="auto". On CPU-only or single-GPU nodes,
# "auto" can trigger a tensor-parallel / torch.distributed init path in recent transformers and
# fail with a `tp_plan='auto'` error. A plain load + optional .to("cuda") is portable and avoids
# it. (Device placement here only affects the local smoke test below — the packaged artifact and
# the served endpoint rebuild independently of it.)
if torch.cuda.is_available():
    model = model.to("cuda")

print(f"Loaded {MODEL_NAME}: {model.num_parameters()/1e9:.2f}B parameters "
      f"(device: {'cuda' if torch.cuda.is_available() else 'cpu'})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick local smoke test (optional but reassuring)
# MAGIC Before we package anything, confirm the model actually generates. We build the prompt with
# MAGIC the tokenizer's **chat template**, which inserts Phi-4-mini's expected
# MAGIC `<|system|>…<|end|><|user|>…<|end|><|assistant|>` markers automatically.

# COMMAND ----------

# Generic smoke-test prompt — swap in a domain-specific question for your own demo.
messages = [
    {"role": "system", "content": "You are a concise, helpful assistant."},
    {"role": "user", "content": "In one sentence, what is machine learning?"},
]

# return_dict=True gives a BatchEncoding (input_ids + attention_mask); pass it with **inputs
# so generate() gets a proper attention mask (avoids a positional-tensor error on recent
# transformers, and silences the "no attention mask" warning).
inputs = tokenizer.apply_chat_template(
    messages, add_generation_prompt=True, return_tensors="pt", return_dict=True
).to(model.device)

with torch.no_grad():
    generated = model.generate(**inputs, max_new_tokens=80, do_sample=False)

# Only decode the newly generated tokens (skip the prompt we fed in).
response = tokenizer.decode(
    generated[0][inputs["input_ids"].shape[-1]:], skip_special_tokens=True
)
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3 — Package with MLflow & register into Unity Catalog
# MAGIC
# MAGIC This is the packaging step. Key choices, and why:
# MAGIC
# MAGIC - **`task="llm/v1/chat"`** — gives the served model a standard OpenAI-style chat schema
# MAGIC   (`{"messages": [...]}` in, chat completion out). No custom pre/post-processing needed.
# MAGIC - **`save_pretrained=True`** (the default) — copies the **actual model weights** into the
# MAGIC   MLflow artifact. This is what makes air-gapped serving work: the serving container builds
# MAGIC   from the artifact and **never needs to reach Hugging Face**.
# MAGIC - **`registered_model_name`** with a 3-level UC name — registers straight into Unity
# MAGIC   Catalog (we set the registry URI to `databricks-uc` first).
# MAGIC - **`pip_requirements`** — pin the libraries the serving container must install (from your
# MAGIC   private mirror, if air-gapped). We include `trust_remote_code`'s dependency needs by
# MAGIC   pinning transformers.
# MAGIC - **`input_example`** — drives signature inference and gives the endpoint UI a ready-made
# MAGIC   sample request.

# COMMAND ----------

import os
import mlflow

# --- Region hint for MLflow's S3 client (needed in some AWS partitions) -----
# When MLflow writes the model artifact to the UC-managed S3 bucket, boto3 must know the AWS
# region. In some AWS partitions/regions, region auto-detection can fail with
# "Unable to determine the region for S3 bucket ...". Setting it explicitly fixes that; it's
# harmless in commercial AWS if the region matches. >>> Set this to YOUR workspace's region. <<<
os.environ.setdefault("AWS_DEFAULT_REGION", "us-gov-west-1")   # example — change for your account

# Register models into Unity Catalog (not the legacy workspace registry).
mlflow.set_registry_uri("databricks-uc")

# Make sure the destination schema exists (safe to re-run). We assume CATALOG already exists
# and is writable (see the note in Stage 1); we create only the schema under it.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# The components dict is the recommended shape for chat models: MLflow logs the model
# and tokenizer together so the endpoint can reconstruct the full chat pipeline.
components = {"model": model, "tokenizer": tokenizer}

# A representative request. With task="llm/v1/chat" this must use the "messages" schema.
input_example = {
    "messages": [
        {"role": "user", "content": "What is machine learning?"}
    ]
}

with mlflow.start_run(run_name=f"log-{MODEL_NAME}"):
    logged_model = mlflow.transformers.log_model(
        transformers_model=components,
        name="model",                      # 'name' is the current param (replaces artifact_path)
        task="llm/v1/chat",                # OpenAI-style chat interface on the endpoint
        input_example=input_example,
        registered_model_name=REGISTERED_MODEL_NAME,
        # Libraries the serving container installs (from your private mirror, if air-gapped).
        # Pinning transformers>=4.49.0 lets the container rebuild Phi-4-mini natively at serve time
        # — no trust_remote_code needed, since the weights are already baked into the artifact.
        pip_requirements=[
            "transformers>=4.49.0",
            "accelerate>=0.34.0",
            "torch",
            "einops",                      # commonly needed by transformers custom-code models
        ],
    )

print(f"Logged model URI: {logged_model.model_uri}")
print(f"Registered as:    {REGISTERED_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the version we just registered
# MAGIC We pull the latest version number so the serving endpoint points at exactly this build.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")
# The version we just created is the highest-numbered one for this registered model.
model_version = max(
    int(mv.version) for mv in client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
)
print(f"Registered {REGISTERED_MODEL_NAME} version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4 — Create the Model Serving endpoint
# MAGIC
# MAGIC We deploy the registered UC model version to a real-time serving endpoint using the
# MAGIC Databricks SDK. Two operational choices worth calling out to the customer:
# MAGIC
# MAGIC - **`workload_type`** — `GPU_SMALL` makes a ~3.8B model feel instant, **if** the workspace
# MAGIC   offers GPU serving (availability varies by region/workspace). The next cell **tries GPU
# MAGIC   first and automatically falls back to CPU** if the workspace rejects it.
# MAGIC - **`workload_size`** — on CPU, a ~7.6GB (bf16) model needs `Large` (CPU_LARGE = 16GB/
# MAGIC   concurrency); `Small`/`CPU` (4GB) would OOM. On GPU, size controls provisioned replicas.
# MAGIC - **`scale_to_zero_enabled=True`** — the endpoint spins down when idle so a demo endpoint
# MAGIC   costs nothing between sessions (first request after idle pays a cold-start).
# MAGIC
# MAGIC The first build takes several minutes (the container installs the pinned libraries and
# MAGIC bakes in the weights). Subsequent updates are faster.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

w = WorkspaceClient()


def _served_entity(workload_type):
    return ServedEntityInput(
        entity_name=REGISTERED_MODEL_NAME,
        entity_version=str(model_version),
        workload_type=workload_type,
        workload_size=WORKLOAD_SIZE,
        scale_to_zero_enabled=SCALE_TO_ZERO,
    )


def _deploy(workload_type):
    """Create or update the endpoint with the given workload type. Raises on rejection."""
    entity = _served_entity(workload_type)
    existing = [e for e in w.serving_endpoints.list() if e.name == ENDPOINT_NAME]
    if existing:
        print(f"Updating '{ENDPOINT_NAME}' -> v{model_version} on {workload_type}...")
        w.serving_endpoints.update_config(name=ENDPOINT_NAME, served_entities=[entity])
    else:
        print(f"Creating '{ENDPOINT_NAME}' on {workload_type}...")
        w.serving_endpoints.create(
            name=ENDPOINT_NAME,
            config=EndpointCoreConfigInput(name=ENDPOINT_NAME, served_entities=[entity]),
        )


# Try GPU first; fall back to CPU (at CPU_LARGE sizing) if the workspace has no GPU serving.
try:
    _deploy(PREFERRED_WORKLOAD_TYPE)
    chosen_workload = PREFERRED_WORKLOAD_TYPE
except Exception as e:
    print(f"'{PREFERRED_WORKLOAD_TYPE}' rejected ({type(e).__name__}: {str(e)[:200]}).")
    print(f"Falling back to '{FALLBACK_WORKLOAD_TYPE}' at size '{WORKLOAD_SIZE}'...")
    _deploy(FALLBACK_WORKLOAD_TYPE)
    chosen_workload = FALLBACK_WORKLOAD_TYPE

print(f"\nDeploying on workload_type={chosen_workload}, workload_size={WORKLOAD_SIZE}.")
print("Endpoint build started. Track progress in the Serving UI (Serving -> " + ENDPOINT_NAME + ").")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for the endpoint to be ready
# MAGIC Blocks until the endpoint reports READY. Safe to run repeatedly.
# MAGIC
# MAGIC **Note the timeout.** The SDK's default wait is only 20 minutes, but a *first* build of a
# MAGIC multi-GB model — especially on GPU — can take longer (container image + weight load). We
# MAGIC pass a generous 45-minute timeout so a slow-but-healthy build isn't reported as a failure.
# MAGIC If it still times out, the build usually finishes on its own shortly after — just re-run
# MAGIC this cell (it's idempotent) or watch the Serving UI.

# COMMAND ----------

from datetime import timedelta

endpoint = w.serving_endpoints.wait_get_serving_endpoint_not_updating(
    ENDPOINT_NAME, timeout=timedelta(minutes=45)
)
print(f"Endpoint state: {endpoint.state.ready} / config update: {endpoint.state.config_update}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 5 — Query the live endpoint
# MAGIC
# MAGIC The payoff. We send an OpenAI-style chat request and get generated text back — over the
# MAGIC same REST interface any downstream application would use. We show two ways:
# MAGIC 1. A **`chat()` helper** on the SDK's authenticated low-level client — simplest from inside
# MAGIC    Databricks (no host/token juggling).
# MAGIC 2. A **raw REST** snippet — the shape an external caller / app would use.
# MAGIC
# MAGIC > **Why not the typed `w.serving_endpoints.query(...)`?** It requires `messages` as
# MAGIC > `ChatMessage` objects and expects a single-object reply — but this MLflow `llm/v1/chat`
# MAGIC > endpoint returns the completion **wrapped in a top-level JSON list** (`[ {"choices": …} ]`),
# MAGIC > which the typed parser rejects. Posting through `w.api_client.do` and unwrapping the list is
# MAGIC > the robust pattern.

# COMMAND ----------

import time

# --- Option 1: Databricks SDK (low-level client) ---
def chat(messages, max_tokens=120, temperature=0.2):
    """Query the chat endpoint and return the assistant's text.

    Uses the SDK's authenticated client so we don't handle host/token ourselves. This endpoint
    wraps its chat completion in a top-level list; we unwrap it defensively so the helper also
    works against a standard (dict-shaped) chat endpoint.
    """
    resp = w.api_client.do(
        "POST",
        f"/serving-endpoints/{ENDPOINT_NAME}/invocations",
        body={"messages": messages, "max_tokens": max_tokens, "temperature": temperature},
    )
    payload = resp[0] if isinstance(resp, list) else resp
    return payload["choices"][0]["message"]["content"]


start = time.time()
answer = chat(
    [
        {"role": "system", "content": "You are a concise, helpful assistant."},
        {"role": "user", "content": "In two sentences, explain what a vector database is."},
    ],
    max_tokens=120,
    temperature=0.2,
)
elapsed = time.time() - start

print(f"Latency: {elapsed:.2f}s\n")
print(answer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw REST — the shape an external app would use
# MAGIC This is the same call any downstream service can make with a workspace token. Useful to
# MAGIC hand to the customer's application developers.

# COMMAND ----------

# The endpoint URL and a token are available from the notebook context. In a real app,
# use a service principal token and store it in a secret scope — never hard-code it.
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
INVOCATION_URL = f"https://{DATABRICKS_HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations"

print("POST", INVOCATION_URL)
print("""
curl -X POST "$INVOCATION_URL" \\
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
        "messages": [
          {"role": "user", "content": "Summarize what this endpoint does in one sentence."}
        ],
        "max_tokens": 80
      }'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done 🎉  — and how to hand this off
# MAGIC
# MAGIC You now have a governed, UC-registered model served behind a real-time endpoint.
# MAGIC
# MAGIC **To reuse this for a different model:** stage the new model into the Volume (via
# MAGIC `00_pull_from_huggingface.py` or your own acquisition step), then change `MODEL_NAME` (and
# MAGIC `TRUST_REMOTE_CODE` if the new model isn't native to transformers) in Stage 1.
# MAGIC
# MAGIC **To run in an air-gapped workspace:**
# MAGIC 1. Get the model files into the UC Volume **once** — move them in via your approved transfer
# MAGIC    path (no Hugging Face egress, so you would not run `00_pull_from_huggingface.py` there).
# MAGIC    The Volume layout is documented in that notebook's verify step.
# MAGIC 2. Set `PREFERRED_WORKLOAD_TYPE = "CPU"` in Stage 1 if the workspace has no GPU serving.
# MAGIC 3. Run this notebook top to bottom — everything else is identical.
# MAGIC
# MAGIC **Cost hygiene:** the endpoint scales to zero when idle. To remove it entirely, run the
# MAGIC teardown cell below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Teardown (optional) — delete the endpoint
# MAGIC Uncomment and run to remove the serving endpoint when you're done demoing.

# COMMAND ----------

# w.serving_endpoints.delete(ENDPOINT_NAME)
# print(f"Deleted endpoint '{ENDPOINT_NAME}'")
