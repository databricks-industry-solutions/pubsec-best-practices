# Databricks notebook source
# MAGIC %md
# MAGIC # Query the live Model Serving endpoint
# MAGIC ### A lightweight, standalone demo — no packaging, no model load
# MAGIC
# MAGIC `01_package_and_serve.py` builds the whole pipeline (Volume → MLflow → Unity Catalog →
# MAGIC Model Serving). **This notebook does only the last step:** it talks to the already-deployed
# MAGIC endpoint and shows generated text coming back over REST.
# MAGIC
# MAGIC Use it when the endpoint is already up and you just want to **demo the payoff** — a governed,
# MAGIC UC-registered open-source model answering questions through a standard chat API.
# MAGIC
# MAGIC **What this needs:**
# MAGIC - The serving endpoint (set in `ENDPOINT_NAME` below) already deployed by `01` and reachable.
# MAGIC - Any small cluster or serverless — **no GPU, no model download, no MLflow.** We only make
# MAGIC   REST calls, so this runs anywhere with network access to the workspace.
# MAGIC
# MAGIC *New to Databricks notebooks? `# COMMAND ----------` splits this file into cells (run top to
# MAGIC bottom); `spark` and `dbutils` are provided automatically — no import needed.*
# MAGIC
# MAGIC **A word on latency (say this out loud during the demo):**
# MAGIC - The endpoint is set to **scale to zero** when idle, so it costs nothing between sessions.
# MAGIC - The **first request after it's been idle pays a cold start** — the endpoint spins a replica
# MAGIC   back up and loads the model (for a ~7.6GB model, observed ~7–8 minutes cold on CPU/GPU).
# MAGIC - Once warm, responses are **a few seconds** (Phi-4-mini on GPU_SMALL was ~2–4s / ~55 ms per
# MAGIC   token). Run the "warm-up" cell first so the live demo feels instant.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1 — Configuration
# MAGIC
# MAGIC Set the endpoint name in the input field (widget) at the top — it must match the endpoint
# MAGIC created by `01_package_and_serve.py`. Everything else (host URL, auth token) is read
# MAGIC automatically from the notebook context.

# COMMAND ----------

# Name of the already-deployed serving endpoint (created by 01_package_and_serve.py).
dbutils.widgets.text("endpoint_name", "os-llm-serving-demo", "Serving endpoint name")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
if not ENDPOINT_NAME:
    raise ValueError("Set the endpoint_name widget at the top of the notebook.")

# A generic system prompt. Optional — remove it to see the model's default behavior, or swap in
# a domain-specific persona for your own demo.
SYSTEM_PROMPT = "You are a concise, accurate, helpful assistant. Answer plainly."

print(f"Target endpoint: {ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2 — Set up the client and a small `chat()` helper
# MAGIC
# MAGIC We first look up the endpoint so the demo fails clearly ("not found") rather than with a
# MAGIC cryptic HTTP error. The state also tells us whether the next query will pay a cold start
# MAGIC (`Scaled to zero`) or be instant (replicas already up).
# MAGIC
# MAGIC We then define one **`chat()` helper** used by every query cell below. A note on *why* we
# MAGIC don't use the SDK's typed `w.serving_endpoints.query(...)`:
# MAGIC - It requires `messages` to be `ChatMessage` objects, not plain dicts, and
# MAGIC - it tries to parse the reply as a single object — but this endpoint (MLflow
# MAGIC   `task="llm/v1/chat"`) returns the chat completion **wrapped in a top-level JSON list**
# MAGIC   (`[ {"choices": [...]} ]`), which the typed parser rejects.
# MAGIC
# MAGIC So we POST through the SDK's authenticated low-level client (`w.api_client.do`) — no manual
# MAGIC host/token handling — and parse the list shape defensively. This is the robust pattern for
# MAGIC this endpoint from inside Databricks.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

ep = w.serving_endpoints.get(ENDPOINT_NAME)
print(f"Endpoint:      {ep.name}")
print(f"Ready state:   {ep.state.ready}")
print(f"Config update: {ep.state.config_update}")

# Surface the per-entity deployment message (e.g. "Scaled to zero" vs "Scaled to 1").
for e in (ep.config.served_entities or []):
    msg = (e.state.deployment_state_message if e.state else "") or ""
    print(f"Served model:  {e.entity_name} v{e.entity_version}  [{e.workload_type}, {e.workload_size}]  {msg}")


def chat(messages, max_tokens=120, temperature=0.2):
    """Query the chat endpoint and return the assistant's text.

    Uses the SDK's authenticated low-level client so we don't juggle host URLs or tokens. This
    endpoint wraps its chat completion in a top-level list; we unwrap it defensively so the helper
    also works against a standard (dict-shaped) chat endpoint.
    """
    resp = w.api_client.do(
        "POST",
        f"/serving-endpoints/{ENDPOINT_NAME}/invocations",
        body={"messages": messages, "max_tokens": max_tokens, "temperature": temperature},
    )
    payload = resp[0] if isinstance(resp, list) else resp
    return payload["choices"][0]["message"]["content"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3 — (Optional) Warm up the endpoint
# MAGIC
# MAGIC If Stage 2 showed **"Scaled to zero"**, the next request triggers a cold start. Run this cell
# MAGIC **once, before you start presenting**, to absorb that wait now instead of mid-demo.
# MAGIC
# MAGIC It sends a tiny throwaway request with a long client timeout and blocks until the endpoint
# MAGIC responds (or ~10 minutes pass). After it returns, all subsequent queries are warm (~2–4s).
# MAGIC If the endpoint is already warm, this returns in a couple of seconds.

# COMMAND ----------

import time

print("Warming up (this can take several minutes on a cold endpoint)...")
start = time.time()
try:
    # A minimal request; we don't care about the answer, just that a replica is up.
    chat([{"role": "user", "content": "ping"}], max_tokens=1)
    print(f"Endpoint warm. Cold-start wait: {time.time() - start:.1f}s")
except Exception as ex:
    # A client-side timeout here doesn't mean failure — the scale-up may still be finishing.
    # Re-run this cell; the second call usually lands once the replica is ready.
    print(f"Warm-up call returned an error after {time.time() - start:.1f}s: {type(ex).__name__}: {str(ex)[:200]}")
    print("If this was a timeout, the endpoint is likely still scaling up — re-run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4 — Query the endpoint
# MAGIC
# MAGIC The payoff: send OpenAI-style `messages` and get generated text back. We use the `chat()`
# MAGIC helper from Stage 2, which handles auth and this endpoint's list-wrapped response for us.

# COMMAND ----------

start = time.time()
answer = chat(
    [
        {"role": "system", "content": SYSTEM_PROMPT},
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
# MAGIC ## Stage 5 — A small gallery of example prompts
# MAGIC
# MAGIC Loop a few questions through the endpoint to show consistent, on-topic generation. Swap in
# MAGIC your own domain prompts freely — this is the interactive part of the demo.

# COMMAND ----------

prompts = [
    "In one sentence, what is a large language model?",
    "Explain what retrieval-augmented generation (RAG) is, in three sentences.",
    "What is the difference between fine-tuning and prompting? Keep it brief.",
]

for i, p in enumerate(prompts, 1):
    start = time.time()
    answer = chat(
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": p},
        ],
        max_tokens=120,
        temperature=0.2,
    )
    elapsed = time.time() - start
    print(f"[{i}] Q: {p}")
    print(f"    ({elapsed:.2f}s) A: {answer.strip()}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 6 — The raw REST call (what an external app would use)
# MAGIC
# MAGIC The SDK is convenient inside Databricks, but any downstream application would call the
# MAGIC endpoint over plain HTTPS with a bearer token. This is the exact shape to hand to application
# MAGIC developers. We build the call from the notebook context, then show the equivalent `curl`.
# MAGIC
# MAGIC > In a real application, use a **service principal token stored in a secret scope** — never a
# MAGIC > personal token, and never hard-code it.

# COMMAND ----------

import json
import requests

# Pull the workspace URL and a short-lived token from the running notebook's context. This is the
# standard (if verbose) way to self-authenticate a raw HTTP call from inside a notebook — fine for
# a demo. In a real app, use a service principal token from a secret scope instead (see note above).
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
TOKEN = ctx.apiToken().get()
INVOCATION_URL = f"https://{DATABRICKS_HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations"

payload = {
    "messages": [
        {"role": "user", "content": "Summarize what this endpoint does in one sentence."}
    ],
    "max_tokens": 80,
}

start = time.time()
resp = requests.post(
    INVOCATION_URL,
    headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    data=json.dumps(payload),
    timeout=300,  # generous: covers a cold start if the endpoint scaled to zero
)
elapsed = time.time() - start

print(f"POST {INVOCATION_URL}")
print(f"HTTP {resp.status_code}  ({elapsed:.2f}s)\n")
# This endpoint returns the chat completion wrapped in a top-level JSON list, so unwrap the
# first element before reading choices. (A standard chat endpoint returns the dict directly —
# handle both.)
body = resp.json()
body = body[0] if isinstance(body, list) else body
print(body["choices"][0]["message"]["content"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### The equivalent `curl` — copy/paste for app developers
# MAGIC The same call any external service can make with a workspace token.

# COMMAND ----------

print(f"""
export DATABRICKS_TOKEN="<your service-principal token>"

curl -X POST "{INVOCATION_URL}" \\
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{{
        "messages": [
          {{"role": "user", "content": "What is machine learning?"}}
        ],
        "max_tokens": 80
      }}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done 🎉
# MAGIC
# MAGIC You just queried a governed, UC-registered open-source model over a real-time REST endpoint —
# MAGIC the same interface any downstream application would use.
# MAGIC
# MAGIC - **Cost hygiene:** the endpoint scales to zero on its own; nothing to tear down here.
# MAGIC - **To rebuild or swap the model:** see `01_package_and_serve.py`.
# MAGIC - **To stage a model from Hugging Face into the Volume:** see `00_pull_from_huggingface.py`.
