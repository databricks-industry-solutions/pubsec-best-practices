# Databricks notebook source
# MAGIC %md
# MAGIC # Pull a model from Hugging Face into a Unity Catalog Volume
# MAGIC ### The Hugging Face *acquisition* step — one of several ways to fill the Volume
# MAGIC
# MAGIC `01_package_and_serve.py` always loads the model **from a Unity Catalog Volume** — it doesn't
# MAGIC care how the files got there. That makes "where the model comes from" a **separate, pluggable
# MAGIC concern**. This notebook is the **Hugging Face acquisition recipe**: it downloads a model from
# MAGIC HF and writes it into the Volume in the layout `01` expects.
# MAGIC
# MAGIC ```
# MAGIC   [ 00 this notebook: HF -> Volume ]  ->  [ 01_package_and_serve: Volume -> MLflow -> UC -> Serving ]
# MAGIC          (acquisition, pluggable)              (packaging/governance/serving, fixed)
# MAGIC ```
# MAGIC
# MAGIC **Use this notebook when** you have (or have been granted) Hugging Face access from the
# MAGIC workspace. If your environment can't reach HF, this is the step you replace — download on an
# MAGIC approved connected machine and move the files into the Volume through your approved transfer
# MAGIC path. **The Volume layout below is the contract; how the bytes arrive is your call.**
# MAGIC
# MAGIC > **Air-gapped note:** an air-gapped workspace has no Hugging Face egress, so you would NOT
# MAGIC > run this notebook there. Run it in a connected workspace that shares the metastore/Volume,
# MAGIC > or use the manual-transfer pattern. Either way, `01` downstream is identical.

# COMMAND ----------

# MAGIC %pip install --upgrade "huggingface_hub>=0.26.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1 — Configuration
# MAGIC
# MAGIC The **catalog / schema / model name must match `01_package_and_serve.py`** so the Volume path
# MAGIC lines up. `VOLUME_MODEL_PATH` here is exactly the `VOLUME_MODEL_PATH` that `01` loads from.

# COMMAND ----------

# ---- Parameters (widgets render as a form at the top of the notebook) -------
# Set these in the input fields at the top. Catalog / schema / model name MUST match
# 01_package_and_serve.py so the Volume path lines up. Use a catalog whose bucket permits writes
# (see the note in 01 Stage 1). `catalog` has no default — set it to a catalog you can write to.
dbutils.widgets.text("hf_model_id", "microsoft/Phi-4-mini-instruct", "Hugging Face model id")
dbutils.widgets.text("catalog", "", "Catalog (writable) — required")
dbutils.widgets.text("schema", "os_llm_serving", "Schema")
dbutils.widgets.text("model_name", "phi4_mini_instruct", "Model name (Volume folder)")

HF_MODEL_ID = dbutils.widgets.get("hf_model_id").strip()   # must be a causal-LM / chat model
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
MODEL_NAME = dbutils.widgets.get("model_name").strip()
if not (HF_MODEL_ID and CATALOG and SCHEMA and MODEL_NAME):
    raise ValueError("Set the hf_model_id / catalog / schema / model_name widgets at the top.")

VOLUME_NAME = "models"
# This is the VOLUME_MODEL_PATH the main notebook loads from.
VOLUME_MODEL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{MODEL_NAME}"

print(f"Will pull {HF_MODEL_ID}")
print(f"        -> {VOLUME_MODEL_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2 — Create the schema / volume if needed
# MAGIC
# MAGIC We assume `CATALOG` already exists and is writable; we create only the schema + volume under
# MAGIC it. Safe to re-run.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
print(f"Volume ready: /Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3 — Download the model snapshot directly into the Volume
# MAGIC
# MAGIC `snapshot_download` pulls the full model repo (weights, config, tokenizer, and any custom
# MAGIC modeling code) and writes it to the Volume path. **This is the only step that needs Hugging
# MAGIC Face access** — everything downstream (`01`) is offline.
# MAGIC
# MAGIC > **Gated models (e.g. Llama):** Phi-4-mini is MIT-licensed and not gated, so no token is
# MAGIC > needed. For a gated model, set `HF_TOKEN` from a secret scope first (commented line below).

# COMMAND ----------

from huggingface_hub import snapshot_download

# For a GATED model, uncomment and set a token from a secret scope first:
# import os; os.environ["HF_TOKEN"] = dbutils.secrets.get("<your-scope>", "hf_token")

local_dir = snapshot_download(
    repo_id=HF_MODEL_ID,
    local_dir=VOLUME_MODEL_PATH,
    # Skip duplicate weight formats to save space; keep safetensors + all config/code.
    ignore_patterns=["*.pth", "*.onnx", "*.msgpack", "*.h5"],
)

print(f"Downloaded to: {local_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4 — Verify the staged files (the Volume "contract")
# MAGIC
# MAGIC `01` needs, at minimum: a `config.json`, tokenizer files, and at least one weight shard
# MAGIC (`*.safetensors`). We assert those are present so a downstream load in `01` won't fail
# MAGIC halfway. This same check defines what any *other* acquisition method must produce.

# COMMAND ----------

files = [f.name for f in dbutils.fs.ls(VOLUME_MODEL_PATH)]

print(f"{len(files)} files staged in {VOLUME_MODEL_PATH}:")
for f in sorted(files):
    print("  ", f)

# Sanity check: config + tokenizer + at least one weight shard must exist.
required_hints = ["config.json", "tokenizer"]
has_weights = any(f.endswith(".safetensors") or f.endswith(".bin") for f in files)
missing = [h for h in required_hints if not any(h in f for f in files)]
assert not missing and has_weights, f"Staging incomplete — missing {missing}, weights={has_weights}"

print("\n✅ Volume is populated and valid.")
print(f"   Next: open 01_package_and_serve.py, confirm its CATALOG/SCHEMA/MODEL_NAME match, and run it.")
