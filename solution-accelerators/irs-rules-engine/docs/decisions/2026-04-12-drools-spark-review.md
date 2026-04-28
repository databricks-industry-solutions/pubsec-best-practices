> **Status:** Historical record. Issues documented here were resolved in commits up to `cf32537` (2026-04-16). Kept for traceability; do not modify.

# Drools DMN on Spark — Technical Review

Reviewed: 2026-04-12  
Notebook: `notebooks/03_drools_evaluate.py`  
Cluster: DBR 16.2, Java 17  
Drools: 8.30.0.Final (notebook header) / 8.44.0.Final (shaded JAR build)

---

## Summary

The batch scoring pipeline is stalling. The root cause is not a single bug but a
combination of four independent issues that compound each other. Three of them
produce silent errors (swallowed by the `catch (Exception e) → -1.0` handler),
making the job appear to hang rather than fail loudly.

---

## Issue 1 — `DMNCompilerImpl` is not thread-safe (HIGH)

`DMNCompilerImpl` holds a mutable `drgCompilers` Deque and `afterDRGcallbacks` List
with no synchronization. When the UDF runs multiple concurrent Spark tasks on the
same executor JVM (the default), threads race on this shared mutable state.

**Failure mode:** `ConcurrentModificationException` during `compile()`, silently
caught, UDF returns `-1.0`. Rows score as ERROR. Does not surface as a job failure.

**How to confirm:** After a run, check:
```sql
SELECT count(*) FROM services_bureau_catalog.irs_rrp.scored_results
WHERE audit_score < 0
```
A non-zero count here means the catch block is swallowing real errors.

**Fix:** Each invocation of `compile()` must use its own `DMNCompilerImpl` instance.
The correct Spark pattern is `@transient lazy val` on an executor-scoped singleton
object. Do not create a new compiler instance per row — compile once per executor
JVM and cache the resulting `DMNModel`:

```scala
object DroolsHolder extends Serializable {
  // dmnXmlBroadcast must be set on the driver before the UDF is registered
  var dmnXmlBroadcast: Broadcast[String] = _

  @transient private lazy val _model: DMNModel =
    new DMNCompilerImpl().compile(new StringReader(dmnXmlBroadcast.value), null)

  @transient private lazy val _runtime: DMNRuntimeImpl =
    new DMNRuntimeImpl(null)

  def evaluate(ctx: DMNContext): DMNResult =
    _runtime.evaluateAll(_model, ctx)
}
```

`@transient lazy val` is the canonical Spark idiom: `@transient` tells Java
serialization to skip the field (so the object itself stays serializable),
and `lazy val` ensures initialization happens once on first access on each
executor JVM — not once per row.

---

## Issue 2 — ANTLR version conflict (CRITICAL — version-dependent)

Drools uses ANTLR4 to parse FEEL expressions at compile time. The version bundled
with Drools must match the ANTLR4 runtime on the Databricks classpath.

| Drools Version | Bundled ANTLR4 | DBR 16.2 ANTLR4 | Compatible |
|---|---|---|---|
| 8.30.0.Final | 4.9.2 | 4.9.3 | ✅ (same ATN wire format) |
| 8.44.0.Final | 4.10.1 | 4.9.3 | ❌ (ATN format changed) |

With Drools 8.44.0.Final the error is:
```
ATN with version 4 (expected 3)
UnsupportedOperationException: Could not deserialize ATN
```
This causes all FEEL expression evaluation to fail. The `catch (Exception e)`
block swallows it and returns `-1.0`. Every row errors silently.

**How to confirm:** Check which version is actually installed on the cluster —
the notebook header says 8.30.0 but the shaded JAR build (`build_shaded_jar.py`)
targets 8.44.0. If both JARs are on the classpath, 8.44.0 wins and breaks things.

**Fix options:**

1. **Pin to 8.30.0.Final** (simplest) — remove 8.44.0 from the cluster library
   list and the shaded JAR build. 8.30.0 is confirmed compatible with DBR 16.x.

2. **Shade ANTLR if 8.44.0 is required** — relocate `org.antlr` in the shaded JAR:
   ```xml
   <relocation>
     <pattern>org.antlr</pattern>
     <shadedPattern>com.irs.shaded.org.antlr</shadedPattern>
   </relocation>
   ```
   The shaded JAR must include its own ANTLR4 runtime under the relocated namespace.
   Databricks classpath ordering cannot override bundled Spark libraries, so shading
   is the only reliable fix if upgrading Drools beyond 8.30.

---

## Issue 3 — Shared cluster sandbox blocks Drools bytecode generation (HIGH)

`DMNCompilerImpl` uses ASM/cglib to generate Java bytecode for decision table
evaluators at compile time. On Databricks **Standard (Shared) access mode**,
executor sandboxing restricts `ClassLoader.defineClass()`. If Drools hits this
restriction, `compile()` hangs or throws a security exception — again swallowed
by the catch block.

This is the most likely cause of the hang on a clean run with no prior errors
in the log.

**How to confirm:** Check the cluster access mode in the Databricks UI (Compute →
cluster → Access mode). If it shows "Standard" or "Shared", this is the issue.

**Fix:** Switch to **Single User** access mode. Drools' classloader usage is
incompatible with the shared executor sandbox. This is not a workaround —
shared mode is documented as blocking arbitrary classloader operations.

---

## Issue 4 — Java 17 module access flags missing (MEDIUM)

DBR 16.2 runs Java 17, which enforces strong module encapsulation by default.
Drools uses reflection to access `java.lang` and `java.util` internals during
bytecode generation. Without explicit `--add-opens` flags, this throws
`InaccessibleObjectException` — swallowed by the catch block, returns `-1.0`.

**How to confirm:** Check the cluster's Spark config (Compute → cluster →
Advanced options → Spark config) for these flags:
```
spark.driver.extraJavaOptions   --add-opens java.base/java.lang=ALL-UNNAMED ...
spark.executor.extraJavaOptions --add-opens java.base/java.lang=ALL-UNNAMED ...
```
If they are absent, add them.

**Required flags:**
```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
```
Add to both `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`
in the cluster Spark config.

---

## Diagnostic Checklist

Run through these before touching the notebook code. Most of the issues above
manifest as silent `-1.0` returns, not as loud job failures.

- [ ] **Cluster access mode** — Single User or Shared? If Shared: switch to Single User.
- [ ] **Drools version on cluster** — 8.30.0 or 8.44.0? Run `dbutils.library.list()` or check the cluster Libraries tab. Remove 8.44.0 if present alongside 8.30.0.
- [ ] **Java 17 `--add-opens` flags** — present in both `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`?
- [ ] **Error rows in output** — `SELECT count(*) FROM scored_results WHERE audit_score < 0`. Non-zero = silent failures in the catch block.
- [ ] **Job failure mode** — fails at task submission (`Task not serializable`) or hangs mid-execution? Submission failure = driver-side object captured in closure. Mid-execution hang = executor init problem (Issues 3 or 4).

---

## Priority Order

| # | Issue | Impact | Effort |
|---|-------|--------|--------|
| 1 | Switch cluster to Single User mode | Likely unblocks the hang immediately | 2 minutes |
| 2 | Confirm Drools version (remove 8.44 if present) | Eliminates ANTLR silent failures | 5 minutes |
| 3 | Add Java 17 `--add-opens` flags to cluster config | Fixes reflective access errors | 5 minutes |
| 4 | Refactor UDF to `@transient lazy val` pattern | Fixes thread-safety + per-row compile cost | 30 minutes |

Do 1–3 first (cluster config changes, no code) before touching the notebook.
If the job works after those three, Issue 4 is still worth fixing for performance
at 100K scale — per-row compilation adds 50–500ms overhead per row even when it
succeeds.
