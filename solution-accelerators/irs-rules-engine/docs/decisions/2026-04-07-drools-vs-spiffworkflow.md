> **Status:** Decision record. Drools was selected; SpiffWorkflow was evaluated and rejected. Kept for audit trail.

# SpiffWorkflow vs Drools: Honest Technical Gap Analysis

**Date:** 2026-04-07
**Purpose:** Determine whether SpiffWorkflow can replace Drools for IRS Blaze Advisor migration

---

## Bottom Line Up Front

**SpiffWorkflow is not ready for this.** It has a DMN parser and a basic evaluation engine, but critical DMN features are either missing or commented out in the source code. For a production Blaze Advisor replacement, Drools (via Kogito) remains the credible choice. SpiffWorkflow could work as a lightweight Python-native option for simple decision tables, but it has real gaps that would bite the IRS use case.

---

## What I Actually Tested

Installed SpiffWorkflow 3.1.2, read the engine source, attempted to parse and evaluate our IRS tax review DMN file. Here's what I found:

### 1. Hit Policies — Only 2 of 9 implemented

The DMN spec defines 9 hit policies. SpiffWorkflow implements **2**:

| Hit Policy | DMN Spec | Drools | SpiffWorkflow |
|------------|----------|--------|---------------|
| **UNIQUE** | ✅ | ✅ | ✅ |
| **FIRST** | ✅ | ✅ | ❌ (commented out) |
| **PRIORITY** | ✅ | ✅ | ❌ (commented out) |
| **ANY** | ✅ | ✅ | ❌ (commented out) |
| **COLLECT** | ✅ | ✅ | ✅ (partial) |
| **RULE ORDER** | ✅ | ✅ | ❌ (commented out) |
| **COLLECT + SUM** | ✅ | ✅ | ❌ (commented out) |
| **COLLECT + COUNT** | ✅ | ✅ | ❌ (commented out) |
| **COLLECT + MIN/MAX** | ✅ | ✅ | ❌ (commented out) |

The source literally has `# SUM = "SUM"`, `# COUNT = "COUNT"`, `# FIRST = "FIRST"` etc. — all commented out.

**Impact for IRS:** Our scoring pattern uses COLLECT + SUM (additive scoring). SpiffWorkflow's COLLECT returns a **dict of lists** — each output column becomes a list of matched values. You'd have to sum them yourself in Python. That's workable but means you're writing glue code that Drools/Kogito handles natively.

### 2. Expression Language — Python, not FEEL

SpiffWorkflow uses **Python syntax** exclusively for DMN expressions. The DMN standard uses FEEL (Friendly Enough Expression Language).

- SpiffWorkflow: `> 500000` works (Python comparison), `"MFS"` works (Python string)
- Drools: Uses FEEL natively — this is what Blaze Advisor exports to DMN

**Impact for IRS:** If Blaze exports FEEL expressions in DMN files, they'd need translation to Python syntax before SpiffWorkflow can evaluate them. For simple numeric comparisons (`> 500000`, `< 25`) this is trivial — the syntax is identical. For complex FEEL (date ranges, list operations, `between`, `in`), it's a real conversion effort.

Drools handles FEEL natively. Zero conversion.

### 3. Standalone DMN Evaluation — Not Really Supported

SpiffWorkflow's DMN engine is **tightly coupled to BPMN**. The `DMNEngine.evaluate()` method requires a `task` object (a BPMN task with a workflow and script engine attached). You can't just hand it a DMN file and a dict of facts.

To evaluate a DMN table, you must:
1. Create a BPMN process with a Business Rule Task
2. Link the Business Rule Task to the DMN file
3. Run the BPMN process with input data
4. Extract the output from the completed task

Drools/Kogito: Drop a `.dmn` file in the project, get a REST endpoint. Or use the Java API directly: `dmnRuntime.evaluateAll(model, context)`.

**Impact for IRS:** Building a standalone DMN evaluator service with SpiffWorkflow requires wrapping the BPMN/DMN coupling in boilerplate. It's possible but awkward. Drools gives you this for free.

### 4. DMN Conformance Level

The DMN spec defines three conformance levels:
- **Level 1**: Decision tables with simple inputs/outputs
- **Level 2**: Level 1 + S-FEEL (simplified FEEL)
- **Level 3**: Full FEEL, DRDs (Decision Requirement Diagrams), BKMs (Business Knowledge Models)

| Feature | Drools | SpiffWorkflow |
|---------|--------|---------------|
| Decision Tables | ✅ Full | ✅ Basic |
| S-FEEL | ✅ | ❌ (uses Python) |
| Full FEEL | ✅ | ❌ |
| DRDs | ✅ | ❌ |
| BKMs | ✅ | ❌ |
| Invocations | ✅ | ❌ |
| Contexts | ✅ | ❌ |
| Boxed Expressions | ✅ | ❌ |

Drools is **DMN Level 3 conformant** — the gold standard. SpiffWorkflow is roughly **Level 1 with caveats**.

### 5. Maturity and Adoption

| Metric | Drools | SpiffWorkflow |
|--------|--------|---------------|
| First release | 2001 | 2020 (DMN support) |
| GitHub stars | ~5.8k | ~1.7k |
| Backing org | Red Hat / Apache KIE | Sartography (small company) |
| Enterprise adoption | Banks, insurance, government | University of Virginia, small orgs |
| FedRAMP / GovCloud | Red Hat has FedRAMP products | No government certifications |
| Commercial support | Red Hat / IBM | Sartography (limited) |
| Test suite | Thousands of DMN TCK tests | Basic unit tests |

### 6. What SpiffWorkflow Does Well

- **Pure Python** — `pip install`, no JVM, runs anywhere Python runs
- **BPMN + DMN integration** — if you want full workflow orchestration with decision tables embedded, it's a solid choice
- **Low-code philosophy** — designed for business analysts to draw workflows, developers to fill in the code
- **Minimal dependencies** — just `lxml`

---

## Recommendation

### For the IRS Demo (Proving the Concept)

**Keep ZEN Engine for the demo app.** It works, it's deployed, it's tested. Add a DMN import feature that parses DMN XML and converts to JDM — this proves the migration path conceptually without committing to an engine swap.

### For the Production Architecture Conversation

**Drools/Kogito is the answer.** Here's why:
1. **Native DMN execution** — Blaze rules export to DMN, Drools runs them unchanged
2. **FEEL support** — no expression translation needed
3. **Full DMN conformance** — DRDs, BKMs, invocations, all of it
4. **Enterprise pedigree** — Red Hat backing, government adoption, 20+ years
5. **Deployable on Databricks** — as a Model Serving endpoint or Databricks App (containerized Quarkus/Kogito service)

### For SpiffWorkflow

**Not appropriate for this use case.** It's a workflow engine that happens to have basic DMN support, not a DMN engine. The missing hit policies, lack of FEEL, and BPMN coupling make it a poor choice for a Blaze Advisor replacement where DMN conformance matters. It could be useful if the IRS wanted to orchestrate multi-step review processes (BPMN workflows) with embedded decision points, but that's a different conversation.

---

## The Path Forward

1. **Phase 1 (now):** Ship the demo with ZEN Engine + DMN import capability. Proves the UI, scoring, governance, migration story.
2. **Phase 2 (next):** Build a Kogito/Drools DMN microservice in a Docker container. Deploy as a Databricks App or Model Serving endpoint. The Streamlit UI calls it over HTTP.
3. **Phase 3 (production):** IRS exports Blaze rules as DMN → loads into Kogito → evaluated on Databricks → results flow to Delta Lake → Unity Catalog governance.

The demo app we built is Phase 1. It proves the user experience. Kogito is Phase 2 — it proves the engine. They're complementary, not competing.
