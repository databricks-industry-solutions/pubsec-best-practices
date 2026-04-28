# Decisions

This folder holds architecture decision records (ADRs) and historical
technical reviews for the IRS Rules Engine project. Files here are kept
for audit trail and traceability; they are not living documentation.

## Contents

- [`2026-04-07-drools-vs-spiffworkflow.md`](2026-04-07-drools-vs-spiffworkflow.md)
  (2026-04-07) — Decision record comparing Drools and SpiffWorkflow for
  the IRS Blaze Advisor migration. Drools selected.

- [`2026-04-12-drools-spark-review.md`](2026-04-12-drools-spark-review.md)
  (2026-04-12) — Technical review of the Drools DMN on Spark integration.
  Issues documented here were resolved in commits up to `cf32537`.

- [`2026-04-27-multi-rule-set-design-space.md`](2026-04-27-multi-rule-set-design-space.md)
  (2026-04-27) — **Decision pending.** Three result-shape options
  (per-rule-set tables, unified table with `rule_set_name`, hybrid view)
  and three orchestration options for running multiple rule sets
  concurrently. Awaiting analyst/dashboard consumption signal before
  committing.
