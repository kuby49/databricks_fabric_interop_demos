# Live evidence — pending

Do not treat this blank template as evidence. Complete with sanitized actual observations, timestamps and links to access-controlled screenshots/exports. Never include credentials. Record pass/fail/blocked and a concise reason for each check; this template must be completed **before manual model edits**.

## Context and isolation

* Date/time (UTC); operator; environment/home tenant; subscription:
* Databricks workspace ID; approved catalog; schema; SQL warehouse ID:
* Fabric workspace ID; capacity ID; model ID/name; publish timestamp:
* Publish feature/UI path; actual selected tables; mode; authentication method:
* Prerequisite results (including XMLA Read Write, consent, permissions, feature availability, licensing): pending
* Ownership evidence and manifest reconciliation; second-run no duplication/data loss: pending

## Source before publishing

* Source check timestamp, actual row counts vs configured counts:
* Null/duplicate PKs and null/orphan FKs for each table/key (expected zero):
* `SHOW CREATE TABLE` output/evidence for four PKs and three FKs:
* Source grand total, date-month, category, region groups and selected combined-filter SQL/output:
* Status/reason: pending

## Model metadata — pre-edit only

* Captured at UTC; inspection tool and sanitized evidence location:
* Actual model tables and per-table storage modes:
* Extra/missing tables; schema-selection proof:

| Observed source table.column | Observed target table.column | Observed cardinality | Active? | Observed cross-filter direction | Evidence / result |
|---|---|---|---|---|---|
| pending | pending | pending | pending | pending | expected FactSales.DateKey → DimDate.DateKey |
| pending | pending | pending | pending | pending | expected FactSales.ProductKey → DimProduct.ProductKey |
| pending | pending | pending | pending | pending | expected FactSales.CustomerKey → DimCustomer.CustomerKey |

* Any additional or ambiguous relationships:
* Preservation objective status/reason: pending. Missing or inactive relationships must not be reported as automatic success.
* Manual edit timestamp, if any, and separately labeled remediation:

## Report and change validation

* Report ID, validation timestamp; total SQL and report results:
* Individual date/category/region selections and SQL vs report values:
* Combined selection and SQL vs report values:
* Status/reason: pending
* Batch key range, collision query, committed row count and commit timestamp:
* Post-change source counts, integrity checks, total and selected-filter results:
* Mode; explicit refresh status/start/end for Import OR fresh-query/visual action for DirectQuery:
* Original deadline/timeout; first matching query timestamp and elapsed seconds; actual downstream result:
* Visibility status/reason (including any error, unexpected republishing or cache observations): pending
