# Databricks + Microsoft Fabric interop demos

This repository contains three deployed demonstrations of Databricks data in Fabric, with documented limitations:

The [proposed specification](demo-specification-proposed.md) is historical; the [final implementation specification](demos/demo-specification.md) is canonical.

| Demo | Integration pattern | What it verifies |
|---|---|---|
| [OneLake external location](demos/one-lake/README.md) | Databricks Unity Catalog managed Delta tables stored in a Fabric Azure Databricks Storage item, surfaced to Fabric through a mirrored Azure Databricks catalog | Shared storage access, read/write from Databricks, read-only Fabric SQL analytics endpoint, and propagation latency |
| [Publish to Power BI](demos/power-bi/README.md) | Databricks' native **Publish to Power BI** feature | UC primary/foreign-key relationship propagation, semantic-model mode, dimension filtering, and source-change visibility |
| [Compatibility Mode](demos/compatibility-mode/README.md) | OneLake catalog-commit sources, a normal mirrored catalog, and Lakehouse shortcuts to user-approved ADLS compatibility outputs | Four normal tables and five generated outputs queried; the selected normal MV was not exposed by its mirror |

These are not interchangeable paths: the OneLake demo uses shared underlying Delta storage and Fabric's mirrored catalog; the Power BI demo publishes a semantic model from Databricks and does not use Fabric mirroring.

## Status

Demo 1 is deployed: its original baseline deadline failed, a later baseline matched, and the append check passed. Demo 2 is deployed with successful DirectQuery checks and a documented report-slicer evidence gap. Demo 3's compatibility outputs, including its native MV, were read in Fabric SQL and Spark. Normal mirrored tables were readable in SQL but failed in Spark with commit-coordinator errors; the selected normal MV was not exposed by the mirror. See each manifest and runbook for evidence and limitations.

The selected Databricks workspace and Unity Catalog parent catalog are both named `dbxfabricinterop`; use of that catalog is explicitly approved. The example config keeps `parent_catalog_approved` set to `false` as a conservative template default. When preparing a private deployment config, verify the catalog identity and set the flag to `true` to record the existing approval. This does not grant catalog privileges or approve other shared changes, chargeable resources, or deployment. The deployment gate still requires explicit Databricks/Fabric workspace confirmations and per-demo chargeable-resource approvals; the Power BI Fabric and Power BI workspace IDs must match, and its capacity ID must be set. These are operator attestations, not live checks. Never add credentials or secret values to the config.

## Local checks

From the repository root:

```powershell
python -m unittest discover -s tests -v
python -m demos.common.preflight --config demos/config.example.json
```

The second command validates the template and reports deployment blockers without connecting to any service. Add `--require-deployment` when a complete set of IDs and explicit confirmations/approvals is required; that mode exits with code 2 while prerequisites remain unresolved. `CONFIGURED` means only that the supplied local fields pass those gates; it does not establish that workspaces, IDs, permissions, capacity, or approvals were independently verified.

The default shared data set is deterministic: 365 dates, 20 products, 100 customers, 10,000 sales lines, and seed 42. Monetary values use `Decimal`; data validation checks row counts, key uniqueness, non-null keys, foreign-key integrity, exact totals, and aggregates by month, product category, and customer region.

Demo 3 also provides an offline Day × Product × Region × Segment aggregate
reference and comparison helper; its SQL templates cannot be run as written and
are not a rerun-safe deployment mechanism. See each demo's README for its
runbook, manual steps, evidence template, and cleanup boundary.
