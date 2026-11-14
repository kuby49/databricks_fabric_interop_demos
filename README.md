# Databricks + Microsoft Fabric interop demos

This repository contains two independent demonstrations of Databricks data in Fabric:

The [latest proposed specification](demo-specification-proposed.md) is preserved at the repository root; the [finalized implementation specification](demos/demo-specification.md) organizes its requirements for this codebase.

| Demo | Integration pattern | What it verifies |
|---|---|---|
| [OneLake external location](demos/one-lake/README.md) | Databricks Unity Catalog managed Delta tables stored in a Fabric Azure Databricks Storage item, surfaced to Fabric through a mirrored Azure Databricks catalog | Shared storage access, read/write from Databricks, read-only Fabric SQL analytics endpoint, and propagation latency |
| [Publish to Power BI](demos/power-bi/README.md) | Databricks' native **Publish to Power BI** feature | UC primary/foreign-key relationship propagation, semantic-model mode, dimension filtering, and source-change visibility |

These are not interchangeable paths: the OneLake demo uses shared underlying Delta storage and Fabric's mirrored catalog; the Power BI demo publishes a semantic model from Databricks and does not use Fabric mirroring.

## Status

The local source, shared deterministic star-schema generator, and local checks are prepared here. No Azure, Databricks, Fabric, or Power BI resource has been created, changed, deployed, or tested by this work. Live execution remains gated on independently verified prerequisites and explicit approval for any additional chargeable or shared resources.

The selected Databricks workspace and Unity Catalog parent catalog are both named `dbxfabricinterop`. The catalog selection is not approval to use it: `parent_catalog_approved` remains `false` in [`demos/config.example.json`](demos/config.example.json) until separately approved. Before deployment, copy the template to a private local config file, verify the workspace/catalog identifiers, and set that flag only after explicit approval. The deployment gate also requires explicit Databricks/Fabric workspace confirmations and per-demo chargeable-resource approvals; the Power BI Fabric and Power BI workspace IDs must match, and its capacity ID must be set. These are operator attestations, not live checks. Never add credentials or secret values to the config.

## Local checks

From the repository root:

```powershell
python -m unittest discover -s tests -v
python -m demos.common.preflight --config demos/config.example.json
```

The second command validates the template and reports deployment blockers without connecting to any service. Add `--require-deployment` when a complete set of IDs and explicit confirmations/approvals is required; that mode exits with code 2 while prerequisites remain unresolved. `CONFIGURED` means only that the supplied local fields pass those gates; it does not establish that workspaces, IDs, permissions, capacity, or approvals were independently verified.

The default shared data set is deterministic: 365 dates, 20 products, 100 customers, 10,000 sales lines, and seed 42. Monetary values use `Decimal`; data validation checks row counts, key uniqueness, non-null keys, foreign-key integrity, exact totals, and aggregates by month, product category, and customer region.

See each demo's README for its runbook, manual steps, evidence template, and cleanup boundary.
