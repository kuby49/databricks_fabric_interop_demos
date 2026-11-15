# Databricks + Microsoft Fabric interop demos

This repository contains four independently selectable Databricks/Fabric demonstrations, with documented limitations:

The [proposed specification](demo-specification-proposed.md) is historical; the [final implementation specification](demos/demo-specification.md) is canonical.

| Demo | Integration pattern | What it verifies |
|---|---|---|
| [OneLake external location](demos/one-lake/README.md) | Databricks Unity Catalog managed Delta tables stored in a Fabric Azure Databricks Storage item, surfaced to Fabric through a mirrored Azure Databricks catalog | Shared storage access, read/write from Databricks, read-only Fabric SQL analytics endpoint, and propagation latency |
| [Publish to Power BI](demos/power-bi/README.md) | Databricks' native **Publish to Power BI** feature | UC primary/foreign-key relationship propagation, semantic-model mode, dimension filtering, and source-change visibility |
| [Compatibility Mode](demos/compatibility-mode/README.md) | One normal OneLake-backed UC schema (four tables + one native MV), a mirror and Lakehouse shortcuts to five ADLS outputs | Setup and resolved targets checked separately; optional SQL/Spark checks need cost consent |
| [OneLake catalog federation](demos/onelake-federation/README.md) | Fabric Lakehouse Delta tables queried directly by Databricks through a workspace-specific OneLake connection and independent UC foreign catalog | Read-only SQL/Spark access and controlled Fabric append visibility; see live evidence for actual status |

These are not interchangeable paths: the OneLake demo uses shared underlying Delta storage and Fabric's mirrored catalog; the Power BI demo publishes a semantic model from Databricks and does not use Fabric mirroring.

## Status

Demo 1 is deployed: its original baseline deadline failed, a later baseline matched, and the append check passed. Demo 2 is deployed with successful DirectQuery checks and a documented report-slicer evidence gap. Demo 3's single-source outputs and ten shortcuts are setup-verified; obsolete compatibility schema/location and canary cleanup are complete. Normal source data, shared resources and historical ADLS outputs are preserved. The native MV is now mirrored to its generated output. Earlier two-producer SQL/Spark results are historical; no new end-to-end result is claimed. See each manifest and runbook for evidence and limitations.

Demo 4 is deployed in MCAP. Direct foreign-catalog SQL and standard Spark reads matched every baseline row and the one controlled 10-row Fabric append. Observed append visibility upper bounds were 166.18 seconds for SQL and 816.05 seconds for Spark, including cold start, within the recorded 30-minute budget. See its [deployment evidence](demos/onelake-federation/deployment-manifest.json) and [editable architecture](demos/onelake-federation/architecture.mmd). Resources are retained; no automatic cleanup was performed.

The historical selected Databricks workspace and Unity Catalog parent catalog are both named `dbxfabricinterop`; use of that catalog was explicitly approved for the recorded deployment. The example config keeps `parent_catalog_approved` set to `false` as a conservative template default. When preparing a private deployment config, verify the catalog identity and set the flag to `true` to record the existing approval. This does not grant catalog privileges or approve other shared changes, chargeable resources, or deployment. The deployment gate still requires explicit Databricks/Fabric workspace confirmations and per-demo chargeable-resource approvals; the Power BI Fabric and Power BI workspace IDs must match, and its capacity ID must be set. These are operator attestations, not live checks. Never add credentials or secret values to the config.

## Guided deployment

Use the [cross-platform guided deployer](demos/deployment/README.md) for a fresh,
independent deployment of one demo or all four:

```text
python -m pip install -r requirements-deploy.txt
python -m demos.deploy --demo all --action configure
python -m demos.deploy --action preview
python -m demos.deploy --action apply
```

Configuration is offline. Live preview/deployment needs separate approval and
sign-in; the CLI never changes your tenant configuration. It prompts for
create/reuse capacity and Databricks infrastructure, independent workspaces and
all demo-specific prerequisites. Required initial loading and refreshes can incur
charges; cloud writes require an explicit preview confirmation. Manual actions
pause at numbered, linked, resumable checkpoints. Default verification checks
resource/checkpoint state only; optional data/report checks require separate
cost consent. Keep the ignored private config/state for safe
`--action resume`; there is no automatic overwrite, replay or cleanup.

The deployer has offline simulated-service coverage, not live compatibility
certification. Historical names, approvals and manifests are not portable
defaults or approval to deploy again.

Demo 4 reverses the data direction: Fabric produces the shared deterministic
sales schema and Databricks reads it without copying. Select `--demo federation`;
the foreign catalog is a new top-level UC object, never nested in an existing
parent catalog. Its [manual gates and evidence](demos/onelake-federation/README.md)
distinguish setup-only verification from separately confirmed SQL/Spark/append checks.

## Offline preparation checks

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
