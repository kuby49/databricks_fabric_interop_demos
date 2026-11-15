# Databricks + Fabric interoperability demos

**Status:** Canonical implementation specification. All three demos have deployed results and documented limitations; demo 3's selected normal MV was not exposed by its mirror. See each manifest and runbook. Resource changes require the applicable deployment approval.

## Objective

Deliver three demonstrations with distinct integration patterns:

1. **OneLake external location:** Azure Databricks Unity Catalog managed Delta tables use OneLake storage through a Fabric **Azure Databricks Storage** item. Fabric reads those same underlying tables through a **mirrored Azure Databricks catalog** and its read-only SQL analytics endpoint.
2. **Publish to Power BI:** Databricks' native **Publish to Power BI** feature publishes a UC star schema to a home-tenant Power BI workspace. The demo observes whether declared UC relationships are preserved before any manual model edits.
3. **Compatibility Mode:** compare OneLake catalog-commit managed sources exposed by a normal mirrored catalog with separately generated read-only compatibility outputs exposed through Lakehouse shortcuts in Fabric SQL and Spark. OneLake output was attempted first; the user approved ADLS output as a fallback and direct output shortcuts in place of the second mirror.

The first path demonstrates shared underlying storage with a catalog mirror; the second publishes a Power BI semantic model. Neither may be represented as the other.

## Defaults, isolation, and approval

For fresh deployments, the [guided cross-platform deployer](deployment/README.md)
asks for tenant/subscription/region, create/reuse capacity and Databricks workspace,
independent Fabric workspaces/UC namespaces, compute, identity/connection/storage
settings and demo-specific prerequisites. It has no historical tenant/resource
defaults. Sharing infrastructure requires an explicit selection; all demos also
deploy separately. New cloud writes, required initial loading and MV/compatibility
refreshes require a cost-bearing preview/confirmation. Default verification checks
resource identities/definitions and checkpoint state; end-to-end data/report/
append/propagation checks are separately confirmed and their manual evidence is
labeled. Numbered, officially linked manual gates cover shared/admin settings,
native publishing and unverifiable location history/provenance. Preserve exact
non-secret IDs/ownership/progress for safe resume; do not replay uncertain writes.

The following defaults and approvals apply to the **historical recorded
deployment**, not to new guided-deployer runs:

| Setting | Default |
|---|---|
| Environment | MCAP |
| Databricks workspace | `dbxfabricinterop` |
| Selected and approved parent catalog | `dbxfabricinterop` |
| Fabric capacity | `fabricadbmirror` |
| Deployment prefix | `fabricinterop` |
| Verification timeout | 900 seconds |
| Data seed | 42 |

Each demo owns its own UC schema and Fabric workspace. All generated tables, notebooks, semantic models, reports, and related artifacts are independently named. Use verified IDs as well as display names.

Use of the selected parent catalog `dbxfabricinterop` is explicitly approved. The example configs retain `parent_catalog_approved: false` as conservative template defaults; after verifying the catalog identity, set the flag to `true` in the private deployment config to record this approval. This approval covers use of the named catalog only: it does not grant catalog privileges, authorize shared-resource changes, or approve chargeable compute or deployment. Neither the active/default catalog nor an existing resource may be selected implicitly or adopted based on a matching name. Additional shared identity, infrastructure, settings, permissions, or chargeable compute require separate verification and approval. Do not modify existing workloads or capacity settings.

Configuration templates contain secret references only, never tokens or secret values. Reruns may reuse resources only when ownership is established by that demo's own manifest. Cleanup is demo-scoped, manifest-bound, confirmation-gated, and never automatic.

## Shared sample model

All demos use the same deterministic, dependency-free generator and independent physical tables and rows. Demo 3 starts from the original baseline rather than the appended live contents of demos 1 and 2:

| Table | Grain | Primary key | Foreign keys |
|---|---|---|---|
| `DimDate` | One row per calendar date | `DateKey` | — |
| `DimProduct` | One row per product | `ProductKey` | — |
| `DimCustomer` | One row per customer | `CustomerKey` | — |
| `FactSales` | One row per sales order line | `SalesLineKey` | `DateKey`, `ProductKey`, `CustomerKey` |

Dimensions include calendar year/month, product category, and customer region. `FactSales` includes quantity and decimal unit/sales amounts. Defaults are 365 dates, 20 products, 100 customers, 10,000 sales lines, and seed 42. Counts, seed, and date origin are configurable. Generated dimensions and facts must have unique non-null keys and no orphaned foreign keys. Exact monetary totals and aggregates by month, category, and region are the cross-engine reference.

| Table | Columns and types |
|---|---|
| `DimDate` | `DateKey INT`, `Date DATE`, `CalendarYear INT`, `CalendarMonth INT`, `MonthName STRING`, `DayOfMonth INT`, `IsWeekend BOOLEAN` |
| `DimProduct` | `ProductKey INT`, `ProductName STRING`, `Category STRING`, `UnitPrice DECIMAL(12,2)` |
| `DimCustomer` | `CustomerKey INT`, `CustomerName STRING`, `Region STRING`, `Segment STRING` |
| `FactSales` | `SalesLineKey BIGINT`, `DateKey INT`, `ProductKey INT`, `CustomerKey INT`, `Quantity INT`, `UnitPrice DECIMAL(12,2)`, `SalesAmount DECIMAL(14,2)` |

Declare primary and foreign keys in Unity Catalog where supported. These constraints are informational and do not replace independent data validation; `NOT NULL` is enforced. Define keys in table DDL or add them after creation because CTAS does not support the constraint clause.

## Demo 1: OneLake external location

### Required sequence

1. Create the dedicated Fabric workspace and Azure Databricks Storage item only after prerequisites and resource approvals are complete. Configure the documented OneLake workspace setting and storage-item security role.
2. Configure a supported Databricks storage credential and UC external location with the documented ID-based URL:
   `abfss://<WorkspaceID>@onelake.dfs.fabric.microsoft.com/<DatabricksStorageID>/Files/`
3. Give only the selected Databricks managed identity or service principal membership in the storage item's OneLake `ReadWrite` security role. Grant the identity the required Fabric workspace access. Do not grant unrelated identities direct OneLake access.
4. Create the dedicated schema under the explicitly approved parent catalog with its managed location set to the external location. Create and populate the four managed Delta tables there.
5. Validate Databricks reads, row counts, exact decimal totals, and aggregates. Append a deterministic batch through Databricks.
6. Create the Fabric mirrored Azure Databricks catalog in the dedicated Fabric workspace. Read all four underlying tables and the appended batch through its SQL analytics endpoint. Record actual visibility latency within the configured timeout.

Do not read directly from the storage item in Fabric or describe mirroring as a second data copy. The mirror exposes the UC catalog and accesses the underlying data through shortcuts. Mirrored reads are read-only and propagation may take seconds to several minutes.

### Prerequisite gate

Before any deployment, verify the active tenant, workspace/capacity IDs, ownership and permission scope, authentication, network access, and feature availability. Confirm Unity Catalog and the OneLake external-location Public Preview are enabled and compute is serverless or Databricks Runtime 18.1+. Check the two OneLake SAS controls separately: tenant-level **Use short-lived user-delegated SAS tokens** for key generation, and the target workspace's **Authenticate with OneLake user-delegated SAS tokens** setting. A disabled tenant authentication default may be overridden by a workspace admin if delegation permits; do not change tenant-wide settings without separate approval. Also confirm the Fabric tenant allows Azure Databricks Storage items and the selected identity's role membership and storage credential are correct.

Schema creation with `MANAGED LOCATION` requires `USE CATALOG`, `CREATE SCHEMA`, and `CREATE MANAGED STORAGE` on the backing external location. The Fabric mirror requires metastore external data access plus `EXTERNAL USE SCHEMA` and `SELECT` on the relevant UC objects.

Use the SQL analytics endpoint for Fabric validation. If Fabric Spark is used instead, document that catalog commits prevent Spark reads through the mirror and describe the exact table feature disabled to work around that limitation.

## Demo 2: Publish to Power BI

### Required sequence

1. Create the dedicated UC schema `fabricinterop_powerbi` using the selected catalog's managed storage, and create the Fabric workspace after prerequisite checks and approvals. Use of managed storage for this separate Power BI schema is explicitly approved; this does not authorize changing storage configuration or expanding permissions.
2. Create and populate the independent four-table star schema. Declare dimension primary keys and the three `FactSales` foreign keys before publishing. Independently validate key uniqueness and referential integrity.
3. From Databricks Catalog Explorer, select the existing Unity Catalog-enabled SQL warehouse and publish the selected schema using **Use with BI tools → Publish to Power BI workspace**. Publish a new semantic model to the specified home-tenant workspace. Default to **DirectQuery**; use **Import** only when explicitly configured. Record authentication and publishing options.
4. Before manual model edits, capture the generated semantic model's tables and all relationship metadata: source/target columns, cardinality, active status, and filter direction. Databricks documentation says foreign-key relationships are preserved, but the demo's result must be observed and reported. Power BI can deactivate relationships when multiple paths between tables exist.
5. Create a minimal report with total sales and date, product-category, and customer-region filters. Compare filtered results with Databricks reference queries.
6. Change source data and record when Power BI observes it and whether a query, semantic-model refresh, or republish was required. For Import mode explicitly run and record a refresh. Do not imply Import data updates immediately.

If automatic relationship propagation does not meet the objective, report it as unmet or partially met. Manual remediation, if demonstrated, is a separate labeled result and does not count as propagation.

### Prerequisite gate

Verify UC-enabled compute, a Databricks SQL warehouse, a Power BI Premium/PPU/Fabric capacity with XMLA Read Write, home-tenant workspace access, Entra consent for the Databricks Dataset Publishing Integration, authentication/credentials, licenses, and the selected semantic-model storage mode. The user has approved managed-storage use only for the separate `fabricinterop_powerbi` schema; verify required existing UC privileges without changing grants or storage configuration. All other permission, infrastructure, and deployment gates remain in force. If an essential capability is unavailable, report the exact blocker and proceed with the other independent demo where feasible; do not silently substitute a manual connector or different integration.

## Demo 3: Compatibility Mode

The approved design reuses the Databricks workspace, catalog, writable isolated
credential, identity, Fabric connection and capacity, with a separate Fabric
workspace, two Storage items and two UC schemas. Both producer sets retain
`catalogManaged`; only the compatible set generates separate, read-only
down-level outputs. Source and output locations and actual reader/writer
protocols and feature lists must be recorded separately.
For guided setup, the user explicitly excluded MV backing location, ID, version
and protocol verification. Missing backing metadata does not block deployment
and is not reported as verified. Required MV creation/refresh and checks of
actual generated compatibility outputs remain; table provenance/freshness checks
are unchanged. The historical backing evidence below is preserved separately.

Each producer set contains the four baseline tables and a native sales
materialized view at Day × Product × Region × Segment grain, without
customer-level grouping. Include date and product descriptors, exact sales
total, total quantity and line count. Verify source keys, row counts, totals,
full aggregate group equality, MV type and refresh status.

The normal mirror explicitly selects the four tables and MV. The validation
Lakehouse uses mirror-based shortcuts for normal tables and direct ADLS
shortcuts to the five actual generated compatibility outputs, as authorized
by the user. Test discovery and queries separately in Fabric SQL and Spark;
permission or readiness failures are not successful negative controls. Check
resolved paths, output provenance, freshness and protocol independently, with
bounded 900-second checks.

**Deployment evidence:** the OneLake compatibility destination attempt
required `CREATE EXTERNAL TABLE`, while the location-scoped grant was rejected
as inapplicable to `EXTERNAL_LOCATION_ONELAKE_MANAGED`. The approved fallback
uses `dbxfabricinterop` ADLS only for compatibility outputs. Both native MVs
refreshed and matched in Databricks. Fabric SQL queried the four normal tables
and five compatibility outputs; Spark read all five compatibility outputs
matching the independent reference, but the four normal mirror-based reads
reported commit-coordinator errors. The selected normal MV was not exposed by
its mirror, so its Fabric reads remain unavailable.

The four normal source table logs show reader 3/writer 7 with `catalogManaged`;
the five output logs show reader 1/writer 7 without that feature and map to
their producer IDs. The actual MV backing-source protocol was not verified.
The documented `DESCRIBE DETAIL` check by UC name returned SQLSTATE `42809`
for both native MVs, saying it expects a table but the object is a view, so
no `tableFeatures` result was available.
These are observed protocol values, not runtime versions. See the
[runbook](compatibility-mode/README.md) and
[manifest](compatibility-mode/deployment-manifest.json) for exact evidence,
including the isolated registration failure and documentation conflict.
Do not substitute external metadata, plain copies or removal of catalog commits.

## Local implementation, evidence, and completion

Each demo directory contains its own README, implementation assets, manual instructions, sanitized ownership/evidence manifest, editable and embedded Mermaid architecture diagram, local checks, and confirmation-gated cleanup instructions. Demo 3's local SQL templates remain non-executable references. The separate `python -m demos.deploy` workflow generates guarded executable SQL, real Fabric payloads and notebook lakehouse binding; it does not execute those templates. Its CLI/adapters/orchestration are tested offline with simulated services, including independent/all paths and resume. This does not certify live API compatibility, cloud permissions, Databricks SQL syntax or end-to-end service behavior. Separately approved live acceptance remains required. The root README links all three demos and explains their differences.

Evidence must distinguish **pass**, **fail**, and **blocked**; include timestamps, actual values, configured timeout, and sanitized errors. Do not fabricate resource IDs, test results, relationship behavior, propagation delays, or manual actions. Check a second run for duplicates and unintended data loss before claiming rerun safety.

Local-only preparation may include source code, SQL, documentation, static checks, deterministic generation, and unit tests. It must not create or mutate Azure, Databricks, Fabric, or Power BI resources; deploy; change shared settings or permissions; or provision chargeable resources. Cloud acceptance requires a separately approved execution after prerequisites are verified. Successful presentation resources remain available; cleanup is not run automatically.

## Authoritative references

- [Connect Azure Databricks to a OneLake external location](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-onelake)
- [Microsoft Fabric mirrored catalog from Azure Databricks](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks)
- [Configure a mirrored catalog from Azure Databricks](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks-tutorial)
- [OneLake shared access signatures](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shared-access-signature-overview)
- [Create a Unity Catalog schema with a managed location](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema)
- [Constraints on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/tables/constraints)
- [Publish to the Power BI service from Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/service)

Recheck preview status, prerequisites, and documentation during the separately approved deployment. Documentation is not evidence that a feature is enabled in the target tenant.
