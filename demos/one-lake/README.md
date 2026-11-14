# OneLake interoperability demo

**Status: deployed; original baseline deadline failed, append check passed.** The dedicated
workspace, Storage item, identity roles, isolated writable credential, external
location, and managed schema are recorded in `deployment-manifest.json`. Four
OneLake-backed managed Delta tables were created and validated in Databricks. The
original shared credential remains read-only and unmodified. A dedicated Fabric
Databricks connection was created by the user, who confirmed the MCAP administrator
account used to sign in; Fabric's connection test returned Online. The user created
the mirror after confirming selection of only the demo schema and its four tables.
Read-back confirms an explicit four-table include list. A later exact baseline
comparison matched, and the separately timed append check passed.
The baseline mirror check missed its 900-second deadline and is a policy failure,
not a measured propagation latency. See the manifest for the original commit time,
deadline, and observed state; do not reset the clock for a later observation.

This demo writes a small managed star schema from Azure Databricks into a Fabric
Databricks Storage item's OneLake location using the documented Unity Catalog external
location feature (Public Preview), then reads it from Fabric **through the mirrored
Azure Databricks catalog's SQL analytics endpoint**. Fabric does not read the Storage
item directly.

## What is here

- [`../demo-specification.md`](../demo-specification.md) — canonical implementation
  specification shared by both demos.
- [`architecture.mmd`](architecture.mmd) — editable Mermaid architecture.
- [`config.example.json`](config.example.json) — non-secret settings template. Make a
  private `config.json` copy and fill in real values only in your local working copy.
- [`scripts/prepare_data.py`](scripts/prepare_data.py) — dependency-free local data
  generation, validation, and CSV export using the shared `demos/common/star_schema.py`.
- [`scripts/render_schema_sql.py`](scripts/render_schema_sql.py) — offline SQL renderer.
  It requires both an explicit parent catalog and `parent_catalog_approved: true`; it
  never looks up or assumes a current/default catalog.
- [`scripts/render_mirror_verification.py`](scripts/render_mirror_verification.py) —
  renders before/after expected-vs-actual SQL from the same deterministic configuration.
- [`notebooks/`](notebooks/) — Databricks notebook source to create the initial tables
  and append one deterministic batch. The notebooks refuse pre-existing tables or an
  unexpected append state.
- [`sql/verify_mirror.sql`](sql/verify_mirror.sql) — instructions for generating the
  config-specific expected-vs-actual queries.
- [`scripts/cleanup_plan.py`](scripts/cleanup_plan.py) — offline, confirmation-gated
  cleanup-plan renderer; it never deletes resources or calls a cloud service.

## Prerequisites

An administrator/operator must verify and arrange these prerequisites before any live
execution:

1. A dedicated Fabric workspace, Fabric capacity, and Fabric **Azure Databricks Storage**
   item for this demo. The workspace and schema must be independent of unrelated demos
   and workloads. Confirm that the Fabric tenant setting permits creation of Azure
   Databricks Storage items. Any additional chargeable resource use requires separate
   approval; this runbook does not create or enable resources/settings.
2. The selected Azure Databricks workspace is `dbxfabricinterop`; the selected parent
   catalog name is also `dbxfabricinterop`, and its use is explicitly approved. Verify
   the exact catalog identity before execution; this approval does not grant catalog
   privileges or authorize other shared-resource changes.
   The workspace must have Unity Catalog enabled and use Databricks Runtime **18.1 or
   later** or supported serverless compute for OneLake external locations. These
   notebooks require the full repository layout to be present in the Databricks
   workspace and the notebook process
   working directory to be exactly `<repo>/demos/one-lake/notebooks`. The path resolver
   fails closed if that layout is absent. This working-directory behavior is documented
   for classic Databricks Runtime 18.1+. A serverless notebook in the deployed layout
   also reported the required working directory and loaded the shared helpers successfully.
3. Use an Azure Databricks access connector managed identity (or supported service
   principal) as the storage credential identity. The identity itself needs Fabric
   workspace **Viewer** access as well as the Storage item's OneLake **ReadWrite** role.
   Grant these only to the chosen identity. Do not grant the demo's Fabric users
   `ReadWrite` for the Databricks write path. Any role changes require separate approval;
   this runbook does not alter workspace or item permissions.
4. Verify the two OneLake SAS controls separately, without changing either setting:
   - Tenant-level **Use short-lived user-delegated SAS tokens** for key generation.
   - Target-workspace **Authenticate with OneLake user-delegated SAS tokens**.

   A disabled tenant authentication default may be overridden by a workspace admin
   when tenant delegation permits. Any tenant-wide setting change requires separate
   approval and is outside this demo. Record the observed setting states before
   deployment; this assignment does not modify shared settings.
5. Enable/use Unity Catalog OneLake external-location support (Public Preview) and create
   its storage credential and external location following the official documentation.
   The external location must target the exact ID-based URL:

   ```text
   abfss://<WorkspaceID>@onelake.dfs.fabric.microsoft.com/<DatabricksStorageID>/Files/
   ```

6. For Fabric catalog mirroring, verify that external data access is enabled for the
   Databricks metastore. The Fabric connection identity needs narrowly scoped Unity
   Catalog access to the chosen demo catalog/schema and tables, including `USE CATALOG`,
   `USE SCHEMA`, `EXTERNAL USE SCHEMA`, and `SELECT` as required by the mirror tutorial.
   Grant only what the selected connection identity needs; permission changes require
   separate approval and are not performed by this demo.
7. New OneLake external locations default to file events. Verify the actual configured
   mode before deployment. `automatic` mode may create/manage a Fabric eventstream and
   requires the relevant identity's **Contributor** role; `provided` mode uses a
   separately provided eventstream and requires **Member** access. If a no-file-events
   mode is available and selected, record that explicitly; do not assume it is supported
   in the target environment. `config.json` records this choice as `onelake.file_events_mode`
   (`disabled`, `automatic`, or `provided`) and separately records
   `file_event_role_escalation_approved` and
   `file_eventstream_provisioning_approved`. The readiness check blocks an unverified
   mode and requires separate explicit approvals for identity-role escalation and
   eventstream provisioning; setting these flags is an operator attestation, not a
   change to cloud permissions or resources. Do not grant Contributor/Member or create
   an eventstream automatically. Potentially chargeable eventstream provisioning needs
   its own approval.
8. Users who need to see the mirror must have **Viewer** access to the Fabric workspace.
   Fabric mirrors the Databricks catalog and exposes its tables in a SQL analytics
   endpoint. The SQL analytics endpoint is read-only; perform writes from Databricks.
9. Prepare a dedicated demo schema. The caller needs `USE CATALOG` and `CREATE SCHEMA` on
   the selected catalog and `CREATE MANAGED STORAGE` on the external location used for
   the schema's managed location. Keep these grants narrowly scoped and remove them
   after setup if no longer needed. Any grant/revoke requires separate approval; do not
   make permission changes as part of local preparation.

Official references:

- [OneLake external locations (Public Preview)](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-onelake)
- [CREATE SCHEMA and MANAGED LOCATION privileges](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema)
- [Azure Databricks mirroring overview](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks)
- [Mirror Azure Databricks catalog tutorial](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks-tutorial)
- [OneLake shared access signatures](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shared-access-signature-overview)
- [Azure Databricks primary and foreign key constraints](https://learn.microsoft.com/en-us/azure/databricks/tables/constraints)
- [Delta idempotent DataFrame writes](https://docs.databricks.com/aws/en/error-messages/delta-invalid-idempotent-writes-options-error-class)

## Local preparation and checks

From the repository root, using Python 3.10+, copy the template once and use that same
deployment config for local data, the notebooks, and expected mirror SQL. The output
directory must be new; preparation refuses to write into an existing directory:

```powershell
Copy-Item demos\one-lake\config.example.json demos\one-lake\config.json
python demos\one-lake\scripts\prepare_data.py --config demos\one-lake\config.json --output-dir demos\one-lake\generated
python -m unittest discover -s demos\one-lake\tests -v
```

The generated CSV and JSON files are local demo inputs/reference metrics only. They are
not deployed. The output folder is ignored by this demo's `.gitignore`. If the selected
output path already exists, choose another new path rather than allowing local files to
be overwritten. The shared schema metadata defines `SalesLineKey` as `BIGINT`,
`UnitPrice` as `DECIMAL(12,2)`, and `SalesAmount` as `DECIMAL(14,2)`; the notebooks
derive their Spark schemas from that metadata. The initial notebook declares all four
primary keys and three fact foreign keys as informational Unity Catalog constraints,
then verifies them with `SHOW CREATE TABLE`. They are not enforced by Databricks;
independent Python validation remains required. Azure Databricks documents UC/Delta PK
and FK constraints as GA in DBR 15.2+, below this demo's DBR 18.1 minimum.

Render the mirror verification SQL from the same config after local preparation. Each
phase contains independently generated expected totals and full date, product-category,
and customer-region aggregates; the Fabric SQL analytics endpoint supplies the actual
values. Each generated file is created exclusively and is not overwritten:

```powershell
python demos\one-lake\scripts\render_mirror_verification.py --config demos\one-lake\config.json --phase before --output demos\one-lake\generated\verify_before.sql
python demos\one-lake\scripts\render_mirror_verification.py --config demos\one-lake\config.json --phase after --output demos\one-lake\generated\verify_after.sql
```

In each query, set `@VisibilityEventUtc` to the actual UTC completion time of the
corresponding source write. The SQL computes a deadline from the config's
`verification_timeout_seconds`. `PASS` means the exact expected totals and date,
product-category, and customer-region aggregates matched when this query was observed
at or before that deadline. It proves on-time visibility at that observation, not the
actual first moment the mirror became visible. A missing/future event timestamp or a
mismatch before the deadline is `BLOCKED`; mismatches at/after the deadline are `FAIL`.
An exact match first observed after the deadline is also `FAIL`: a late observation
cannot establish that the mirror was visible within the required window. If the query
cannot run because the mirror/schema is not visible, record it as `BLOCKED` before the
deadline and `FAIL` at/after it. Do not extend the configured deadline or claim a result
from an unexecuted query.

To render the schema DDL, first fill the required values in a local `config.json` copy.
The renderer is offline and intentionally refuses to use the selected parent catalog
until its approval flag is set in the private config. The example template keeps that
flag false; after verifying the catalog identity, set it true to record the existing
approval:

```powershell
python demos\one-lake\scripts\render_schema_sql.py --config demos\one-lake\config.json
```

Review the emitted SQL and URL before running it manually in the intended Databricks
workspace. The SQL renderer does not execute SQL.

## Manual deployment sequence

Do not treat these instructions, scripts, or notebooks as deployment output. There is
no auto-deploy path in this demo.

1. Obtain the independent workspaces, storage item, capacity, supported Databricks
   workspace/compute, approved catalog, identity, and settings listed above. If a
   proposed demo workspace, Storage item, mirror, schema, external location, or table
   already exists, stop rather than adopting it; select a newly approved isolated name
   or inspect the manifest and use the guarded cleanup process.
   Check **Use short-lived user-delegated SAS tokens** at tenant level and
   **Authenticate with OneLake user-delegated SAS tokens** at workspace level
   independently. If the tenant authentication default is disabled, verify whether a
   workspace admin is permitted to override it by tenant delegation. Request separate
   approval before any tenant-wide change; do not make that change as part of this demo.
   Verify Storage-item creation is enabled, the storage identity's workspace Viewer and
   item OneLake role, metastore external data access, and the mirror connection identity's
   scoped UC privileges. Select/verify the file-events mode and obtain independent
   approval for any required Contributor/Member role elevation and any eventstream
   provisioning before recording the corresponding config attestations.
2. Copy `config.example.json` to an untracked `config.json`. Verify the selected
   Databricks workspace and catalog, fill the actual workspace and Storage item IDs, and
   set `parent_catalog_approved` to `true` to record the explicit approval for this
   catalog. This does not approve grants, chargeable resources, or deployment.
   Never use a default/current catalog. Do not put credentials, tokens, SAS strings, or
   secret values in config.
3. Create the storage credential and external location by following the OneLake preview
   docs. Verify that only the chosen Databricks identity has Storage-item `ReadWrite`.
4. Run `prepare_data.py` using the same local `config.json` used by the notebooks and
   inspect `validation.json`. Render and review the schema DDL locally. Run it manually
   only after validating the exact ID-based location and privileges. A pre-existing
   schema is a stop condition; do not adopt it or use `IF NOT EXISTS`.
5. In a Databricks Repo/workspace copy with the complete repository layout and local
   `demos/one-lake/config.json`, run `notebooks/01_create_initial_tables.py` with the
   working directory `<repo>/demos/one-lake/notebooks`. It creates four managed Delta
   tables using fully qualified names, adds the informational UC keys, and verifies
   those keys in `SHOW CREATE TABLE`. It fails instead of replacing existing tables.
6. In Fabric, create the Azure Databricks mirror for the selected catalog in the
   dedicated Fabric workspace, following Microsoft's current tutorial. Wait for mirror
   visibility; do not assume immediate availability.
7. Run the generated `verify_before.sql` in the Fabric SQL analytics endpoint. Replace
   `<mirror_schema>` with the endpoint's displayed schema name and enter the actual UTC
   completion time of the initial Databricks write in `@VisibilityEventUtc`. Require a
   `PASS`; an exact match observed only after the configured deadline is `FAIL`, not
   evidence of on-time visibility.
8. Run `notebooks/02_append_batch.py`. It accepts only the exact configured baseline or
   exact expected final rows and key/count state. The Delta writer uses a stable
   `txnAppId`/`txnVersion` for this deterministic batch; concurrent submissions of the
   same logical batch are idempotent. A rerun that sees the final state compares the
   existing batch to the generated rows and skips the write. Any partial or unexpected
   state stops before writing.
9. Wait for the mirror to reflect the appended rows, then run `verify_after.sql` with
    the actual UTC append completion time and the same configured deadline policy.
    Record observed times, counts, and outcomes only after actual execution in a reviewed
    deployment manifest. Do not infer latency or completion from the notebook ending.

Consult the manifest for created resources and verification evidence.
The connection account was confirmed by the user, not inferred from the owner.
The mirror and baseline are now checked. The baseline deadline above has already
elapsed; the later matching query does not turn that attempt into an on-time pass.
The guarded append was observed matching after about 73 seconds; this is the time to
the matching observation, not the first visibility time.

## Presenter walkthrough

1. Explain the boundary: Databricks writes to a demo schema whose managed location is
   the ID-based OneLake path; Fabric reads the mirrored Databricks catalog, never the
   storage item directly.
2. Show the generated `DimDate`, `DimProduct`, `DimCustomer`, and `FactSales` model and
   local `validation.json`. Money is represented as Decimal in Python; `UnitPrice` is
   `DECIMAL(12,2)` and `SalesAmount` is `DECIMAL(14,2)` in Spark.
3. Show the Fabric mirror and query its read-only SQL analytics endpoint. Note that
   mirror visibility is asynchronous and is not guaranteed by this local project.
4. Append the deterministic batch from Databricks, then demonstrate eventual appearance
   through the mirror. Clearly label any measured delay as specific to that live run.
5. Close with preview status, identity scope, configuration prerequisites, and cleanup
   guardrails.

## Troubleshooting

- **Renderer refuses configuration:** verify the selected parent catalog and set its
  approval flag in the private config to record the existing approval,
  provide workspace/storage item IDs, and use safe identifier names. The example is
  deliberately not deployable.
- **External location/schema creation fails:** confirm preview eligibility, DBR/serverless
  support, the chosen identity's OneLake `ReadWrite` role, both SAS controls (tenant
  key-generation and workspace authentication), delegation/override eligibility, and
  the narrowly scoped UC privileges. Do not broaden access or change tenant-wide
  settings as a first response.
- **Serverless rejects `errorifexists`:** the observed Spark Connect runtime rejected
  that mode before writing. The initial notebook uses its documented fail-if-exists
  alias `error`; a live guarded rerun rejected an existing table without changing data.
- **No default context/lakehouse:** this is not a Fabric notebook workflow; this demo
  relies on the explicit UC schema managed location. Check the Databricks schema and
  external-location configuration.
- **Tables already exist:** stop. This demo does not adopt, overwrite, or drop existing
  objects. Use a newly approved, isolated schema or follow the guarded manual cleanup
  procedure.
- **A named Fabric resource already exists:** stop rather than treating a matching name
  or prefix as proof that this demo created it. Only a resource explicitly recorded as
  created by this demo in the sanitized manifest is eligible for manual cleanup.
- **Append refuses to run:** verify the fact count, distinct keys, minimum/maximum keys,
  and—when the batch is already present—the exact generated batch values. Unexpected or
  partial data is a stop condition; use a new approved schema if the source dataset or
  logical batch has changed.
- **Tables not visible in Fabric:** verify that the mirror targets the intended catalog,
  workspace Viewer access is present, and the SQL endpoint is ready. Allow for the
  documented visibility delay; do not query the Storage item directly as a workaround.
- **Fabric query/write behavior:** the mirrored SQL endpoint is read-only. Write or
  append in Databricks.

## Limitations and safety

- OneLake external-location integration is Public Preview and may change or have
  availability restrictions.
- The manifest contains resource IDs and partial deployment evidence, but no measured
  mirror latency or claim of successful end-to-end deployment.
- Configuration and samples contain no secrets. Keep any local real configuration out
  of version control; never put tokens or SAS values in files or logs.
- Reruns intentionally stop on existing tables. The demo does not discover/adopt
  resources by prefix.
- Delta transaction idempotency and the append-state guard are implemented locally, but
  no concurrent or repeated notebook run has been exercised against a live Delta table.
- Cleanup is a manual operation, never automatic. Review the sanitized manifest, require
  the exact confirmation shown by the offline cleanup planner, and manually remove only
  resources explicitly recorded as created by this demo. First verify every entry and
  then run:

  ```powershell
  python demos\one-lake\scripts\cleanup_plan.py --manifest demos\one-lake\deployment-manifest.json --confirm "CLEANUP ONLY RESOURCES IN MANIFEST"
  ```

  This command only prints supported top-level kinds; it does not list the recorded
  Fabric connection, Databricks workspace files, notebooks, directory tree, or
  individual tables. Review those manifest entries separately. It never deletes
  anything. Do not remove resources absent from the sanitized manifest.
