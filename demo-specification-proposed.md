# Databricks to Fabric Demo Specification

## 1. Objective

Build, deploy, test, and document two reproducible demonstrations:

1. **OneLake interoperability:** Databricks uses OneLake as a Unity Catalog external location, reads and writes Delta tables there, and Fabric reads those same tables.
2. **Publish to Power BI:** Databricks publishes a Unity Catalog star schema through its **Publish to Power BI** feature, demonstrating whether declared table relationships propagate to the generated Power BI semantic model.

Treat relationship propagation as a capability to verify, not an assumed outcome. Report unsupported behavior explicitly.

## 2. Environment and resource isolation

Default to:

| Setting | Default |
|---|---|
| Environment | MCAP |
| Databricks workspace | `fabricinterop` |
| Fabric capacity | `fabricadbmirror` |

Reuse only the existing tenant/subscription context, Databricks workspace, Fabric capacity, and an explicitly selected parent Unity Catalog catalog.

Create a separate Unity Catalog schema and Fabric workspace for each demo. All demo tables, notebooks, semantic models, reports, and other artifacts must be newly created and independently named.

List any additional shared infrastructure or identity required by the implementation and obtain approval before reusing or modifying it. Do not modify existing workloads, shared permissions, or capacity settings without approval.

Use a configurable deployment prefix and stable per-demo names. Reruns may reuse resources created by the same demo deployment but must not adopt unrelated existing resources.

## 3. Feasibility and prerequisites

Before deploying either demo:

- Confirm the active tenant, target workspace identifiers, and capacity assignment.
- Verify required permissions, authentication, network access, licensing, and feature availability.
- For Demo 1, verify that Unity Catalog external-location support for OneLake is enabled in the Databricks workspace; the feature is currently in Public Preview. Verify the ID-based OneLake path, identity, security-role membership, and Databricks read/write operations. Creating the schema with a managed location requires `USE CATALOG`, `CREATE SCHEMA`, and `CREATE MANAGED STORAGE` privileges. Fabric must read through a mirrored Azure Databricks catalog, not directly from the storage item; verify external data access on the metastore and the mirror identity's `EXTERNAL USE SCHEMA` and `SELECT` privileges.
- Check both OneLake SAS controls separately: tenant-level **Use short-lived user-delegated SAS tokens** for key generation, and **Authenticate with OneLake user-delegated SAS tokens** for the target workspace. A disabled tenant authentication default may be overridden by a workspace admin if delegation permits; do not change tenant-wide settings without approval.
- For Demo 2, verify availability and prerequisites of Databricks **Publish to Power BI**, including a Unity Catalog-enabled SQL warehouse, Power BI capacity with XMLA Read Write, home-tenant workspace access, Entra consent, selected semantic-model storage mode, credentials, and relationship handling.
- Identify which steps can be automated and which require UI interaction.
- Record additional compute or infrastructure needs and obtain approval before provisioning chargeable resources beyond the stated environment.

If a required capability is unavailable, report the exact blocker and supporting error or documentation. Do not silently substitute another integration path. Continue the other demo if it is independent and feasible.

## 4. Configuration

Provide a documented configuration template covering:

- Tenant ID and subscription ID where applicable.
- Databricks workspace URL/ID, parent catalog, per-demo schema names, and required compute or SQL warehouse identifiers.
- Fabric capacity ID, per-demo workspace names/IDs, and relevant item names/IDs.
- Demo 1 OneLake location and Unity Catalog external-location and credential configuration.
- Demo 2 publishing options and the selected semantic-model storage mode.
- Deployment prefix, sample-data seed, row counts, and verification timeout.
- Authentication method and secret references.

Defaults must select the environment above. Resolve and record identifiers rather than relying solely on display names. Never store tokens or secrets in source control, configuration templates, logs, or diagrams.

## 5. Sample star schema

Use the same logical model and deterministic generator in both demos, but create independent physical tables and data in each demo.

| Table | Grain | Key relationships |
|---|---|---|
| `DimDate` | One row per calendar date | Primary key: `DateKey` |
| `DimProduct` | One row per product | Primary key: `ProductKey` |
| `DimCustomer` | One row per customer | Primary key: `CustomerKey` |
| `FactSales` | One row per sales order line | Primary key: `SalesLineKey`; foreign keys to each dimension |

Include useful attributes for filtering, such as calendar month, product category, and customer region. Include quantity and monetary amounts in `FactSales`, using decimal types for money.

Document all columns and types. Declare primary and foreign keys in Unity Catalog where supported, noting whether constraints are enforced. Validate the generated data independently of constraint enforcement.

Default to 365 dates, 20 products, 100 customers, and 10,000 sales lines, with seed `42`. Make these values configurable and generate data that exercises all three relationships. Use a default verification timeout of 900 seconds, configurable before execution.

## 6. Demo 1: OneLake as a Unity Catalog external location

### Required flow

1. Create a dedicated Fabric workspace and an **Azure Databricks Storage** item. Configure the documented OneLake settings on the new workspace and item.
2. Configure and register the OneLake external location using the documented ID-based `abfss://<WorkspaceID>@onelake.dfs.fabric.microsoft.com/<DatabricksStorageID>/Files/` path and verified supported authentication method. Make the Databricks managed identity or service principal the only member of the storage item's ReadWrite OneLake security role.
3. Create the dedicated schema under the explicitly selected parent catalog with its managed location set to the OneLake external location, then use Databricks to create and populate managed Delta tables backed by that location.
4. Read the tables through Unity Catalog and validate their contents.
5. Mirror the Unity Catalog schema into the dedicated Fabric workspace and use its read-only SQL analytics endpoint to read the same underlying Delta data. This uses the documented zero-copy mirrored-catalog path; do not claim direct reads from the Azure Databricks Storage item.
6. Insert a deterministic batch through Databricks and verify that both Databricks and the mirrored Fabric catalog can read the committed changes after the documented propagation delay.

Document the Fabric reader, registration or discovery steps, credentials, permissions, preview enablement, and observed visibility delay. The external-location feature is in Public Preview. Mirrored-catalog reads are read-only and changes can take seconds to several minutes to appear. If Fabric Spark is used instead of the SQL analytics endpoint, document the catalog-commit limitation and any table feature disabled to work around it. Distinguish shared storage access from data ingestion or replication.

### Acceptance criteria

- Unity Catalog records the intended external storage location.
- Databricks successfully writes and reads all four managed Delta tables backed by the OneLake location.
- Fabric reads the same four underlying tables through the mirrored Azure Databricks catalog and SQL analytics endpoint, without a second data copy.
- Both engines return matching row counts, exact decimal sales totals, and agreed aggregates grouped by date, product category, and customer region.
- Both engines observe the inserted batch within the configured timeout; report actual propagation latency and do not imply immediate consistency.

## 7. Demo 2: Databricks Publish to Power BI

### Required flow

1. Create a dedicated Unity Catalog schema and Fabric workspace.
2. Populate the four star-schema tables and declare primary keys on dimensions and foreign-key relationships from `FactSales` to each dimension before publishing. Primary- and foreign-key constraints are informational, so validate uniqueness and referential integrity in the data; `NOT NULL` constraints are enforced. Declare keys in table DDL or add them afterward, since CTAS does not support the constraint clause.
3. Use Databricks **Publish to Power BI** to publish the schema to the dedicated home-tenant Power BI workspace. Record the SQL warehouse, table selection, authentication method, and publishing options. Default to DirectQuery; use Import only when explicitly configured, and report a blocker rather than silently switching modes.
4. Inspect the resulting semantic model's tables, storage mode, and relationships.
5. Create a minimal report showing total sales and filtering by date, product category, and customer region.
6. Change source data and verify downstream visibility, documenting whether a new query, semantic-model refresh, or republishing is required for the selected mode.

### Acceptance criteria

- Publishing uses the specified Databricks feature, not a substitute connection or mirroring workflow.
- All four intended tables appear in the generated semantic model.
- Before any manual model edits, capture evidence of whether the three declared Unity Catalog foreign-key relationships propagated. Databricks documentation says foreign-key relationships are preserved; the demo must verify this observed result.
- For each relationship, record its source/target columns, cardinality, active status, and filter direction.
- Dimension filters produce the expected fact aggregations, verified against Databricks reference queries.
- Source changes become visible within the configured timeout after the documented refresh/query action. For Import mode, explicitly run and record a refresh; do not imply that source changes are immediately visible.

If relationships do not propagate, mark the preservation objective as unmet or partially met. A separately labeled manual-remediation demonstration may be documented, but must not count as automatic preservation.

## 8. Deployment and parallel execution

- Place each demo in its own subdirectory under the main demo directory.
- Run the two implementation/deployment tracks in parallel where tooling permits and their prerequisites are satisfied.
- Keep resource names, configuration outputs, and execution state separate.
- Serialize any unavoidable changes to shared infrastructure or permissions.
- Make reruns safe: detect owned resources, avoid duplicates, and require explicit confirmation before destructive replacement.
- Do not let one demo's cleanup or failure disrupt the other.

Shared local utilities, such as the data generator, may be reused if each demo's entry point and documentation remain self-contained.

## 9. Validation and evidence

For each demo, provide executable checks where supported and explicit manual checks otherwise:

- Expected row counts, unique/non-null dimension keys, unique fact keys, and zero orphaned foreign keys.
- Reference totals and dimension-filtered aggregates.
- The required read/write, publishing, and downstream-change tests.
- A second deployment run without duplicate resources or unintended data loss.
- Evidence of resource isolation and cleanup scope.

Record pass/fail/blocked status, actual results, timestamps, configured timeouts, and sanitized error details. Set and document a numeric verification timeout before execution; do not extend it silently to turn a failed check into a pass.

## 10. Deliverables and completion

Each demo directory must include:

- A README with prerequisites, configuration, deployment steps, a presenter walkthrough, known limitations, and troubleshooting.
- Deployment scripts/notebooks and documented manual steps.
- Sample-data generation and validation scripts/queries.
- A sanitized manifest of created resource names/IDs and relevant configuration.
- An architecture diagram in editable Mermaid form showing resource boundaries, identity/authentication, storage, compute, and data access or movement.
- Test results and supporting evidence.
- Cleanup instructions or scripts limited to that demo's owned resources, with an explicit confirmation step.

Provide a top-level README linking both demos and explaining the differences between the integration patterns.

Completion requires deployed resources, executed validation, recorded outcomes, and reproducible documentation. Clearly distinguish successful demonstrations from blocked or unmet objectives. Leave successful demo resources available for presentation; do not run cleanup automatically.

## 11. Authoritative references

- [Connect Azure Databricks to a OneLake external location](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-onelake)
- [Microsoft Fabric mirrored catalog from Azure Databricks](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks)
- [Configure a mirrored catalog from Azure Databricks](https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks-tutorial)
- [OneLake shared access signatures](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shared-access-signature-overview)
- [Create a Unity Catalog schema with a managed location](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema)
- [Constraints on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/tables/constraints)
- [Publish to the Power BI service from Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/service)

Recheck preview status and prerequisites during deployment; documentation is not evidence that a feature is enabled in the target tenant.
