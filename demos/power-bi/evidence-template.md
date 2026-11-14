# Live evidence — pre-edit model captured; report and post-change checks pending

This record contains sanitized live observations. Credentials and bearer tokens were not logged or persisted. Relationship evidence was captured before manual model edits; none have been made.

## Context and isolation

* Verification date/time (UTC): source recheck `2026-11-14T17:37:22.034Z`; exact timestamp of the earlier pre-edit model inspection was not retained in the session checkpoint.
* Environment/home tenant: MCAP / `4526cee1-4399-4e4b-9134-46fabd583527`.
* Databricks workspace: `dbxfabricinterop`, ID `5878076557050`; approved catalog `dbxfabricinterop`; schema `fabricinterop_powerbi`; SQL warehouse `fabricinterop-powerbi-demo`, ID `f37863b7242d0931`.
* Fabric workspace `fabricinterop_powerbi`, ID `bdd6b268-1844-42a5-93ac-261b084fbaee`; shared capacity ID `de12195c-40d2-4bee-af60-f0d6dedfbb0f`.
* Semantic model `dbxfabricinterop-fabricinterop_powerbi`, ID `39b32d34-8a65-4d0f-8c3a-5aa37d95a7c1`. Configured expected name was `fabricinterop_powerbi_model`; preserve actual identity. The user completed Databricks Catalog Explorer **Publish to Power BI** in a regular browser.
* Dataset/model metadata separately reported `defaultMode: Import` and `targetStorageMode: Abf`; each of the four observed table partitions reported `DirectQuery` and `Ready`. Do not conflate these values.
* Workspace Admin was previously verified; XMLA Read Write and tenant XMLA integration setting were user-provided verified facts. No shared settings or permissions were modified.
* Ownership and baseline resource evidence: `manifest.json` and ignored local `prepared/deployment-evidence.json`. The dedicated workspace had zero items before publishing; the model is now the sole discovered dataset. No report exists yet.

## Source before publishing

* Databricks SQL Statement Execution API read recheck at `2026-11-14T17:37:22.034Z` (statement `01f1e052-0bb8-168e-8654-6d33e779a77a`), using the verified MCAP identity and the named warehouse: `DimDate` 365, `DimProduct` 20, `DimCustomer` 100, `FactSales` 10,000; `FactSales` total `930299.36`.
* The named warehouse was observed `STOPPED` immediately before the first source query; SQL execution may have started it. It is X-Small with a 10-minute auto-stop. No additional resource was created.
* Earlier baseline integrity checks: null keys 0, duplicate keys 0, orphans 0; four primary keys and all three fact-to-dimension foreign keys verified in Unity Catalog metadata. Sanitized creation/query evidence: `prepared/deployment-evidence.json`.
* Source groups matched the deterministic expected reference: 365 dates, 12 months, four product categories, four customer regions. The source combined filter `2024-01-01` / `Accessories` / `Central` returned `214.71`.
* A separate read found 0 occupied `SalesLineKey` values in 10001–10010. This is only a collision snapshot, not an exclusive-writer lock. Recheck immediately before the one-time insert.
* Status: baseline source verification passed. Individual filter totals below are observed from the published model; no report-vs-SQL visual comparison has occurred.

## Model metadata — pre-edit only

* Inspection: Power BI REST workspace dataset/report reads and Power BI modeling `ConnectFabric`, table, partition, relationship, and schema operations. No manual edits have been made.
* Tables: exactly `dimdate`, `dimproduct`, `dimcustomer`, and `factsales`; all four partitions `DirectQuery` / `Ready`.
* Schema selection: the REST dataset list returned one model in the dedicated workspace; model inspection returned the four demo tables.

| Observed source table.column | Observed target table.column | Observed cardinality | Active? | Observed cross-filter direction | Evidence / result |
|---|---|---|---|---|---|
| `factsales.DateKey` | `dimdate.DateKey` | Many-to-One | Yes | OneDirection | Active relationship returned by modeling API |
| `factsales.ProductKey` | `dimproduct.ProductKey` | Many-to-One | Yes | OneDirection | Active relationship returned by modeling API |
| `factsales.CustomerKey` | `dimcustomer.CustomerKey` | Many-to-One | Yes | OneDirection | Active relationship returned by modeling API |

* Additional or ambiguous relationships: none observed.
* Model baseline via Power BI REST `executeQueries`: 10,000 fact rows; total `930299.36`; full groups had zero mismatches (365 dates, 12 months, four categories, four regions). Filter totals: date `2024-01-01` = `2436.41`; category `Accessories` = `212937.91`; region `Central` = `231873.24`; combined selection = `214.71`.
* Preservation objective: **PASS for pre-edit relationship metadata** (three active expected relationships, many-to-one, one-direction). Report visual/filter acceptance is still pending.
* Manual edit timestamp/remediation: none.

## Report and change validation

* Report: none. Power BI REST and Fabric workspace report-list reads returned an empty list.
* Report creation blocker: the embedded browser redirects to sign-in. The documented [Fabric Create Report API](https://learn.microsoft.com/en-us/rest/api/fabric/report/items/create-report) requires `Report.ReadWrite.All` or `Item.ReadWrite.All`; the verified MCAP Fabric API token has neither delegated scope. No permissions were changed.
* User action: in a signed-in regular Power BI browser, create `fabricinterop_powerbi_report` in `fabricinterop_powerbi` using the existing model `dbxfabricinterop-fabricinterop_powerbi` (ID `39b32d34-8a65-4d0f-8c3a-5aa37d95a7c1`), add the agreed sales card and date/category/region slicers, and save. Do not republish or edit relationships.
* Report ID, visual totals, individual slicer comparisons, combined report filter, and report-vs-SQL status: blocked until the report is saved and its ID is recorded.
* Batch key range: 10001–10010; last collision snapshot 0; committed rows 0; commit timestamp: none. Do not use the snapshot as concurrency control.
* Post-change source/model counts and integrity checks: pending; no insert was run.
* Mode: observed partitions are DirectQuery/Ready. Fresh report query/visual action: pending; no refresh or query-after-change was performed.
* Configured visibility deadline: 900 seconds. The timer has not started because no batch commit occurred.
* Visibility status: blocked pending report baseline, immediate collision recheck, and exclusive-writer gate.
