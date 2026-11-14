# Live evidence — report baseline and one-time append recorded

This record contains sanitized live observations. Credentials and bearer tokens were not logged or persisted. Relationship evidence was captured before manual model edits; none have been made. The approved append ran once, and exact source/model totals matched within the configured visibility window.

## Context and isolation

* Verification date/time (UTC): baseline report UI around `2026-11-14T19:12Z`–`19:13Z`; pre-append source recheck `2026-11-14T19:43:02.697Z`; append commit `2026-11-14T19:47:09Z`; post-change source read `2026-11-14T19:48:22Z`; exact model check completed by `2026-11-14T19:52:05.744Z`. The earlier pre-edit model inspection timestamp was not retained in the session checkpoint.
* Environment/home tenant: MCAP / `4526cee1-4399-4e4b-9134-46fabd583527`.
* Databricks workspace: `dbxfabricinterop`, ID `5878076557050`; approved catalog `dbxfabricinterop`; schema `fabricinterop_powerbi`; SQL warehouse `fabricinterop-powerbi-demo`, ID `f37863b7242d0931`.
* Fabric workspace `fabricinterop_powerbi`, ID `bdd6b268-1844-42a5-93ac-261b084fbaee`; shared capacity ID `de12195c-40d2-4bee-af60-f0d6dedfbb0f`.
* Semantic model `dbxfabricinterop-fabricinterop_powerbi`, ID `39b32d34-8a65-4d0f-8c3a-5aa37d95a7c1`. Configured expected name was `fabricinterop_powerbi_model`; preserve actual identity. The user completed Databricks Catalog Explorer **Publish to Power BI** in a regular browser.
* Dataset/model metadata separately reported `defaultMode: Import` and `targetStorageMode: Abf`; each of the four observed table partitions reported `DirectQuery` and `Ready`. Do not conflate these values.
* Workspace Admin was previously verified; XMLA Read Write and tenant XMLA integration setting were user-provided verified facts. No shared settings or permissions were modified.
* Ownership and baseline resource evidence: `manifest.json` and ignored local `prepared/deployment-evidence.json`. The dedicated workspace had zero items before publishing. The saved report is `fabricinterop_powerbi_report` (ID `3dcfdaab-7eca-4415-8bf2-ba55cec3a091`) and is bound to the existing model; the report is tracked in `manifest.json`.

## Source before publishing

* Databricks SQL Statement Execution API read recheck at `2026-11-14T17:37:22.034Z` (statement `01f1e052-0bb8-168e-8654-6d33e779a77a`), using the verified MCAP identity and the named warehouse: `DimDate` 365, `DimProduct` 20, `DimCustomer` 100, `FactSales` 10,000; `FactSales` total `930299.36`.
* The named warehouse was observed `STOPPED` immediately before the first source query; SQL execution may have started it. It is X-Small with a 10-minute auto-stop. No additional resource was created.
* Earlier baseline integrity checks: null keys 0, duplicate keys 0, orphans 0; four primary keys and all three fact-to-dimension foreign keys verified in Unity Catalog metadata. Sanitized creation/query evidence: `prepared/deployment-evidence.json`.
* Source groups matched the deterministic expected reference: 365 dates, 12 months, four product categories, four customer regions. The source combined filter `2024-01-01` / `Accessories` / `Central` returned `214.71`.
* The earlier separate read found 0 occupied `SalesLineKey` values in 10001–10010; this was only a snapshot. Immediately before the insert, the expected MCAP identity was confirmed, schema/table grants showed `ALL PRIVILEGES` only for that identity, query history showed no active warehouse queries, and the standalone collision query again returned 0.
* The pre-append source read at `2026-11-14T19:43:02.697Z` confirmed 10,000 rows and `930299.36`. Post-append source and model totals matched exactly. The report UI observations below are rounded and attributed to the coordinating assistant's signed-in browser canvas.

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

* Report: `fabricinterop_powerbi_report`, ID `3dcfdaab-7eca-4415-8bf2-ba55cec3a091`, in workspace `fabricinterop_powerbi` (`bdd6b268-1844-42a5-93ac-261b084fbaee`), bound to model `dbxfabricinterop-fabricinterop_powerbi` (`39b32d34-8a65-4d0f-8c3a-5aa37d95a7c1`). `GetReportMetadata` verified the binding and returned the card visual, but did not list slicers. The coordinating assistant observed the date, category, and region slicers in the signed-in browser canvas.
* Coordinating assistant's rounded UI observations before append: default card `930.30K`; Category=Accessories `212.94K`; Category=Accessories plus Region=East `53.39K`; Date 2024-12-01 through 2024-12-15 `40.68K`; reset showed `930.30K` and `No filters applied`.
* Exact pre-append model values independently verified: 10,000 rows / `930299.36`; Accessories `212937.91`; Accessories plus East `53389.83`; 2024-12-01 through 2024-12-15 `40683.30` (422 rows). The report's combined date/category/region UI interaction was not captured.
* Batch key range 10001–10010. Standalone collision query `01f1e064-2bf4-14fc-8412-006cc9f38f34` returned 0 at `2026-11-14T19:47:06.403Z`. Insert statement `01f1e064-2cb2-1830-974b-ffbb4feeeb76` succeeded; Query History records execution `19:47:07.649Z`–`19:47:10.341Z`. Delta version 21 has raw timestamp `2026-11-14 19:47:09` in SQL session timezone `Etc/UTC`, so the 900-second deadline was `20:02:09Z`.
* Timestamp correction: the raw Delta history value was returned as an unzoned `DateTime`. Converting it with PowerShell `ToUniversalTime()` incorrectly assumed the Windows host's UTC+1 local zone and produced `18:47:09Z`. Query History and the `Etc/UTC` SQL session establish the correct commit time as `19:47:09Z`; no deadline failure occurred.
* Post-change source at `19:48:22Z`: 10,010 FactSales rows, total `930995.27`; 10 batch rows totaling `695.91`; 10,010 distinct keys; null and duplicate fact keys 0; dimension counts 365/20/100 with no duplicate keys; all three FK orphan counts 0.
* Fresh exact model query completed by `2026-11-14T19:52:05.744Z` (elapsed upper bound 296.7 seconds, inside the 900-second deadline): 10,010 rows / `930995.27`, matching the source. Jan 1–10 returned 287 rows / `25462.05`; Dec 1–15 returned 422 rows / `40683.30`. Category totals: Accessories `213111.04`, Apparel `221047.64`, Equipment `245050.15`, Outdoor `251786.44`. Region totals: Central `232046.37`, East `237989.45`, South `230195.63`, West `230763.82`.
* The coordinating assistant refreshed the saved report around `19:49Z`; its default card changed from stale `930.30K` to `931.00K` and remained reset to default filters. This rounded UI read is consistent with the exact source/model total `930995.27`, but does not establish an exact card value. A separately suggested exact candidate `930996.86` is not supported by the exact source/model reads.
* Visibility status: **PASS within 900 seconds** by exact model/source match. No further insert, model edit, relationship edit, permission change, or cloud write was performed. The signed-in UI's combined slicer interaction remains unverified.
