# Guided demo deployment

The cross-platform Python CLI provisions the three demos independently, or all
three with **explicitly selected** shared capacity and Databricks infrastructure.
It uses Azure ARM, Fabric public REST, and Databricks unified authentication/SQL
Statement Execution. It does not run the historical deployment manifests or
non-executable demo 3 SQL templates.

**Validation boundary:** the implementation is tested offline with simulated
services. This is not evidence of live API compatibility, tenant permissions,
feature availability, successful cloud deployment or end-to-end behavior.
Obtain separate approval before any live preview, deployment or chargeable test.

## Installation and configuration

Use Python 3.10+ and [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli).
From the repository root, create and activate the virtual environment **before**
installing dependencies.

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-deploy.txt
python -m demos.deploy --demo all --action configure
```

macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-deploy.txt
python -m demos.deploy --demo all --action configure
```

The `configure` and `status` actions are offline and need no installed SDK or
cloud sign-in. The Azure CLI and SDK are needed only for live actions.

Choose `onelake`, `power-bi`, `compatibility`, or `all`. Configuration prompts for:

| Input | When |
|---|---|
| Tenant/subscription GUID, region, existing resource group | Always |
| Capacity create/reuse, exact ARM ID, or new name/F SKU/admin UPNs | Per foundation |
| Databricks create/reuse, exact ARM ID, or new Premium name/managed resource group/networking | Per foundation |
| Explicit sharing of capacity and Databricks workspace | Only `all` |
| Independent 2-40-character letters/digits/underscore prefix, catalog, named unified-auth profile, create/reuse SQL warehouse | Each demo |
| Independent Fabric workspace display name (1-256 characters, spaces allowed), create/reuse and exact ID | Each demo |
| Access connector resource/object IDs, writable isolated credential, explicit file-events mode | OneLake and compatibility |
| Dedicated unsigned ADLS root and existing UC external location | Compatibility only |
| DirectQuery (default) or explicitly selected Import | Native Power BI only |

Additional immutable IDs are requested at their dependency checkpoint: Fabric
capacity GUID (different from its ARM resource ID), tested connection IDs,
native semantic-model/report IDs, and compatibility output/backing provenance.
Never paste credentials, PATs, tokens or SAS URLs into these prompts. Credentials
belong in supported auth profiles, environment variables or the relevant service UI.

Each deployment has its own private directory; `.demo-deploy` is the ignored
default. Choose separate directories for independent runs:

```text
python -m demos.deploy --demo onelake --directory .demo-deploy/onelake --action configure
python -m demos.deploy --demo power-bi --directory .demo-deploy/power-bi --action configure
python -m demos.deploy --demo compatibility --directory .demo-deploy/compat --action configure
```

These command arguments work cross-platform; PowerShell also accepts backslash
path separators. Every demo owns new schemas/tables/items, even when infrastructure
is shared. Existing Fabric workspaces may be reused only by exact ID and explicit
dedicated-workspace approval, with the selected capacity already assigned. The
script will not change an existing capacity assignment.

## Authentication and preview

Sign in **separately** to the explicitly selected tenant. The deployer never runs
`az login`, `az account set`, changes `AZURE_CONFIG_DIR`, swaps MCP configuration,
registers providers, or modifies shared settings. For simultaneous tenant sessions,
set a tenant-isolated `AZURE_CONFIG_DIR` in the shell before signing in.

Configure the named Databricks profile for the exact resolved workspace using
[unified authentication](https://learn.microsoft.com/azure/databricks/dev-tools/auth/).
For newly created workspaces, this happens at the Unity Catalog checkpoint after
Azure provisioning. Authentication errors do not fall back to a different account.

```text
python -m demos.deploy --action preview
python -m demos.deploy --action apply
```

`preview` reads Azure account/group/provider metadata and existing resource IDs;
new capacity/workspace definitions are shown through Azure CLI ARM **What-If**.
It makes no provisioning or data writes. Provider registration/region checks do
not prove quotas, licensing, permissions or tenant feature policies: the script
explicitly asks administrators to verify those gaps. Public Databricks networking
uses secure cluster connectivity/no public cluster IP; custom/private networking
pauses for a separately approved network deployment and an explicit reuse config.
The script does not invent a private-network design.

`apply` repeats preview, presents all writes and costs, and requires `yes` before
cloud changes. Initial deterministic loading, serverless/SQL compute, native MV
creation/refresh and `SYNC UNIFORM` compatibility generation **are deployment
work**, not optional tests. Capacity, compute, storage and background predictive
optimization refresh can continue charging after the CLI exits. The deployment
does not suspend, delete, overwrite, revoke, or clean up anything automatically.

Pricing: [Fabric](https://azure.microsoft.com/pricing/details/microsoft-fabric/)
and [Databricks](https://azure.microsoft.com/pricing/details/databricks/).
What-If is a scope preview, not a price or quota guarantee.

## Manual checkpoints in execution order

Every gate prints the exact target IDs/paths, numbered instructions, an official
link and the expected action. Answer `no` to pause safely; no dependent operation
then runs. A completed manual gate is **operator-attested**, not a fabricated API
pass. API-readable prerequisites are reread on resume.

1. **Preflight and capacity mapping:** verify permissions, policies, quotas and
   SKU/region entitlement. Verify the selected Fabric capacity GUID corresponds
   to the exact ARM resource and tenant. See
   [What-If](https://learn.microsoft.com/azure/azure-resource-manager/templates/deploy-what-if)
   and [capacity administration](https://learn.microsoft.com/fabric/admin/capacity-settings).
2. **Unity Catalog:** for a new Databricks workspace, an account/metastore admin
   attaches UC and creates/approves the named parent catalog. Set up its auth
   profile, privileges and serverless entitlement. The script checks catalog
   metadata and creates/reuses the selected SQL warehouse; it does not configure
   shared metastore policies. See
   [UC setup](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/get-started).
3. **OneLake identity and external location:** after each new Storage item, verify
   tenant Storage-item creation and short-lived SAS key generation, and the target
   workspace's delegated SAS authentication setting. Verify the connector's
   managed identity. Add **only that identity** to workspace Viewer and Storage
   item OneLake ReadWrite. In Catalog Explorer create/reuse a writable,
   workspace-isolated Azure Managed Identity storage credential with that connector.
   Create the named location using **Storage type OneLake**, the printed GUID-based
   `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/Files/` URL and the
   selected file-events mode; test it and arrange narrowly scoped
   `CREATE MANAGED STORAGE`. The API read-back checks URL, credential identity,
   writability and reported file-events state. Role membership, mode-specific
   eventstream settings and UI connection tests remain attestations. Automatic
   events need Contributor and may create a chargeable eventstream; provided events
   need Member and separately provisioned events. Approve those changes separately;
   disabled events must actually be supported. See
   [the complete OneLake procedure](https://learn.microsoft.com/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-onelake).
4. **Mirror access:** after tables exist, have a metastore admin verify external
   data access. Arrange only the connection identity's required catalog/schema/
   table privileges (`USE CATALOG`, `USE SCHEMA`, `EXTERNAL USE SCHEMA`, `SELECT`).
   In Fabric Manage connections and gateways create/test the exact Databricks
   cloud connection with supported organizational/OAuth authentication; enter only
   its GUID in the script. The deployer uses an explicit Partial catalog **and**
   Partial schema selection and reads back the definition. See
   [mirror setup](https://learn.microsoft.com/fabric/mirroring/azure-databricks-tutorial).
5. **Native Power BI only:** verify home-tenant license/capacity entitlement and
   XMLA Read Write; obtain Entra Dataset Publishing Integration consent if needed.
   In Catalog Explorer select the independent schema and SQL warehouse, choose
   **Use with BI tools > Publish to Power BI workspace**, sign in, choose the exact
   workspace, a **new** model, selected mode and OAuth. Capture generated
   relationship metadata **before editing**, then provide its GUID. Create a new
   report with sales and date/category/region slicers and provide its GUID. The
   script reads back model/report identity and their binding, never substitutes
   a manually connected model or asserts relationship/filter success. See
   [native publishing](https://learn.microsoft.com/azure/databricks/partners/bi/power-bi/service)
   and [report creation](https://learn.microsoft.com/power-bi/create-reports/service-report-create-new).
6. **Compatibility only, before enablement:** in the existing HNS ADLS container,
   create the five exact UUID-scoped empty output directories printed by the
   script. Verify covering UC external-location permissions, including
   `CREATE EXTERNAL TABLE`, and a tested read-capable Fabric ADLS connection.
   Inspect inventories/audits to confirm **each path and every ancestor/descendant**
   has not been another table's compatibility location in seven days. Unique names
   do not prove that history. The script independently checks existence/emptiness
   before enabling each untouched output. It never writes files into these paths.
   See [Compatibility Mode](https://learn.microsoft.com/azure/databricks/external-access/compatibility-mode).
7. **Compatibility generation and freshness:** required deployment statements
   create/refresh both native MVs and refresh all five genuine compatibility
   versions with `SYNC UNIFORM`. Inspect producer/backing history, Uniform
   Compatibility Information and output Delta histories. Record clone source/
   backing IDs; these are compared to actual producer IDs (MV backing identity is
   operator-entered). The script verifies returned compatibility routing and
   actual output log IDs/protocol/features. Managed sources must remain beneath
   their selected OneLake managed roots; absent native MV backing-location
   metadata triggers a separate exact-location checkpoint rather than inference.
   The validation Lakehouse explicitly uses non-schema-enabled Tables-root
   shortcuts. It compares latest source and
   last-refreshed versions via SQL metadata; unfamiliar metadata shapes and MV
   backing versions trigger explicit fresh manual inputs. Stale/missing output
   stays waiting. If the actual MV backing protocol is inaccessible, retain that
   gap instead of inferring it from `MATERIALIZED_VIEW`.
8. **Normal mirror discovery:** inspect actual normal mirror table paths; provide
   the discovered `Tables/<schema>` folder. The script checks all four normal
   tables before creating their mirror-based shortcuts. It records normal MV
   discovery separately and creates its shortcut only if actually discovered.
   The five compatibility shortcuts always target the five verified actual ADLS
   outputs directly. The Fabric notebook is deployed with real
   `metadata.trident.lakehouse` binding, not merely a Python metadata comment.
   It is **not run by default**.

Creating an access connector, storage account/container, changing ACLs/roles,
entering connection credentials, and shared/preview/tenant settings deliberately
remain administrator-assisted operations. They require an independently approved
scope; the script does not manufacture missing permissions or replace full shared
role sets.

## Setup verification and optional checks

The default outcome is `setup-checked`, `waiting`, or `blocked` per demo.
`setup-checked` means resource IDs/ownership, assignment/definition read-back,
recorded deployment statements and applicable metadata/manual gates were checked.
It is **not an end-to-end success**. Power BI origin/relationships and compatibility
clone metadata/backing versions include explicitly labeled operator evidence.

```text
python -m demos.deploy --action resume --optional-checks
```

`--optional-checks` merely offers checks; the operator must independently answer
`yes` to the per-demo cost warning. Only then does it scan Databricks baseline
count/decimal total and present detailed Fabric SQL/Spark/Power BI/filter/append/
propagation evidence instructions. Counts are not full row equality. Reader and
report tests remain manual evidence, labeled as such. A source append needs its
own approval and guarded new key range; the deployer does not execute an append.
Existing demo append helpers must be adapted to this run's exact config/schema,
never executed with a historical manifest. Preserve actual source commit times
and original deadlines; a late match does not reset a failed SLA.

Compatibility setup rereads SQL **version metadata** on resume because freshness
is a prerequisite, not an optional data comparison. This may activate approved
warehouse compute. The output inspection notebook only reads fully qualified
Lakehouse tables and prints counts; running it is optional and counts alone do
not establish the full compatibility comparison.

## Safe resume and partial writes

```text
python -m demos.deploy --action status
python -m demos.deploy --action resume
```

Keep **the original config and state together**, backed up securely. Config
fingerprints, exact immutable IDs, ownership markers, parent scope, step hashes,
statement/operation handles and manual checkpoints are stored without secrets.
Files use owner-only POSIX permissions or Windows ACLs and an exclusive process
lock. Inside the repository only `.demo-deploy` may hold generated private files;
it and `*.deploy.local.json`/`*.deploy.state.json` are ignored by Git.

Submitted Fabric/Azure LROs and SQL statements resume by recorded handle with
bounded polling. Completed writes are reconciled rather than resubmitted: no
reload, refresh replay, shortcut overwrite or name-based adoption. Successful
reused resources remain unowned. Throttled 429 requests honor Retry-After; only
safe GETs retry transient 5xx. Ambiguous writes are never automatically retried.

If a process loses its response **after a durable intent but before saving a
handle**, the script refuses that step. Inspect service audit/query/operation
history and actual IDs with an administrator. Preserve state and use an explicit
reviewed recovery procedure; there is intentionally no unsafe `--force`, automatic
adoption, state reconstruction, deletion or automatic stale-lock removal.
Missing/changed IDs or ownership stop deployment. If a lock remains after a crash,
verify the owning process is gone before manually removing only that lock file.

Answering `no` at one demo's gate does not force other independent demos to stop.
A resumed run rechecks completed prerequisites and asks for current apply consent,
but does not replay completed data/refresh operations. If outputs became stale,
approve and perform an isolated refresh separately, then resume. Do not edit
configuration to repurpose a partially deployed run.

## Offline checks and limitations

```text
python -m unittest discover -s tests -v
python -m unittest discover -s demos/one-lake/tests -v
python -m unittest discover -s demos/power-bi -p test_*.py -v
python -m unittest discover -s demos/compatibility-mode -p test_*.py -v
```

Windows paths can use backslashes. New tests exercise actual CLI/workflow/adapters
against simulated services, independent/all deployments, creation/reuse, gates,
safe resume, statement/LRO handling, throttling/errors, secret rejection, generated
SQL/data and notebook binding. Mocks do not validate live Databricks syntax,
Fabric payload acceptance, UC feature rollout, credentials or permissions.

The historical normal-MV discovery gap and actual MV backing-protocol gap remain
observations, not universal product limits. No catalog-commit feature is disabled,
no plain Delta copy substitutes for Compatibility Mode, and no externally
registered copy substitutes for a generated output.

**MV metadata feasibility:** the historical manifest records both native MVs'
backing-table GUIDs and reports OneLake backing locations. Its failed
`DESCRIBE DETAIL` by name (`EXPECT_TABLE_NOT_VIEW`), OneLake 403, and direct-path
`LOCATION_OVERLAP` checks prevented verification of the **actual backing
protocol**, not those recorded identities/locations. They cannot guarantee the
same metadata will be accessible in a fresh environment. If this run cannot
establish a required backing GUID or OneLake path, answer `no` at its dedicated
checkpoint. The run stays waiting with the create/refresh handles intact;
independent demos can continue. Seek supported UC/pipeline metadata access or
vendor clarification and resume the same run. Do not reuse historical GUIDs,
infer an ID/path, weaken provenance, replace the native MV or remove catalog commits.
