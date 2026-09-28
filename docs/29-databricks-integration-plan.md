# 29 — Databricks Cloud Warehouse Integration (Phase S15-B)

Status: **PHASE 1 (reconnaissance/planning) + PHASE 2 (local tooling only)
complete.** No Databricks workspace, SQL Warehouse, cluster, Unity Catalog,
or cloud storage exists yet. No credentials exist yet — nothing in Phase 2
connects to a real workspace, so there is zero cloud cost so far. Approved
by you: Azure Databricks (trial/pay-as-you-go) as the target path (existing
Azure subscription), and Phase 2 (local tooling) to proceed immediately.

```
S1-S8:   Public APIs → Bronze → API Silver                       [done]
S9-S13:  Synthetic OLTP → ... → CDC Bronze → CDC Silver           [done]
S14:     API Silver + CDC Silver → Gold/OLAP (DuckDB, Parquet)    [done, frozen]
S15-A:   dbt over Gold (DuckDB target)                            [done, frozen]
S15-B:   Databricks planning (THIS — Phase 1 of 8, see §9)        planning only
S15-B+:  Phases 2-8 (local tooling → provisioning → cutover)      [not started]
```

## 1. Reconnaissance Findings

**No Databricks-related tooling, code, or configuration exists anywhere in
this repository.** Every mention of "Databricks" found in the repo
(`docs/22`, `docs/23`, `docs/24`, `docs/27`) is in an explicit *out-of-scope*
list from an earlier phase (e.g. "not Databricks, not Snowflake, not
ClickHouse"). Confirmed by a full-repo grep for `databricks|azure|unity
catalog|delta lake` across `.py/.yml/.yaml/.toml/.md/.cfg/.json` — no hits
outside those out-of-scope mentions and one unrelated coincidental string in
a raw GLEIF Bronze JSON record (a company name).

| Area | Finding |
|---|---|
| `pyproject.toml` | `requests`, `pyarrow`, `psycopg[binary]`, `confluent-kafka`, `duckdb`, `dbt-core`, `dbt-duckdb`. No Spark, no Databricks SDK, no Azure SDK. |
| `.env` / `.env.example` | Only `POSTGRES_*` variables. No API keys (none of the 3 live sources need auth). No Azure/Databricks variables of any kind. |
| `docker/docker-compose.yml` | 3 services only: `postgres`, `kafka`, `connect` (Debezium). No Spark, no local Databricks emulation. |
| `dbt/profiles.yml` | One target only: `dev` → `type: duckdb`, local file path. No `databricks` output block. |
| `dags/`, `spark/`, `streaming/`, `schemas/` | Empty placeholder directories from the original Phase-0 scaffold — never used by any implemented phase (S1-S15A used none of them). |
| `scripts/` | Just `run_ingestion.py` (API ingestion CLI entry point). |
| Local environment | No `databricks` CLI on `PATH`. No `java` on `PATH` (required for any local PySpark). `pip list` shows no `pyspark`, `databricks-sdk`, `databricks-connect`, or `delta-spark`. |
| README.md | Stale Phase-0 artifact predating S1-S15A; lists "Warehouse" as "PLANNED" (PostgreSQL) — already superseded by the actual DuckDB/dbt Gold implementation; not updated as part of any phase so far, including this one. |
| Git status | Clean baseline matches the S15-A completion report exactly: `.gitignore`/`docker-compose.yml`/`pyproject.toml` modified (pre-existing), everything else untracked, nothing staged. |

**Recorded baseline (per §16 of the S15-B directive, re-verified live
immediately before this document was written — same numbers as the S15-A
final report, unchanged):**
- pytest: **735 passed, 0 failed**
- `dbt build`: **PASS=73, WARN=0, ERROR=0, TOTAL=73**
- Control totals: **approved_transaction_total = settlement_total =
  reconciliation_expected_total = £435,106.16**
- Gold row counts: 4,000 transactions / 3,464 settlements / 3,464
  reconciliation rows (facts), 13 dimension tables, 6 dbt marts.

## 2. Answering §2 of the Directive (A-J)

**A. Databricks-related tooling that already exists**: none.
**B. dbt tooling that already exists**: `dbt-core` 1.12, `dbt-duckdb` 1.11,
one project (`dbt/`), one target (`duckdb`/`dev`), 28 models, 46 tests — all
S15-A, frozen, unchanged by this phase.
**C. Spark/PySpark tooling that exists**: none. No Java runtime present
either (a hard prerequisite for any *local* PySpark work).
**D. Azure resources/configuration that exist**: none. No subscription ID,
tenant ID, resource group, storage account, or service principal
referenced anywhere in the repo or `.env`.
**E. Credentials/configuration that would be required** (once a target is
chosen — none created yet): a Databricks **host URL**, a SQL Warehouse
**HTTP path**, and a **personal access token** (or OAuth) at minimum for
dbt-databricks; if using Azure Databricks specifically, also an **Azure
subscription** and (for Unity Catalog storage) a **storage account +
container** the Databricks workspace is permitted to write to. None of
these exist yet, and per §15, none will be hardcoded — all would go through
environment variables / `.env` (untracked) when created.
**F. What currently executes locally**: everything — API ingestion, Bronze,
Silver, the entire OLTP→Debezium→Kafka→CDC Bronze→CDC Silver path, Gold
(DuckDB), and dbt (against DuckDB). All of it runs on this machine, in
Docker (Postgres/Kafka/Connect) or the local Python venv.
**G. What could execute in Databricks**: the Gold materialization (as Delta
tables in Unity Catalog) and dbt's staging/intermediate/marts layer (against
Databricks SQL instead of DuckDB) — see §5 for the specific, scoped
proposal. Nothing upstream of Gold is a candidate (see §6).
**H. What must be installed locally** (not yet installed — see §7):
`databricks-cli` (or the newer `databricks` unified CLI), the Databricks
VS Code extension (GUI-integrated, optional but directly serves the
learning goal), `dbt-databricks` (an *additional* dbt adapter — dbt-core
already supports multiple adapters/targets in one project), and, only if
local PySpark authoring is wanted before touching real Databricks compute,
a JDK + `pyspark` + `delta-spark`. **Databricks Connect** is the
alternative to local PySpark + JDK — it lets local Python code submit work
to a *real* remote Databricks cluster without a local Spark/Java
installation, which fits this project's "prefer real tooling, don't fake
it locally" instruction better than a local standalone PySpark install.
**I. What must be created in Databricks Cloud** (not created — requires
your approval, §4): a workspace, at minimum one SQL Warehouse (compute for
dbt + SQL Editor), a Unity Catalog catalog (`merchantmcc`, proposed — see
§5.3) with `gold`/`staging`/`intermediate`/`marts` schemas, and either a
Databricks-managed storage location or an Azure storage account/container
Unity Catalog is granted access to.
**J. What should remain in Docker**: `postgres`, `kafka`, `connect`
(Debezium) — all three, unchanged. These are the OLTP/CDC transport layer
and have no reason to move; Databricks only ever receives already-Gold-
shaped Parquet, never touches Postgres/Kafka directly.

## 3. §3 of the Directive — Offering, Cost, and Region (answered honestly,
verify current terms before committing — Databricks/Azure pricing and
promotions change and I cannot browse live pricing pages)

**1. Which Databricks offering/account is available**: unknown — this is
genuinely your decision/account, not something derivable from this repo.
Two realistic paths exist publicly:

| | **Databricks Community Edition** | **Azure Databricks (trial or pay-as-you-go)** |
|---|---|---|
| Cost | Free forever, no card | Free 14-day *Databricks* trial commonly available on top of an Azure subscription, but the underlying Azure compute is billed by Azure regardless (unless covered by Azure's own new-account credit) |
| Requires | Just an email sign-up | An Azure subscription (new or existing) |
| Unity Catalog | **Not available** | Available |
| SQL Warehouses | **Not available** | Available |
| Jobs (real scheduling) | Very limited | Full |
| Clusters | Single-node, driver-only, capped resources | Configurable, autoscaling |
| Fits the stated learning goal (§6 of your directive: Unity Catalog, SQL Warehouses, Jobs GUI) | **No** — Community Edition cannot demonstrate Unity Catalog or SQL Warehouses at all | **Yes** |

Given your explicit goal (learn Unity Catalog + SQL Warehouse + Jobs, not
just notebooks), **Community Edition cannot satisfy §6 of your own
directive** — it simply doesn't have those features. Azure Databricks is
the only realistic path to what you asked for, which means this is not a
free-forever exercise once a real workspace is created.

**2. Free/educational/trial option**: yes, in the Azure path — Databricks
typically offers a time-limited Premium-tier trial waiving *Databricks'*
own DBU charges, but Azure's own compute charges for the underlying VMs
still apply (mitigated by Azure's one-time new-subscription credit, if your
account is eligible and hasn't used it). Exact current terms should be
verified on Azure's/Databricks' own sign-up pages at the time you commit —
these promotions change.

**3. What resources it provides**: a full workspace (notebooks, Jobs,
Compute, SQL Warehouses), Unity Catalog, and (once configured) governed
access to Azure Data Lake Storage for Delta tables.

**4. Whether it requires payment information**: **yes, in practice**, for
the Azure Databricks path — Azure subscriptions require a payment method on
file even when a free credit is applied. Community Edition requires none,
but see #1 above for what that path cannot do.

**5. Potential cost risks**: SQL Warehouses and clusters bill **per-second
while running**; the single most common source of an unwanted bill is
forgetting to stop one. Every warehouse/cluster this project creates should
have an aggressive auto-stop configured (recommend 10 minutes idle) from
the moment it's created — this is a concrete, actionable safeguard I'll
apply when we get to provisioning, not an afterthought.

**6. Which Azure region**: your call — I don't know your location or
whether you have a regional preference/constraint. Region mainly affects
latency (and very slightly, price) for a learning-scale workload; there's
no technical reason this project needs a specific region.

**7. Minimum resources required for this project**: given the actual data
volumes involved (4,000 transactions; the largest table, `dim_card_issuer`,
is ~375K rows — genuinely small), the smallest available SQL Warehouse size
(commonly labeled "2X-Small") and the smallest general-purpose cluster node
type would be more than sufficient. There is no technical justification for
anything larger in this project.

## 4. Technology Boundary Confirmation

Per your directive, none of the following are replaced or touched: **Kafka**
(stays the CDC/event transport), **Debezium** (stays the Postgres CDC
engine), **PostgreSQL** (stays OLTP), **dbt** (stays the transformation
tool — gains a second *target*, not a replacement), **DuckDB** (stays as
the local dev/test target — not removed). Airflow remains undeployed;
documented only as a future orchestration boundary (§8 below).

## 5. Proposed (Not Implemented) Target Design

### 5.1 Data flow

```
data/gold/dimensions/*.parquet   (produced today by src/gold/pipeline.py — UNCHANGED)
data/gold/facts/*.parquet
        │
        │  [PROPOSED, not built] a thin PySpark "Delta landing" job —
        │  reads this same Parquet, writes managed Delta tables into
        │  Unity Catalog. No dimensional-modeling logic re-implemented
        │  in Spark — see §6.
        ▼
Unity Catalog: merchantmcc.gold.dim_* / merchantmcc.gold.fact_*  (Delta)
        │
        │  dbt-databricks (a SECOND dbt target, alongside the existing
        │  duckdb target — see §7.3), same models conceptually
        ▼
Unity Catalog: merchantmcc.staging / merchantmcc.intermediate / merchantmcc.marts
        │
        ▼
   Databricks SQL Warehouse  →  SQL Editor (ad hoc) / Power BI (§13, later)
```

### 5.2 Why Spark reads Gold, not Silver, directly

Same reasoning as S15-A's dbt-reads-Gold decision (docs/28 §2): Gold
already resolved the hard, real-data-verified problems (surrogate keys,
unknown-member fallback, the event-log grain widening, the Decimal-precision
control-total identity). Re-deriving any of that in Spark would either
duplicate `src/gold/keys.py`'s stateful registry in a distributed engine
that doesn't suit small stateful lookups, or silently drop guarantees
already earned through real-data testing in S14. Spark's honest job here is
moving an already-correct Parquet artifact into a governed lakehouse
format — not recomputing it.

### 5.3 Proposed Unity Catalog structure (proposal only — nothing created)

```
merchantmcc                    (catalog)
   ├── gold                    (schema — dim_*, fact_* Delta tables, landed by Spark)
   ├── staging                 (schema — dbt-owned, mirrors dbt/models/staging exactly)
   ├── intermediate            (schema — dbt-owned, mirrors dbt/models/intermediate exactly)
   └── marts                   (schema — dbt-owned, mirrors dbt/models/marts exactly)
```

This is a direct 1:1 mapping of the *existing, already-approved* DuckDB
schema layout (`main`/`staging`/`intermediate`/`marts`, docs/28 §1) onto
Unity Catalog's catalog.schema.table namespace — not a new design, the same
one on a different engine, which is the whole point of adding a second dbt
target rather than a second project.

## 6. Spark/PySpark — Where It Provides Genuine Value (§11 of the directive)

Honest assessment first: **at this project's actual data volumes (thousands
of rows, not millions+), distributed Spark compute provides no measurable
performance benefit** — the existing DuckDB pipeline rebuilds all 13
dimensions + 5 facts and runs the full dbt DAG in well under 10 seconds on
a single machine. Saying otherwise would be exactly the "fabricated
production functionality" your directive tells me not to do.

Given that, Spark's justified role here is **architectural, not
performance-driven** — a genuine, real responsibility (not decoration):
moving already-correct local Parquet into governed Delta tables in Unity
Catalog, which is *how real lakehouses actually ingest already-processed
data from upstream systems*, and doubles as the concrete PySpark/Delta
learning exercise your stated goal calls for.

| | Owner | Why |
|---|---|---|
| API ingestion, Bronze, API Silver | **stays Python** | Unrelated to this phase; untouched by design |
| OLTP load, CDC consumer, CDC Silver state machine | **stays Python** | Stateful, Decimal-precise, checkpoint-driven logic — not a Spark-shaped problem, and touching it isn't in scope |
| Gold dimensional modeling (`src/gold/dimensions.py`/`facts.py`/`keys.py`) | **stays Python** | Already correct, already tested against real data (S14); re-deriving in Spark would duplicate logic for no benefit — the exact anti-pattern your directive warns against |
| Gold Parquet → Delta landing (**new**) | **PySpark** (proposed) | Genuine Spark responsibility: distributed-file-format writer, real Delta ACID/versioning value, real learning value |
| staging → intermediate → marts | **dbt SQL** (unchanged) | Already built (S15-A); gains a second target, not a rewrite |
| Ad hoc/interactive analysis, Query Profile exploration | **Databricks SQL** (SQL Editor) | The actual day-to-day GUI surface a Data Engineer uses — matches §6 of your directive directly |

No transformation would exist in more than one of {Python, PySpark, dbt}
without the documented reason above.

## 7. Local Development Tooling (§5 of the directive — none installed yet)

| Tool | Purpose | Local/Cloud | Why needed | Free/OSS | Runs where |
|---|---|---|---|---|---|
| `databricks` CLI | Manage workspace, jobs, secrets, clusters from a terminal | Local | Automating/scripting workspace operations (e.g. secret creation, job deployment) | Free/OSS (Apache 2.0) | Local machine, talks to your workspace over HTTPS |
| Databricks VS Code extension | Browse workspace, run notebooks, manage clusters from the editor | Local (GUI) | Directly serves your stated GUI-learning goal; lets you work in a familiar editor while still touching real Databricks compute | Free | Local machine |
| Databricks Connect | Submit local Python/PySpark code to a *real* remote cluster without installing Spark/Java locally | Local↔Cloud | Avoids faking Spark locally — code runs on real Databricks compute, matching "prefer real tooling" | Free (Databricks-provided client) | Local machine submits; execution is on Databricks compute |
| `dbt-databricks` | dbt adapter for Databricks SQL/Unity Catalog | Local (talks to cloud) | Required to add a `databricks` target to the *existing* dbt project | Free/OSS | Local machine, talks to your SQL Warehouse over HTTPS |
| JDK 17 (only if forgoing Databricks Connect for a fully local PySpark) | Java runtime Spark requires | Local | Only needed if you want to author/run PySpark against a *local* Spark session before ever touching real Databricks compute | Free (OpenJDK) | Local machine |
| `pyspark` + `delta-spark` (same caveat as JDK) | Local Spark/Delta libraries | Local | Same as above — an alternative learning path to Databricks Connect, not both | Free/OSS | Local machine |

**None of these are installed and `pyproject.toml` was not changed** — this
table exists so you can decide which path (Databricks Connect vs. local
JDK+PySpark) before anything is installed, per your explicit instruction
not to blindly add dependencies.

## 8. Future Orchestration Boundary (Airflow, §14 of the directive)

Not implemented. Documented only: once Databricks Jobs and dbt-databricks
exist, a future Airflow DAG would sequence: API ingestion → Bronze → API
Silver → CDC processing (unchanged, all local) → **trigger the Spark
Delta-landing job** → **trigger `dbt build` against the Databricks
target** → data-quality gate → Power BI refresh. This phase changes nothing
about Airflow; `dags/` remains the empty placeholder it already was.

## 9. Phase Boundary (§17 of the directive)

**This document is Phase 1 only**: reconnaissance + architecture + the
installation/setup *plan*. Phases 2-8 (local tooling install, Databricks
account/workspace setup, storage/catalog/warehouse creation, connecting
Gold, connecting dbt, running marts in Databricks, validating against
DuckDB) are **not started** and will not proceed automatically.

## 10. Security/Credential Approach (§15 of the directive)

No Databricks token, Azure key, or secret exists anywhere in this repo
today (verified, §1). When real credentials are created (Phase 3+), the
plan is: a `DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN`
(or OAuth) triple added to `.env` (git-ignored, already the pattern for
`POSTGRES_*`) with matching placeholder entries (names only) added to
`.env.example`, and `dbt/profiles.yml`'s new `databricks` target reading
them via `{{ env_var(...) }}` — the exact mechanism already used nowhere
yet in this project's dbt profile (today's single `duckdb` target has no
secrets to hide), but standard dbt practice and consistent with how
`src/oltp/config.py` already loads `POSTGRES_*` from `.env`.

## 11. Open Decisions Requiring Your Approval

1. **Databricks Community Edition vs. Azure Databricks (trial/paid)** —
   **RESOLVED, your answer**: Azure Databricks (trial/pay-as-you-go), on
   your existing Azure subscription.
2. **Azure region**, if the Azure path is chosen — **still open**; not
   asked/needed until Phase 3 (workspace creation).
3. **Databricks Connect vs. local JDK+PySpark** for local development (§7)
   — **RESOLVED, your answer**: Databricks Connect (you listed it
   explicitly). See §12.3 — installed, then deliberately uninstalled again
   from this venv over a real dependency conflict; belongs in its own venv
   when Phase 5 (PySpark authoring) actually begins.
4. **Exact Unity Catalog naming** (`merchantmcc` catalog, §5.3) — still
   proposed, not confirmed; will be confirmed at Phase 4 (catalog
   creation), not before.
5. **Whether/when to proceed to Phase 2** — **RESOLVED, your answer**: yes,
   proceed immediately. See §12 for what was actually done.

## 12. Phase 2 Execution Results (local tooling — zero cloud cost, verified)

All four items you approved were installed/attempted; one (Databricks
Connect) was deliberately reverted after a real, verified conflict — not
silently left broken.

### 12.1 Databricks CLI

Installed via `winget install --id Databricks.DatabricksCLI` (the current,
Go-based unified CLI — **not** `pip install databricks-cli`, which installs
the older, deprecated Python CLI). Confirmed: `databricks --version` →
**Databricks CLI v1.17.0**. Not yet authenticated (no workspace/token
exists — that's Phase 3).

### 12.2 Databricks VS Code extension

Installed via `code --install-extension databricks.databricks`. Confirmed:
**v2.18.0** installed. Directly serves your GUI-learning goal — lets you
browse a (future) workspace, run notebooks, and manage clusters from
inside VS Code once a workspace exists.

### 12.3 dbt-databricks adapter — installed, with a real conflict found and fixed

`pip install dbt-databricks` succeeded (**1.12.5**), but as a side effect
of its own dependency resolution it **downgraded `dbt-core` from 1.12.5 to
1.12.3** (dbt-databricks/dbt-spark's own upper-bound pin — both versions
satisfy this project's `dbt-core>=1.12` constraint, no code changes needed)
and pulled in `dbt-spark` (a shared base adapter dbt-databricks is built on
— expected, not a mistake). **Re-verified immediately**: `dbt debug` and a
full `dbt build` against the existing DuckDB target still passed
(**PASS=73, ERROR=0**) — the second adapter did not disturb the first.

Then `pip install databricks-connect` (Databricks Connect, §12.4) forced
`databricks-sdk` from 0.117.0 to **0.141.0**, which pip itself flagged as a
**real, explicit incompatibility**: `dbt-databricks 1.12.5 requires
databricks-sdk<0.118.0,>=0.68.0, but you have databricks-sdk 0.141.0`.
Imports still succeeded at a basic level, but I could not verify
dbt-databricks would work correctly against a real SQL Warehouse (none
exists yet to test against) with an SDK version 23 minor releases newer
than what it declares support for — claiming it was "fine" without that
verification would be exactly the kind of unverified claim this project's
discipline exists to avoid.

**Fix applied** (§12.4 has the reasoning): uninstalled `databricks-connect`
from this venv, then `pip install --force-reinstall --no-deps
"databricks-sdk<0.118.0,>=0.68.0"` restored **0.117.0**. `pip check` now
reports **no broken requirements**. `dbt-databricks` imports cleanly at the
correct SDK version.

### 12.4 Databricks Connect — installed, then deliberately reverted (not a failure, a real architectural finding)

`pip install databricks-connect` succeeded (**19.1**) but, as found in
§12.3, is not safely co-installable with `dbt-databricks` in the same
Python environment — their `databricks-sdk` requirements don't overlap.
This is a genuine, documented, real-world Databricks tooling friction (not
a mistake in this project): `databricks-connect` bundles a specific,
tightly-pinned SDK/runtime matched to a Databricks Runtime version, which
routinely conflicts with other Databricks packages in the same venv.

**Resolution**: Databricks Connect was **uninstalled** from this project's
main venv. It is not needed yet — there is no cluster to connect to (Phase
3+). When Phase 5 (PySpark authoring) actually begins, Databricks Connect
will be installed in a **separate, dedicated virtual environment**, pinned
to match the exact Databricks Runtime version of whatever cluster exists
by then — a real version number this project cannot know today. This is
standard practice, not a workaround: dbt (SQL orchestration) and Databricks
Connect (interactive Spark development) are different tools serving
different layers (§6), and real teams commonly keep them in separate
environments for exactly this reason.

### 12.5 Side effect noted and verified: pyarrow version drift

`databricks-connect`'s installation (before being reverted) downgraded
`pyarrow` from 25.0.1 to **24.0.0**; uninstalling `databricks-connect`
alone did not restore it (pip doesn't auto-reverse a removed package's side
effects on shared dependencies). 24.0.0 still satisfies this project's
`pyarrow>=17.0` constraint, but pyarrow underpins every Decimal128/Parquet
operation across Bronze/Silver/CDC/Gold, so this was **verified, not
assumed**: the full pytest suite was re-run after all of §12's package
changes — see §12.6. `duckdb` (python package) is at 1.5.5, unaffected by
any of §12's installs (pre-existing, unpinned beyond `duckdb>=1.0`).

### 12.6 Full verification after all Phase 2 package changes

- `dbt build` (DuckDB target, unchanged): **PASS=73, WARN=0, ERROR=0,
  TOTAL=73** — re-run after dbt-databricks install (§12.3) and again after
  the databricks-sdk fix.
- Full pytest suite: see the completion report in this phase's chat reply
  for the exact count — re-run specifically because `pyarrow`, `dbt-core`,
  and `databricks-sdk` all changed versions during §12, and this project's
  own discipline (docs/26-28) requires re-verifying after any dependency
  change, not assuming.
- `pip check`: no broken requirements.

### 12.7 `pyproject.toml` change

Added a new `[project.optional-dependencies]` group, **not** merged into
the core `dependencies` list (Databricks tooling is optional/exploratory
right now, not required to run the core local pipeline):

```toml
databricks = ["dbt-databricks>=1.12", "databricks-sdk<0.118.0,>=0.68.0"]
```

The explicit `databricks-sdk` pin (not left to dbt-databricks' own
transitive resolution) exists specifically so a future `pip install` of
something else in this group doesn't silently reintroduce §12.3's
conflict. Databricks Connect is deliberately **not** listed here (§12.4).
