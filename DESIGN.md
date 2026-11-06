# StrideForge CDP – Design

This document explains how the demo is built and why. For setup and usage see [README.md](README.md) and
[docs/customer_lake_setup.md](docs/customer_lake_setup.md).

## 1. Goal

Show a complete, realistic Customer Data Platform on Databricks for a made-up sportswear brand (StrideForge, 1.5M
customers in the US, UK, DE and CA, Oct 2023 – Oct 2026). Raw system data goes in, gets cleaned and resolved into
one view per customer, and is activated through **Customer Lake**, where marketers build audiences in natural
language and sync them to channels.

Design priorities:

1. **Realistic data with stories** – messy identities, skewed spend, seasonality and a few business problems to find.
2. **Stable activation contract** – the objects Customer Lake points at never get dropped or replaced.
3. **Self-describing** – every gold and CDP table, view and column has a description in Unity Catalog.
4. **Portable** – one Databricks Asset Bundle deploys everything to any workspace and catalog.

## 2. Architecture

```
  Source systems (synthetic)          Databricks (Unity Catalog: customer_lake_si)
  ──────────────────────────          ─────────────────────────────────────────────────────────────────
  POS, e-commerce, app,      ──►  retail_bronze ──►  retail_silver ──►  retail_gold ──►  retail_cdp ──►  Customer Lake
  loyalty, run app, ESP,          42 raw tables      37 clean tables    20 Delta tables  14 views         entity, attributes,
  ads, support, reviews,                             + identity                                           audiences, syncs
  weather, inventory                                 resolution                                                │
                                                                                                               ▼
                                                     ◄─────────── engagement flows back (opens, clicks, buys) ─ Braze, SFMC, Meta …
```

| Layer | Schema | Role | Written by |
|---|---|---|---|
| Bronze | `retail_bronze` | Raw data as each system would deliver it, including its mess | Generator (`strideforge_bronze_setup`) |
| Silver | `retail_silver` | Cleaned, standardised, every record linked to a `unified_customer_id` | `silver_*` tasks |
| Gold | `retail_gold` | Customer 360, scores, product feed, triggers, Customer Lake event tables | `gold_*` tasks |
| CDP | `retail_cdp` | Views the Customer Lake entity points at | `ensure_cdp_views` task |
| Truth | `retail_synthetic_truth` | Generator answer key (which records are the same person) | Generator |

Diagrams: [gold and CDP layers](docs/gold_data_model.png) · [gold tables and keys](docs/gold_er.png)
(sources in `docs/*.mmd`).

## 3. Synthetic data (bronze)

The generator (`src/generator/gen/`) builds 1.5M people and then the records each source system would hold about
them – about 274M rows at `scale=1.0`. It runs on serverless with Spark, Faker and pandas UDFs.

- **People first, then systems.** A hidden `persons` table is the ground truth. Each system (POS, e-com, loyalty,
  app, run app, race partners) gets its own customer records with its own ids and errors.
- **Deliberate mess.** Email typos and domain variants, phone formats, nicknames, abbreviated names, moved
  addresses, guest checkouts, shared household emails. About 4.0M source records describe the 1.5M people.
- **No flat distributions.** Log-normal spend, calendar weights (January, August, Black Friday–December), country
  and channel mix, 80/20 loyalty value.
- **Stories built into the data:**

  | Story | What happens | Where it shows |
  |---|---|---|
  | Messy identity | 4.0M records for 1.5M people | identity resolution accuracy |
  | Shoe runs small | CloudStride 3 gets ~43% "too small" online returns until size guidance in July 2026 | returns, reviews |
  | Worn-out shoes | Runners past 400 miles who don't replace their shoes drift away | run app, orders |
  | Too many emails | Black Friday 2025 over-mailing, unsubscribes ~30x for a week | message events |
  | Resellers | Bot-like accounts sharing addresses buy up limited drops | orders, addresses |
  | Weather | Rainy autumn in Seattle and the UK | weather, trail sales |

- **Answer key.** `retail_synthetic_truth` keeps `persons` and which source record belongs to which person, so
  identity resolution can be scored. A final `cleanup` stage drops the generator's scratch tables.

## 4. Silver: cleaning and identity resolution

Silver standardises each domain (commerce, behaviour, loyalty and running, marketing, service) with shared SQL
functions (`silver/00_functions`: email, phone, name, street and postal normalisation) and links every record to a
customer.

**Identity resolution** (`silver/02_identity_resolution`):

1. **Standardise** all source customer records into one table (`customer_source_records`).
2. **Match keys.** Deterministic keys are normalised email, E.164 phone and loyalty id, plus two cross-system
   links: a POS loyalty-card scan and a run-app order reference. Probabilistic keys are canonical first name + last
   name + postal code, and canonical first name + last name + birth date.
3. **Hub protection.** A key only creates matches if it is shared by 2–25 records, so shared or junk values (a
   family email, a store phone) cannot merge unrelated people.
4. **Connected components.** Matches form a graph. Min-label propagation with pointer jumping finds the groups,
   and each group becomes one `unified_customer_id`.
5. **Survivorship.** One best record per customer. Each field is taken from the most trusted source that has it
   (e-com > loyalty > app > POS > run app > race partner); phone is the most recently updated.
6. **Evaluation.** Clusters are scored against the answer key for purity (98.3%) and completeness (97.3%) and
   logged in `identity_resolution_eval`.

## 5. Gold: Customer 360 and Customer Lake tables

| Area | Tables | Notes |
|---|---|---|
| Customer 360 | `customer_profile`, `customer_consent`, `customer_identity_map`, `customer_traits` | One row per customer (identity map: one row per identifier). Consent is resolved per channel and jurisdiction (GDPR, UK-GDPR, CCPA, PIPEDA). |
| Scores | `customer_predictions` | Transparent rule-based scores (`heuristic_v1`): CLV, churn, shoe replacement date, drop hype, reseller risk. Easy to swap for trained models. |
| Products | `product_catalog_enriched`, `customer_product_recommendations` | Product feed per SKU (price today, stock, rating, fit). It keeps discontinued SKUs so past activity still joins. Recommendations are the top 5 in the customer's size. |
| Triggers | `trigger_events` | Signals computed as of the latest data day: abandoned cart or browse, back in stock, shoe mileage, race training start, rain for trail runners, tier upgrade, fit return. |
| Customer Lake | `household_profile`, `purchase_events`, `purchase_line_items`, 8 more `*_events` tables | One row per event, with a unique event id and a TIMESTAMP column. |
| Ops | `identity_resolution_stats` | One row per run: records, customers, dedup rate, matches. |

Rules for gold tables:

- Defined once in `ddl/03_gold.py` with typed columns, a description on every column, a primary key and **liquid
  clustering** (customer id, plus the event time for event tables). Predictive optimization maintains the
  clustering.
- Refreshed with `INSERT OVERWRITE ... BY NAME`, which is atomic and keeps the table's identity.
- The DDL helper is idempotent: on existing tables it only syncs descriptions and clustering keys.

## 6. CDP layer: `retail_cdp`

The Customer Lake entity points only at `retail_cdp`. It holds 14 views:

| Kind | Views | Rule |
|---|---|---|
| 1:1 pass-through | `customer_profile`, `customer_consent`, `customer_traits`, `trigger_events`, `support_events`, `review_events`, `race_registration_events`, `message_engagement_events`, `loyalty_events` | Same rows and columns as the gold table, with an explicit column list |
| Product-enriched events | `purchase_item_events`, `digital_product_events`, `return_item_events`, `run_shoe_events` | Event rows with the product name, category, sport and size added, because event filters can only use an event table's own columns |
| 1:1 helper | `customer_top_recommendation` | The #1 pick with product name, price, image and link, ready to map into sync fields |

Rules for `retail_cdp` views:

- **No view on a view.** Every view reads gold tables directly.
- **Created once** with `CREATE VIEW IF NOT EXISTS`, so job runs never replace them. To change a view, drop it on
  purpose and then re-point Customer Lake.
- **Separation of concerns.** Gold can be rebuilt and optimised freely. `retail_cdp` is the stable, curated
  contract for marketing.
- Customer Lake only needs `SELECT` on `retail_cdp`. Views run with their owner's rights.

## 7. Customer Lake model

- **Entity:** Retail - StrideForge Customer, profile `retail_cdp.customer_profile`, key `unified_customer_id`.
- **1:1:** consent, traits, top recommendation. Their columns can be bulk-created as attributes.
- **Event tables (10):** purchases (line level, with order totals), web/app, returns, runs, messages, loyalty,
  races, support, reviews, triggers.
- **Audiences live in Customer Lake, not in gold.** The earlier pre-built segment, export and campaign-measurement
  tables were removed. Campaign results come from engagement and purchase events flowing back into the lakehouse.

Purchase events are at line level, so "number of orders" needs a Count Distinct of `order_id` aggregation.
Order-level conditions use the `order_*` columns repeated on each line.

## 8. Pipelines and deployment

Everything is a notebook task in a Databricks Asset Bundle (`databricks.yml`, `resources/`):

- **`strideforge_bronze_setup`** (once per workspace): bronze, silver, gold and CDP DDL → generate bronze →
  validate the stories.
- **`strideforge_medallion`** (daily, schedule paused): DDL checks → silver (functions, reference, identity
  resolution, then the domains in parallel) → gold (Customer 360 → scores and products → triggers and Customer Lake
  tables). `ensure_cdp_views` runs alongside.
- **`strideforge_greenstar_enrichment`** (optional module, on demand): clean room enrichment with a consumer data
  provider. See section 12.

| Setting | Variables |
|---|---|
| Catalog and schemas | `catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `cdp_schema`, `truth_schema` |
| Data volume | `scale` (`1.0` = 1.5M customers; `0.01` for a quick test) |
| Targets | `dev` (development mode) and `prod` (shared root path) |

Every notebook includes `src/_params.py`, which reads the catalog and schema widgets.

**Built-in checks:**
- **Generator:** the stories are present.
- **Identity resolution:** purity and completeness against the answer key.
- **`gold_customer_lake`:** unique event ids, TIMESTAMP columns, one row per key for 1:1 objects, and `retail_cdp`
  row counts equal to gold. The task fails if a Customer Lake rule is broken.

## 9. Technical implementation

### 9.1 Stack

| Area | Technology |
|---|---|
| Language | Python 3.12, PySpark (Spark Connect), SQL (Databricks SQL, ANSI mode) |
| Data generation | Faker, numpy, pandas, run through `mapInPandas` / `applyInPandas` on Spark |
| Local development | Databricks Connect 17.3 on serverless (`DatabricksSession`) |
| Compute | Serverless jobs compute (notebook tasks, no cluster definitions) |
| Storage and governance | Unity Catalog, Delta Lake, liquid clustering, predictive optimization |
| AI | Databricks AI Functions: `ai_analyze_sentiment`, `ai_classify` |
| Deployment | Databricks Asset Bundles (Databricks CLI v1.18) |
| Source control | Git, GitHub (`ariforu/customer-lake-retail-demo`, branch `main`), Databricks Git folder |
| Diagrams | Mermaid, rendered with mermaid-cli (`mmdc`) |

### 9.2 Repository layout

```
databricks.yml                      bundle: name, variables, dev/prod targets
resources/
  strideforge_bronze_setup.job.yml  one-time job: DDL + generator + validation
  strideforge_medallion.job.yml     daily job: bronze -> silver -> gold, CDP views
  strideforge_greenstar_enrichment.job.yml  clean room module job (section 12)
src/
  _params.py                        shared widgets (catalog, schemas) + run() SQL helper
  ddl/01_bronze.py .. 04_cdp.py     schemas, tables, views (idempotent)
  generator/
    gen/                            generator library (one module per domain)
    generate_bronze.py              notebook entry point (job task)
    validate_bronze.py              notebook: checks the stories (logic in validate_stories.py)
    run.py, sf_session.py           local runner + Spark session handling
  silver/00_functions.py .. 08_*    bronze -> silver notebooks
  gold/01_customer_360.py .. 04_*   silver -> gold notebooks
  cleanroom/                        optional clean room enrichment module (section 12)
docs/                               Customer Lake setup guide, diagrams (.mmd + .png)
```

### 9.3 Notebooks

- Every step is a **source-format notebook** (`# Databricks notebook source`, `# COMMAND ----------`,
  `# MAGIC %md`), so it is a plain `.py` file in Git and a notebook in the workspace.
- Each notebook starts with a short header (what it does, reads, writes, which job task runs it), followed by
  numbered steps.
- `%run ../_params` creates widgets for `catalog`, `bronze`, `silver`, `gold`, `truth` and `cdp` (defaults to the
  demo names; jobs pass them as `base_parameters`). `run(sql)` replaces `${catalog}`, `${gold}` etc. and runs the
  statement, so the SQL stays readable and portable.
- The generator notebook installs Faker with `%pip install faker`.

### 9.4 Generator internals

- `gen/` has one module per domain: `dims`, `persons`, `profiles`, `messy`, `catalog`, `commerce`, `loyalty`,
  `digital`, `marketing`, `sport`, `service`, `ops`, `cleanup`, plus `config` and `refdata`.
- Rows are generated in parallel. `spark.range()` creates the work, and pandas functions (Faker plus numpy) build
  each partition. Results are written straight to Delta. There is no driver-side looping, and no `.cache()` or
  `.persist()` (not supported on serverless). Scratch tables go to `retail_synthetic_truth` and are dropped by the
  `cleanup` stage.
- **Shipping the code to the workers:**
  - *Locally:* Databricks Connect with `DatabricksEnv().withDependencies("faker")`, plus
    `cloudpickle.register_pickle_by_value(gen)` so the library travels with the functions.
  - *In a job:* `sf_session.get_runtime()` zips the `gen` package (file name includes a content hash) and uploads it
    with `spark.addArtifacts(zip, pyfile=True)` on Spark Connect, or `sparkContext.addPyFile` on classic clusters.
- **Configuration** comes from environment variables: `SF_CATALOG`, `SF_BRONZE_SCHEMA`, `SF_TRUTH_SCHEMA`,
  `SF_SCALE`, `SF_PROFILE`, `SF_IN_JOB`. The notebook sets them from its widgets.
- **Local runner:** `python run.py [stage ...]` runs all or selected stages (`dims persons profiles commerce loyalty
  digital marketing sport service ops cleanup`).

### 9.5 Spark and SQL patterns

| Pattern | Used for |
|---|---|
| SQL UDFs (`CREATE FUNCTION`, 8 in `silver/00_functions`) | Email, phone, name, street, postal and country normalisation, shared by all silver steps |
| `INSERT OVERWRITE ... BY NAME` | Full refresh of silver and gold tables, atomic and identity-preserving |
| `MERGE INTO` | Customer enrichment, AI review tags, next-best style on predictions |
| `QUALIFY row_number() OVER (...) = 1` | Latest version of each raw record, de-duplicated events, top-N per customer |
| Temp views | Readable multi-step SQL inside a notebook |
| PySpark DataFrames + scratch Delta tables | Connected components for identity resolution (iterative label propagation with pointer jumping) |
| `try_cast`, `try_element_at` | Safe casting and array indexing under ANSI mode |
| AI Functions | Sentiment, fit feedback and themes for the 20K newest reviews (rules for the rest) |

### 9.6 Unity Catalog features

- **Descriptions everywhere.** The gold DDL holds a `COMMENT` on every table and column. The `ddl()` helper parses
  each `CREATE TABLE IF NOT EXISTS` statement and re-applies the table comment, changed column comments and
  clustering keys to existing tables, so the DDL file is the source of truth.
- **Primary keys** (informational constraints) on every customer, household, product and event table.
- **Liquid clustering** (`CLUSTER BY`, applied to existing tables with `ALTER TABLE ... CLUSTER BY`).
  **Predictive optimization** (inherited from the metastore) runs `OPTIMIZE` automatically.
- **Table property** `cdp_activation = 'true'` marks objects that Customer Lake reads.
- **Views with column comments** in `retail_cdp`.
- **`information_schema`** is used by one-time migration cells (for example, replacing old views or tables
  without a primary key).

### 9.7 Databricks Asset Bundle

- Bundle `strideforge-cdp`, `include: resources/*.yml`.
- **Variables:** `catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `cdp_schema`, `truth_schema`, `scale`.
  Override them with `--var name=value`.
- **Targets:**
  - `dev` (default): `mode: development`, so jobs are prefixed with the user's name and schedules stay paused.
  - `prod`: `mode: production`, deployed under `/Workspace/Shared/.bundle/strideforge-cdp/prod`.
- **Jobs:** notebook tasks on serverless. A YAML anchor (`&params` / `*params`) shares the `base_parameters`
  across tasks. `max_concurrent_runs: 1`. The generator task has a 4-hour timeout. The medallion schedule is
  `0 0 5 * * ?` (05:00 UTC), paused.
- **Commands:**
  ```bash
  databricks bundle validate --strict
  databricks bundle deploy  [-t prod] [--var catalog=<catalog>]
  databricks bundle run strideforge_bronze_setup   # once per workspace
  databricks bundle run strideforge_medallion
  ```

### 9.8 Development workflow

1. Edit locally (notebooks are plain `.py` files). Test ad-hoc queries through Databricks Connect.
2. `databricks bundle validate --strict` and `databricks bundle deploy`.
3. Run the affected tasks (`databricks jobs run-now` with `only: [...]`) or the full job, and check the built-in
   validations.
4. Commit and push to GitHub `main`. The workspace Git folder pulls `main`.

## 10. Key decisions

| Decision | Why |
|---|---|
| Gold event tables are Delta tables, not views | Fast audience counts on 40–90M-row tables; clustering and primary keys |
| `INSERT OVERWRITE`, never drop and recreate | Dropping a table gives it a new identity, and Customer Lake loses it ("table not found") |
| `retail_cdp` views created once (`IF NOT EXISTS`) | Same reason: the entity's references stay valid across runs |
| Product details copied onto event rows | Event filters can't reach joined tables; "bought a CloudStride 3" must be one condition |
| Rule-based scores | Explainable in a demo; the table contract allows a drop-in ML model later |
| Descriptions in DDL, not in the UI | Versioned with the code; the Customer Lake assistant and Genie read them |
| Fixed demo FX rates (CAD 1.35, GBP 0.80, EUR 0.93) | Simple, deterministic USD reporting |

## 11. Limitations and next steps

- `customer_predictions` (CLV, churn, reseller risk) has no `retail_cdp` view yet. Add one with the same pattern
  as `customer_traits`.
- Household data is thin (few multi-member households). A Household entity works but is not a strong demo.
- Customer Lake (Beta) has no bundle or API support for entities and attributes, so they are set up in the UI
  following `docs/customer_lake_setup.md`.
- Changing a `retail_cdp` view means dropping it and re-pointing the entity, so plan view changes.

## 12. Module: clean room enrichment (Greenstar Consumer Data)

An optional, self-contained module. It shows how a **Databricks Clean Room** lets StrideForge enrich its customer
profiles with a third-party consumer data provider's file, where neither side sees the other's raw data. The
provider, **Greenstar Consumer Data**, is fictional; it stands in for a commercial consumer-data product. Nothing
outside `src/cleanroom/`, `resources/strideforge_greenstar_enrichment.job.yml` and two bundle variables changes, and
the existing jobs and `retail_cdp` views are untouched.

### 12.1 What it shows

1. **Detailed data in, summaries out.** Greenstar holds exact date of birth, income, home value, occupation title,
   20 interest scores, marital status and number of children. StrideForge receives only summary fields:
   `generation` (Gen Z, Millennial, Gen X, Baby Boomer, ...), `income_band`, `homeowner`, `occupation_category`,
   `top_interests` (top 3) and `life_stage`.
2. **Gap filling.** Where StrideForge has no phone or birth date, Greenstar's phone (as-is) and age band fill it.
   First-party values always win.
3. **Customer Lake.** The result is a 1:1 related object on the existing StrideForge Customer entity, so audiences
   like "Millennial homeowners whose phone came from Greenstar" work.

### 12.2 Parties and workspaces

| | StrideForge (retailer) | Greenstar Consumer Data (provider) |
|---|---|---|
| Workspace (CLI profile) | `fevm-vijay` | `partner-demo-catalog` |
| Metastore | `aws:us-east-1:c0da88f1-…` | `aws:us-east-1:9bfdb312-…` |
| Clean room alias | `strideforge` | `greenstar` |
| Role | Creates and owns the room; **runs** the notebooks; receives the output | Shares its table; **writes** the notebooks |
| Shares | `vijay_catalog.retail_cleanroom.cr_match_keys` | `greenstar_demo.consumer.person_attributes` |

Clean room: `strideforge_greenstar_enrichment` (central room on AWS us-east-1, **restricted internet egress**),
output catalog `strideforge_greenstar_enrichment_output` on the StrideForge side.

```
 StrideForge (fevm-vijay)                    clean room (serverless, no internet)            Greenstar (partner-demo-catalog)
 retail_gold.customer_profile                                                                  consumer.person_attributes
   + customer_consent                                                                          (50K people, detailed)
        |  01_refresh_match_keys                                                                       |
        v                                                                                              |
 retail_cleanroom.cr_match_keys  ----share---->  greenstar_enrich_customers  <----share--------------- +
 (hashed email, country, gap flags)              (Greenstar's notebook, run by StrideForge)
                                                          |
                                                          v
                                  <clean room>_output.output_schema_<run>.customer_enrichment (30 days)
                                                          |  02_save_enrichment
                                                          v
                                  retail_cleanroom.customer_enrichment  -->  retail_cdp.customer_enrichment  -->  Customer Lake
```

### 12.3 Data

**Greenstar `consumer.person_attributes`** (50,000 rows, synthetic, deterministic from the email hash):
`person_id`, `email_sha256`, `country`, `date_of_birth`, `phone_number` (E.164, country formats), `household_income`,
`home_ownership`, `home_value`, `occupation_title`, `education`, `marital_status`, `num_children`,
`interest_scores` (MAP of 20 affinities), `last_verified_date`.

| Segment | Rows | Purpose |
|---|---|---|
| StrideForge customers with a phone or birth-date gap | 15,000 | Shows gap filling |
| StrideForge customers with no gap | 5,000 | Shows "first-party wins"; 5% have a date of birth 1–3 years off, to show conflicts |
| People StrideForge doesn't know | 30,000 | Realistic match rate (~2% of StrideForge's shared customers, 40% of Greenstar's file) |

`scripts/setup_provider.py` builds it: it samples the matching customers on the StrideForge side (hash, country and
birth date only), writes them to a temporary `greenstar_demo.staging` schema, generates every attribute in SQL
(`sql/greenstar_person_attributes.sql`) and drops the staging schema. This copy step is demo staging only; a real
provider already has its own file.

**StrideForge `retail_cleanroom.cr_match_keys`** (~1.03M rows): `unified_customer_id`, `email_sha256`
(SHA-256 of the lower-cased, trimmed email), `country`, `has_phone`, `has_birth_date`. Only customers with
`personalization_profiling` consent who are not globally suppressed. Emails shared by two customers are left out so
a match maps to exactly one customer.

**Output `customer_enrichment`** (one row per matched customer): `generation`, `age_band` (only when StrideForge
has no birth date), `phone_e164` (only when StrideForge has no phone), `income_band`, `homeowner`,
`occupation_category`, `top_interests`, `has_children`, `life_stage`, `fields_enriched`, `enriched_at`.

| Greenstar detail | Summary rule (in Greenstar's notebook) |
|---|---|
| `date_of_birth` | Generation by birth year: Gen Alpha 2013+, Gen Z 1997–2012, Millennial 1981–1996, Gen X 1965–1980, Baby Boomer 1946–1964, Silent before 1946. Age band in StrideForge's bands (16-24 … 55+) |
| `household_income` | `<50K`, `50-100K`, `100-150K`, `150K+` (USD-equivalent) |
| `home_ownership`, `home_value` | `homeowner` boolean |
| `occupation_title` (20 titles) | 7 categories (Professional, Healthcare & Education, Skilled Trades, Service & Retail, Business Owner, Student, Retired) |
| `interest_scores` | Up to 3 labels with affinity ≥ 0.6, strongest first |
| age, `marital_status`, `num_children` | `has_children`; `life_stage` (Retiree 67+, Empty Nester 55+, Young/Established Family, Couple No Kids, Young Single, Single Adult) |

**Customer Lake view `retail_cdp.customer_enrichment`**: one row per customer (1:1 with `customer_profile`), with
`greenstar_matched`, best `phone_e164` / `age_band` / `generation` plus `*_source` (`first_party` or `greenstar`),
`generation_conflict`, and the Greenstar summaries. Generation is computed from StrideForge's own birth date when it
has one.

First full run: 20,000 matched; phone added for 6,090; age band and generation added for 11,829; 36 generation
conflicts (first-party kept).

### 12.4 Privacy controls

- **Hashed match key only.** StrideForge shares a SHA-256 email hash, country and two gap flags; no names, raw
  emails, phones or birth dates.
- **Consent gate.** Only customers who allow profiling (and are not suppressed) are shared.
- **Summaries, not records.** Greenstar's notebook decides what leaves the room. Exact date of birth, income, home
  value, occupation title, interest scores and Greenstar's `person_id` never leave.
- **Minimum match guard.** The notebook refuses to run with fewer than 100 matches, so a tiny list can't be used to
  look up individuals.
- **Approval.** Every notebook version needs approval from all collaborators except the uploader. The room's default
  auto-approval rule approves, on StrideForge's behalf, Greenstar notebooks that only StrideForge runs (the output
  only reaches StrideForge). Greenstar approves by uploading. Any change creates a new version that must be approved
  again.
- **Output goes only to the runner.** Greenstar never sees which customers matched or the results.
- **No internet egress** from clean room compute (`RESTRICTED_ACCESS`, fixed at creation).

### 12.5 Running it

```
# once: provider data (partner-demo-catalog), then the clean room
python3 src/cleanroom/scripts/setup_provider.py
python3 src/cleanroom/scripts/setup_clean_room.py --step create      # room + output catalog
python3 src/cleanroom/scripts/setup_clean_room.py --step notebooks   # Greenstar uploads its notebooks
databricks bundle deploy -t prod --profile fevm-vijay --var catalog=vijay_catalog
databricks bundle run strideforge_greenstar_enrichment --only ddl,refresh_match_keys -t prod --profile fevm-vijay --var catalog=vijay_catalog
python3 src/cleanroom/scripts/setup_clean_room.py --step tables      # both parties share their table
python3 src/cleanroom/scripts/setup_clean_room.py --step status

# every run (~3-4 minutes)
databricks bundle run strideforge_greenstar_enrichment -t prod --profile fevm-vijay --var catalog=vijay_catalog
```

Job `strideforge_greenstar_enrichment`: `ddl` (`target/00_ddl`) → `refresh_match_keys` → `enrich` (**clean room
notebook task**, `greenstar_enrich_customers`) → `save_enrichment`, which reads the run's output schema through
`{{tasks.enrich.output.catalog_name}}` / `{{tasks.enrich.output.schema_name}}`.

The order matters because the job definition references the room and its notebook: the room and the notebook must
exist before `bundle deploy`, and `cr_match_keys` must exist before StrideForge can share it.

Demo script: `docs/cleanroom_demo_script.md` (also published as a Google Doc).

### 12.6 Files

```
src/cleanroom/
  scripts/_dbsql.py                     local helper: SQL and CLI calls through the Databricks CLI profile
  scripts/setup_provider.py             one-time: Greenstar data on the provider workspace
  scripts/setup_clean_room.py           one-time: room, output catalog, shared tables, notebooks, approvals
  sql/greenstar_person_attributes.sql   provider table DDL + synthetic generation
  clean_room_notebooks/                 Greenstar's notebooks (uploaded into the room by the provider)
    greenstar_validate_access.py        counts on both sides (storage/access smoke test)
    greenstar_enrich_customers.py       match + summarise
  target/                               StrideForge notebooks (bundle job tasks)
    _cr_params.py                       adds the cleanroom schema + output widgets to ../../_params
    00_ddl.py                           retail_cleanroom schema/tables + retail_cdp.customer_enrichment view
    01_refresh_match_keys.py            consented hashed-email keys
    02_save_enrichment.py               copy the run output (expires in 30 days) into a stable table
resources/strideforge_greenstar_enrichment.job.yml
```

### 12.7 Decisions

| Decision | Chosen | Rejected / why |
|---|---|---|
| Story | Data enrichment from a consumer data provider | Retail media audience overlap / campaign lift; co-marketing with a race organizer. Enrichment best shows detailed-to-summary rules |
| Provider identity | Fictional "Greenstar Consumer Data"; no real provider named in tables or workspace objects | Using the real product's name |
| Workspaces | `partner-demo-catalog` (provider) and `fevm-vijay` (StrideForge) | Clean rooms need different metastores, which these are. Same region is not required (collaborators can be on any cloud or region) |
| Match key | SHA-256 of the normalised email only | Hashed phone as a fallback (awkward when phone is also an output); fuzzy name + postal code (puts raw PII in the room) |
| Volume | 50K provider rows (15K gap, 5K full, 30K unmatched) | 500K: slower runs for no extra story. Small enough for interactive runs |
| Outputs | Phone as-is (gap only) + 6 summaries + generation/age band | A smaller set (generation, phone, income band, life stage) |
| Who creates the room | StrideForge (retailer-led, owns the room and egress policy) | Greenstar-led: doesn't avoid StrideForge needing `CREATE CLEAN ROOM` anyway (needed to get a sharing identifier) |
| Who writes / runs the notebook | Greenstar writes, StrideForge runs | StrideForge writing it would let the retailer write code that extracts raw provider data |
| Shared StrideForge object | A table (`cr_match_keys`) refreshed with `INSERT OVERWRITE` | Sharing `customer_profile` (far too much); a view (works, but a table keeps the clean room asset simple and its identity stable) |
| Consent | Share only `personalization_profiling` customers who are not suppressed | Sharing everyone with an email |
| Customer Lake | New 1:1 view `retail_cdp.customer_enrichment`, one row per customer, first-party wins, `*_source` columns | Changing `retail_cdp.customer_profile` (breaks the 1:1 pass-through rule and needs a view migration) |
| Output persistence | Copy each run into `retail_cleanroom.customer_enrichment` (full snapshot) | Pointing Customer Lake at output tables: they are per-run schemas and expire after 30 days |
| Packaging | Separate module: setup scripts + bundle job + UI demo script | UI-only (not repeatable). Bundles can't manage clean rooms, so creation is scripted with the CLI |
| Approval | Keep the room's default auto-approval rules | Deleting them for a manual approval moment on stage |
| Egress | `RESTRICTED_ACCESS` (no internet from clean room compute) | Full access: nothing in the notebooks needs the internet |
| Permission | Time-boxed `CREATE CLEAN ROOM` grant on the FEVM us-east-1 metastore (FEVM "Clean Room Collaboration", 5 days) | The room keeps working after the grant expires; creating a new room needs a new grant |
| Network | Fine as is: FEVM's VPN-only IP access list protects the workspace UI/API, not Delta Sharing or clean room compute. `vijay_catalog` is on a customer S3 bucket; `greenstar_validate_access` proved clean room compute can read it | — |

### 12.8 Limitations

- The clean room and its assets are created by script, not by the bundle (no bundle resource for clean rooms yet).
- Greenstar's attributes are synthetic and uncorrelated with StrideForge behaviour, apart from a running and fitness
  interest boost for matched customers.
- `retail_cleanroom.customer_enrichment` is a full snapshot per run; customers who stop matching lose their enrichment.
- Output tables in the clean room output catalog expire after 30 days; the saved copy does not.
