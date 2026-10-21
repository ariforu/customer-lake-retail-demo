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

## 9. Key decisions

| Decision | Why |
|---|---|
| Gold event tables are Delta tables, not views | Fast audience counts on 40–90M-row tables; clustering and primary keys |
| `INSERT OVERWRITE`, never drop and recreate | Dropping a table gives it a new identity, and Customer Lake loses it ("table not found") |
| `retail_cdp` views created once (`IF NOT EXISTS`) | Same reason: the entity's references stay valid across runs |
| Product details copied onto event rows | Event filters can't reach joined tables; "bought a CloudStride 3" must be one condition |
| Rule-based scores | Explainable in a demo; the table contract allows a drop-in ML model later |
| Descriptions in DDL, not in the UI | Versioned with the code; the Customer Lake assistant and Genie read them |
| Fixed demo FX rates (CAD 1.35, GBP 0.80, EUR 0.93) | Simple, deterministic USD reporting |

## 10. Limitations and next steps

- `customer_predictions` (CLV, churn, reseller risk) has no `retail_cdp` view yet. Add one with the same pattern
  as `customer_traits`.
- Household data is thin (few multi-member households). A Household entity works but is not a strong demo.
- Customer Lake (Beta) has no bundle or API support for entities and attributes, so they are set up in the UI
  following `docs/customer_lake_setup.md`.
- Changing a `retail_cdp` view means dropping it and re-pointing the entity, so plan view changes.
