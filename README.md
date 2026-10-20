# StrideForge CDP demo

A complete Customer Data Platform demo on Databricks for **StrideForge**, a made-up sportswear brand
(1.5M customers in the US, UK, Germany and Canada, Oct 2023 – Oct 2026).

It generates realistic raw data, cleans it, works out which records belong to the same person, and builds the
customer tables that **Customer Lake** uses for audiences and activation. Everything is packaged as a
Databricks Asset Bundle, so it can be deployed to any workspace.

## The three layers

| Layer | Schema | What's in it |
|---|---|---|
| Bronze | `retail_bronze` | 42 raw tables, as they would arrive from each system (stores, website, app, loyalty, email, ads, support, …) |
| Silver | `retail_silver` | 37 cleaned tables, with every record linked to one customer id |
| Gold | `retail_gold` | 20 ready-to-use tables: the customer view, scores, product feed, journey triggers and the Customer Lake event tables |

`retail_synthetic_truth` holds the generator's "answer key" (which records really belong to whom), used to check
identity-resolution accuracy. Every gold table and column has a description in Unity Catalog, and every gold
table uses liquid clustering (customer id, plus the event time for event tables).

## Stories in the data

1. **Messy identity** – 4.0M source records for 1.5M people; identity resolution merges them with 98% accuracy.
2. **A shoe that runs small** – CloudStride 3 (launched Mar 2026) gets ~43% online returns for "too small" until size guidance is added in July.
3. **Worn-out running shoes** – the run app tracks miles per shoe; runners past 400 miles who don't replace them drift away.
4. **Too many emails** – Black Friday 2025 over-mailing makes unsubscribes jump ~30x for a week.
5. **Resellers** – bot-like accounts sharing addresses buy up limited drops.
6. **Seasons and weather** – peaks in January, August and Black Friday–December; a rainy autumn in Seattle and the UK.

## How it runs

Two jobs. Every step is a notebook with a short "what it does / reads / writes" note at the top.

**1. `strideforge_bronze_setup`** – run once per workspace
1. Create the bronze, silver and gold tables
2. Generate the fake data into bronze
3. Check the data and the stories

**2. `strideforge_medallion`** – bronze → silver → gold (can run daily; schedule is paused)

| Step | What it does |
|---|---|
| `silver_functions`, `silver_reference` | Cleanup functions; products, stores, prices, inventory, weather |
| `silver_identity_resolution` | Match records across systems and give each person one `unified_customer_id` |
| `silver_commerce`, `silver_behavior`, `silver_loyalty_sport` | Orders and returns, web/app activity, loyalty and running – all linked to customers |
| `silver_marketing`, `silver_service`, `silver_ai_review_enrichment` | Campaigns and messages; support, surveys and reviews (AI tags on the newest 20K reviews) |
| `silver_customer_enrichment` | Addresses, households, shoe/apparel sizes, consent |
| `gold_customer_360` | One row per customer: profile, consent, identifiers, behaviour; identity-resolution stats |
| `gold_predictions_products` | Simple rule-based scores, top-5 product picks, product feed |
| `gold_triggers` | Journey triggers (abandoned cart, worn-out shoes, rain on trail runners ...) |
| `gold_customer_lake` | Household table, purchase line items and 9 event tables for Customer Lake |

## Customer Lake

Point one entity at the gold tables:

| Entity | Profile table (key) | Joined tables |
|---|---|---|
| Retail - StrideForge Customer | `customer_profile` (`unified_customer_id`) | 1:1 `customer_consent`, `customer_traits`, `customer_predictions`; many:1 `household_profile`; 1:many `customer_product_recommendations`, `customer_identity_map`; 9 event tables (`purchase_events`, `return_events`, `digital_events`, `message_engagement_events`, `run_activity_events`, `loyalty_events`, `race_registration_events`, `support_events`, `review_events`); `purchase_line_items` (1:many from `purchase_events`); `product_catalog_enriched` (many:1 on `sku`) |

Audiences are built in Customer Lake, not in gold. Campaign results (opens, clicks, conversions) are measured from the event tables. Exact wizard settings, suggested attributes and example audiences: [docs/customer_lake_setup.md](docs/customer_lake_setup.md).

## Gold data model

![Gold ER diagram](docs/gold_er.png)

![How the gold tables are used](docs/gold_data_model.png)

## Deploy

Needs the Databricks CLI, Unity Catalog and serverless jobs.

```bash
# this workspace (data already loaded)
databricks bundle deploy
databricks bundle run strideforge_medallion

# a new workspace (create the catalog first)
databricks bundle deploy -t prod --profile <profile> --var catalog=<catalog>
databricks bundle run strideforge_bronze_setup -t prod --profile <profile> --var catalog=<catalog>   # ~25 min
databricks bundle run strideforge_medallion   -t prod --profile <profile> --var catalog=<catalog>   # ~25 min
```

Add `--var scale=0.01` for a quick test (15K customers).

## Folders

```
databricks.yml        bundle settings (catalog, schema names, scale, targets)
resources/            the two job definitions
src/_params.py        shared catalog/schema widgets
src/ddl/              create the bronze, silver and gold tables
src/generator/        fake-data generator (notebooks + gen/ library; run.py runs it locally via Databricks Connect)
src/silver/           bronze -> silver notebooks
src/gold/             silver -> gold notebooks
docs/                 diagrams and the Customer Lake setup guide
```
