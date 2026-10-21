# Customer Lake setup for StrideForge

How to model the StrideForge gold layer as **Customer Lake** entities, attributes and audiences.
Reference: [Set up entities](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/customer-data/set-up-entities),
[Create attributes](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/customer-data/create-attributes),
[Create audiences](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/create-audiences).

All objects live in `customer_lake_si.retail_gold` (replace with your catalog/schema if deployed elsewhere).
They are plain Delta tables, refreshed by the `strideforge_medallion` job (`gold_customer_lake` task).
Every table has a primary key and a description on each column.

## 0. Prerequisites
- Grant Customer Lake access to the data ([Grant access to your data](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/set-up/configure-workspace#grant-data-access)):
  `USE CATALOG` on `customer_lake_si`, `USE SCHEMA` + `SELECT` on `customer_lake_si.retail_gold`.
- Entity budget: an instance supports **5 entities**; this design uses **2** (Customer, Household).

## 1. Entity: Customer

**Entities → + New entity**

| Wizard field | Value |
|---|---|
| Name | `Customer` |
| Description | One unified StrideForge customer (identity-resolved across POS, e-com, app, loyalty, run app) |
| Select table | `retail_gold.customer_profile` |
| Primary key | `unified_customer_id` |

**Manage relationships** (join every table on `unified_customer_id = unified_customer_id`):

| Related table | Cardinality | Notes |
|---|---|---|
| `customer_consent` | 1:1 | consent flags (also use in connector rules) |
| `customer_traits` | 1:1 | RFM, lifecycle, affinities, run miles, upcoming / goal race |
| `customer_predictions` | 1:1 | CLV, churn, replacement date, drop hype, reseller risk |
| `customer_product_recommendations` | 1:many | aggregate with `List` (e.g. top SKUs) |
| `customer_identity_map` | 1:many | aggregate with `Count Distinct` / `List` on `id_type` |
| `purchase_events` | **event table** | event ID `order_id`, timestamp `order_ts` |
| `purchase_line_items` | 1:many **from `purchase_events`** | join `order_id = order_id` (one row per order line, with `sku`) |
| `purchase_item_events` | **event table** (view) | event ID `order_line_id`, timestamp `order_ts`; one row per order line with order **and product** details: the easiest way to filter purchases by product name |
| `return_events` | **event table** | event ID `return_id`, timestamp `return_ts` |
| `digital_events` | **event table** | event ID `event_id`, timestamp `event_ts` |
| `message_engagement_events` | **event table** | event ID `message_event_id`, timestamp `event_ts` |
| `run_activity_events` | **event table** | event ID `activity_id`, timestamp `start_ts` |
| `loyalty_events` | **event table** | event ID `ledger_id`, timestamp `activity_ts` |
| `race_registration_events` | **event table** | event ID `registration_id`, timestamp `registered_at` |
| `support_events` | **event table** | event ID `ticket_id`, timestamp `created_at` |
| `review_events` | **event table** | event ID `review_id`, timestamp `submitted_at` |

**Product data.** `product_catalog_enriched` (key `sku`) is not keyed by customer, so relate it to the tables that
carry a `sku`, each as **many:1** on `sku = sku`:
`purchase_line_items`, `customer_product_recommendations`, `digital_events`, `return_events`, `review_events`,
`run_activity_events` (the shoe used). This makes product fields such as `category`, `sport`, `current_price`,
`is_on_markdown`, `stock_status`, `is_new_arrival` and `is_limited_drop` usable in attributes and event filters.

### Recommended Customer attributes
- **Column attributes (bulk-create)** from `customer_profile`, `customer_consent`, `customer_traits`, `customer_predictions`:
  `country`, `state`, `city`, `gender`, `age_band`, `loyalty_tier`, `points_balance`, `shoe_size_us`, `home_store_id`,
  `email_contactable`, `sms_contactable`, `push_contactable`, `paid_media_eligible`, `do_not_sell`, `global_suppression`,
  `lifecycle_stage`, `rfm_segment`, `top_category`, `top_sport`, `discount_sensitivity`, `days_since_last_order`,
  `order_count_12m`, `revenue_12m`, `return_rate`, `fit_return_count`, `run_miles_90d`, `goal_race_date`,
  `predicted_clv_12m`, `clv_tier`, `churn_probability`, `active_shoe_miles`, `predicted_shoe_replacement_date`,
  `drop_hype_score`, `reseller_risk_score`, `best_send_hour`.
- **Aggregation attributes**
  - `Total Spend (90d)` = Sum of `purchase_events.revenue_usd`, last 90 days
  - `Orders (Lifetime)` = Count of `purchase_events.order_id`
  - `Email Opens (30d)` = Count of `message_engagement_events.message_event_id` where `event_type = 'open'`, last 30 days
  - `Run Miles (30d)` = Sum of `run_activity_events.distance_miles`, last 30 days
  - `Recommended SKUs` = List of `customer_product_recommendations.sku`
- **Event attributes** (for event filters): `purchase_item_events.style_name`, `.category`, `.sport`, `.gender`,
  `.size`, `.is_limited_drop`, `.revenue_usd`; `purchase_events.channel`, `.categories`, `.has_running_shoes`,
  `.has_cloudstride_3`, `.revenue_usd`; `return_events.return_reason`, `.is_fit_related`, `.style_name`;
  `digital_events.event_type`, `.category`, `.sku`; `message_engagement_events.event_type`, `.channel`, `.campaign_id`;
  `run_activity_events.activity_type`, `.distance_miles`; `race_registration_events.event_type`, `.event_date`;
  `support_events.category`; `review_events.rating`, `.ai_fit_feedback`.
- **Product attributes** (through the `sku` joins): `product_catalog_enriched.category`, `.sport`, `.style_name`,
  `.current_price`, `.is_on_markdown`, `.stock_status`, `.is_new_arrival`, `.is_limited_drop`, `.fit_feedback`.

## 2. Entity: Household

| Wizard field | Value |
|---|---|
| Name | `Household` |
| Select table | `retail_gold.household_profile` |
| Primary key | `household_id` |

| Related table | Cardinality | Join |
|---|---|---|
| `customer_profile` | 1:many | `household_id = household_id` (aggregate: Count members, List emails, Max `loyalty_tier`) |

Column attributes: `member_count`, `has_kids`, `kids_age_bands`, `household_revenue_12m`, `household_predicted_clv_12m`,
`any_email_contactable`, `any_paid_media_eligible`, `top_sport`, `city`, `state`, `country`, `home_store_id`.

## 3. Audiences to build in Customer Lake

| Audience | Entity | Conditions |
|---|---|---|
| Runners due for new shoes | Customer | `active_shoe_miles >= 400` AND `email_contactable = true` |
| CloudStride 3 fit-return save | Customer | event `return_events` where `has` `style_name LIKE 'CloudStride 3%'` and `return_reason = too_small` in last 120 days |
| Goal race in 8–20 weeks | Customer | `goal_race_date` between today+56d and today+140d |
| High value at risk | Customer | `clv_tier in (platinum, gold)` AND `churn_probability > 0.5` |
| Lapsed win-back | Customer | `lifecycle_stage = lapsed` AND `discount_sensitivity != full_price` AND `email_contactable` |
| Sneakerheads | Customer | `drop_hype_score >= 0.5` **but not** audience *Reseller suspects* |
| Reseller suspects | Customer | `reseller_risk_score >= 0.6` (use as a **connector rule blocklist** for drop syncs) |
| Back-to-school families | Household | `has_kids = true` AND `any_email_contactable = true` |
| Close to Elite | Customer | `loyalty_tier = Pro` AND `revenue_12m between 800 and 999` |
| Lookalike seed | Customer | `predicted_clv_12m` in top 5% AND `paid_media_eligible` |
| Recent purchasers (suppress) | Customer | event `purchase_events` in last 14 days (use as a blocklist on prospecting syncs) |
| Bought trail shoes recently | Customer | event `purchase_item_events` in last 30 days where `category = Trail` |
| Bought a CloudStride 3 | Customer | event `purchase_item_events` in last 180 days where `style_name` contains `CloudStride 3` |
| Viewed a limited drop | Customer | event `digital_events` `event_type = product_view` in last 7 days where product `is_limited_drop = true` |
| Recommended item on markdown | Customer | `customer_product_recommendations` product `is_on_markdown = true` AND `stock_status = in_stock` |
| Email fatigued | Customer | `email_contactable` AND `Email Opens (30d) = 0` |
| Trail runners + rain | Customer | `top_sport = Trail Running` (the rain-forecast condition is in `gold.trigger_events`) |

Connector rules: block `global_suppression = true` everywhere, `do_not_sell = true` / `paid_media_eligible = false`
on ad platforms, and *Reseller suspects* on drop-related syncs.

To filter purchases by product (name, category, sport, size ...), use the `purchase_item_events` event table: event
attributes can only use an event table's own columns, and this view already has the product details on each line.
