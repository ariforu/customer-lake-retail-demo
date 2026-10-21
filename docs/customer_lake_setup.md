# Customer Lake setup for StrideForge

How to model StrideForge as a **Customer Lake** entity with attributes and audiences.
Reference: [Set up entities](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/customer-data/set-up-entities),
[Create attributes](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/customer-data/create-attributes),
[Create audiences](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/create-audiences).

Point Customer Lake at **`customer_lake_si.retail_cdp`** (replace the catalog if deployed elsewhere). It holds the
14 views the entity uses, created by notebook `ddl/04_cdp`:

- 1:1 views over the gold tables (same rows and columns), and
- event views that add product details to purchases, web/app events, runs and returns.

All views read the `retail_gold` Delta tables, which the `strideforge_medallion` job refreshes. Every view and column
has a description. Views are created once (`IF NOT EXISTS`), so job runs never replace them and the entity keeps
working.

## 0. Prerequisites
- Grant Customer Lake access ([Grant access to your data](https://docs.databricks.com/aws/en/custmrlke/campaign-agents/set-up/configure-workspace#grant-data-access)):
  `USE CATALOG` on `customer_lake_si`, `USE SCHEMA` + `SELECT` on `customer_lake_si.retail_cdp` (or use **Grant
  tables** in the relationship editor). Views run with their owner's rights, so no access to `retail_gold` is needed.

## 1. Entity: Customer

**Entities → + New entity**

| Wizard field | Value |
|---|---|
| Name | `Retail - StrideForge Customer` |
| Description | One unified StrideForge customer (identity-resolved across POS, e-com, app, loyalty, run app) |
| Select table | `retail_cdp.customer_profile` |
| Primary key | `unified_customer_id` |

**Manage relationships**. Every join is `unified_customer_id = unified_customer_id`:

| Related object (`retail_cdp`) | Cardinality | Event ID / timestamp | What it adds |
|---|---|---|---|
| `customer_consent` | 1:1 | | Consent flags per channel (also use in connector rules) |
| `customer_traits` | 1:1 | | RFM, lifecycle, top category/sport, spend, returns, run miles, goal race |
| `customer_top_recommendation` | 1:1 | | #1 recommended product: name, price, markdown, stock, image URL, product URL |
| `purchase_item_events` | **event table** | `order_line_id` / `order_ts` | One row per order line: product name, category, sport, size, line and **order** totals, promo, payment, UTMs |
| `digital_product_events` | **event table** | `event_id` / `event_ts` | Web/app events with product name, sport, size, limited drop, today's price/markdown/stock |
| `return_item_events` | **event table** | `return_id` / `return_ts` | Returns with product, size, reason, original order channel, refund (USD) |
| `run_shoe_events` | **event table** | `activity_id` / `start_ts` | Runs with the shoe model, size and its total miles |
| `message_engagement_events` | **event table** | `message_event_id` / `event_ts` | Email/SMS/push sends, opens, clicks, unsubscribes |
| `loyalty_events` | **event table** | `ledger_id` / `activity_ts` | Points earned, redeemed, tier changes |
| `race_registration_events` | **event table** | `registration_id` / `registered_at` | Race sign-ups with race date and distance |
| `support_events` | **event table** | `ticket_id` / `created_at` | Support tickets with category, status, sentiment |
| `review_events` | **event table** | `review_id` / `submitted_at` | Product reviews with rating and AI fit/sentiment |
| `trigger_events` | **event table** | `trigger_id` / `trigger_ts` | Journey triggers (cart abandoned, shoe mileage, rain for trail runners ...) |

`purchase_item_events` has one event per **order line**. Conditions on order totals (`order_revenue_usd`,
`is_full_price`, `payment_type`) work directly; to count **orders**, use an aggregation attribute with Count Distinct
of `order_id` (an event count counts lines).

### Recommended attributes
- **Column attributes (bulk-create)** on the Tables tab:
  - `customer_profile`: `country`, `state`, `city`, `gender`, `age_band`, `loyalty_tier`, `points_balance`,
    `shoe_size_us`, `home_store_id`, `email_contactable`, `sms_contactable`, `push_contactable`, `paid_media_eligible`
  - `customer_consent`: `do_not_sell`, `global_suppression`
  - `customer_traits`: `lifecycle_stage`, `rfm_segment`, `top_category`, `top_sport`, `discount_sensitivity`,
    `days_since_last_order`, `order_count_12m`, `revenue_12m`, `return_rate`, `fit_return_count`, `run_miles_90d`,
    `goal_race_date`
  - `customer_top_recommendation`: all columns (map `rec_product_name`, `rec_image_url`, `rec_product_url` in syncs)
- **Event attributes** (+ New attribute → Create an event attribute). Event filters can only use an event table's own
  columns, so create the ones your audiences need:
  - `purchase_item_events`: `style_name`, `category`, `sport`, `size`, `is_limited_drop`, `order_revenue_usd`,
    `is_full_price`, `promo_code`, `channel`
  - `digital_product_events`: `event_type`, `style_name`, `category`, `is_limited_drop`
  - `return_item_events`: `style_name`, `return_reason`, `is_fit_related`, `size`
  - `run_shoe_events`: `shoe_name`, `shoe_total_miles`, `distance_miles`
  - `message_engagement_events`: `event_type`, `channel`, `campaign_id`
  - `race_registration_events`: `event_type`, `event_date`; `support_events`: `status`, `category`;
    `review_events`: `rating`, `ai_fit_feedback`; `trigger_events`: `trigger_type`
- **Aggregation attributes**
  - `Total Spend (90d)` = Sum of `purchase_item_events.revenue_usd`, last 90 days
  - `Orders (90d)` = Count Distinct of `purchase_item_events.order_id`, last 90 days
  - `Email Opens (30d)` = Count of `message_engagement_events.message_event_id` where `event_type = 'open'`, last 30 days
  - `Run Miles (30d)` = Sum of `run_shoe_events.distance_miles`, last 30 days

## 2. Audiences to build

| Audience | Conditions |
|---|---|
| CloudStride 3 fit-return save | event `return_item_events` in last 180 days where `style_name` contains `CloudStride 3` and `return_reason = too_small`; `email_contactable`; **but not** event `support_events` where `status` in (open, pending) |
| CloudStride 3 cart abandoners | event `digital_product_events` in last 7 days where `event_type = add_to_cart` and `style_name` contains `CloudStride 3`, **but not** event `purchase_item_events` in last 7 days where `style_name` contains `CloudStride 3` |
| Running in a worn-out CloudStride 2 | event `run_shoe_events` in last 30 days where `shoe_name` contains `CloudStride 2` and `shoe_total_miles >= 400` |
| Bought trail shoes recently | event `purchase_item_events` in last 30 days where `category = Trail` |
| Big-basket full-price buyers | event `purchase_item_events` in last 90 days where `order_revenue_usd > 200` and `is_full_price = true` |
| Goal race in 8–20 weeks | `goal_race_date` between today+56d and today+140d |
| Lapsed win-back | `lifecycle_stage = lapsed` AND `discount_sensitivity != full_price` AND `email_contactable` |
| Close to Elite | `loyalty_tier = Pro` AND `revenue_12m` between 800 and 999 |
| Top pick on markdown (email) | `rec_is_on_markdown = true` AND `rec_stock_status = in_stock` AND `email_contactable` |
| Recent purchasers (suppress) | event `purchase_item_events` in last 14 days (blocklist on prospecting syncs) |
| Email fatigued | `email_contactable` AND `Email Opens (30d) = 0` |
| Trail runners + rain | event `trigger_events` in last 3 days where `trigger_type = weather_rain` |

Connector rules: block `global_suppression = true` everywhere, and `do_not_sell = true` / `paid_media_eligible =
false` on ad platforms.

Scores such as CLV tier, churn probability, drop hype and reseller risk live in `retail_gold.customer_predictions`,
which has no `retail_cdp` view yet. Add one to `ddl/04_cdp` (same pattern as `customer_traits`) to use them.
