# Databricks notebook source
# MAGIC %md
# MAGIC # Create gold tables
# MAGIC
# MAGIC **What it does:** Creates the gold schema and its 20 tables with a description on every table and column. Safe to re-run: it only refreshes descriptions on existing tables.
# MAGIC
# MAGIC - **Reads:** -
# MAGIC - **Writes:** `retail_gold` schema and tables
# MAGIC - **Runs in:** `strideforge_bronze_setup` -> `ddl_gold`; `strideforge_medallion` -> `ensure_gold_tables`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params

# COMMAND ----------

import re

_COL = re.compile(r"^\s+(\w+) [^\n]*? COMMENT '((?:[^'\\]|\\.)*)',?$", re.M)


def ddl(stmt):
    """Run a CREATE ... IF NOT EXISTS statement, then re-apply its table comment and any changed column comments,
    so existing objects stay in sync with the descriptions in this DDL."""
    run(stmt)
    m = re.search(r"CREATE (TABLE|SCHEMA) IF NOT EXISTS (\S+)", stmt)
    if not m:
        return
    kind, name = m.group(1), m.group(2)
    for k, v in P.items():
        name = name.replace("${" + k + "}", v)
    if kind == "SCHEMA":
        c = re.search(r"COMMENT '((?:[^'\\]|\\.)*)'", stmt)
        if c:
            spark.sql(f"COMMENT ON SCHEMA {name} IS '{c.group(1)}'")
        return
    c = re.search(r"\n\)\s*COMMENT '((?:[^'\\]|\\.)*)'", stmt)
    if c:
        spark.sql(f"COMMENT ON TABLE {name} IS '{c.group(1)}'")
    current = {f.name: f.metadata.get("comment", "") for f in spark.table(name).schema}
    for col, cm in _COL.findall(stmt.split("\n)")[0]):
        if col in current and current[col] != cm.replace("\\'", "'").replace("\\\\", "\\"):
            spark.sql(f"ALTER TABLE {name} ALTER COLUMN {col} COMMENT '{cm}'")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Create the gold schema
# COMMAND ----------

ddl(r"""
CREATE SCHEMA IF NOT EXISTS ${catalog}.${gold}
COMMENT 'StrideForge CDP demo - GOLD: Customer 360, predictive scores, audiences, triggers, Customer Lake event tables and campaign measurement, built from retail_silver by job strideforge_medallion (gold_* tasks). Tables with cdp_activation = true are the activation contract.'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - `customer_profile` - Customer 360 profile - one row per unified customer (1.53M) with contact details, primary address, household, home/nearest store, loyalty ti
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_profile (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  first_name STRING COMMENT 'Best first name across sources (survivorship: ecom > loyalty > app > POS > run app > race > guest).',
  last_name STRING COMMENT 'Best last name across sources (same survivorship).',
  email STRING COMMENT 'Best email (lower-cased) across sources.',
  phone_e164 STRING COMMENT 'Most recently updated phone number, normalized to E.164 (+1..., +44..., +49...).',
  birth_date DATE COMMENT 'Birth date from the highest-priority source that has one.',
  age_band STRING COMMENT 'Age band from birth_date: 16-24, 25-34, 35-44, 45-54, 55+, unknown.',
  gender STRING COMMENT 'F, M, X (non-binary) or null when no source captured it.',
  city STRING COMMENT 'City of the primary address.',
  state STRING COMMENT 'State / province / region of the primary address.',
  postal_code STRING COMMENT 'Postal code of the primary address (US/DE 5-digit, UK and CA alphanumeric).',
  country STRING COMMENT 'Country code: US, UK, DE, CA.',
  household_id STRING COMMENT 'Household id (HH + hash of standardized primary street + postal code); joins to household_profile.',
  kids_age_bands ARRAY<STRING> COMMENT 'Kids age bands inferred from kids-size purchases in the household (0-5, 6-9, 10-14, 6-14).',
  home_store_id STRING COMMENT 'Store the customer shops most (POS orders), else the store where they registered; joins to silver.stores.',
  nearest_store_id STRING COMMENT 'Home store if known, else a flagship/standard store in the customer\'s city.',
  nearest_store_distance_km DOUBLE COMMENT 'Distance to nearest_store_id (not computed in this demo; customer coordinates are not geocoded).',
  loyalty_id STRING COMMENT 'Primary loyalty membership id (LY...).',
  loyalty_tier STRING COMMENT 'Loyalty tier on the 12-month points window: Rookie, Pro (>=300 pts), Elite (>=1000 pts).',
  points_balance INT COMMENT 'Current loyalty points balance.',
  shoe_size_us DOUBLE COMMENT 'Shoe size (US) from the latest gait-lab scan, else the most common kept footwear size purchased.',
  shoe_width STRING COMMENT 'Shoe width (D, 2E for men; B for women).',
  pronation_type STRING COMMENT 'neutral, overpronation or supination (gait lab).',
  apparel_top_size STRING COMMENT 'Most purchased tops size (XS-XXL).',
  apparel_bottom_size STRING COMMENT 'Most purchased bottoms size (XS-XXL).',
  is_known BOOLEAN COMMENT 'True for identified customers (all rows here; anonymous cookies are not profiled).',
  has_app BOOLEAN COMMENT 'Has at least one StrideForge app device.',
  has_fitness_app BOOLEAN COMMENT 'Has a StrideForge Run app account.',
  email_contactable BOOLEAN COMMENT 'True when email marketing consent is opted in, the customer is not suppressed and an email is on file.',
  sms_contactable BOOLEAN COMMENT 'SMS consent opted in and a phone on file.',
  push_contactable BOOLEAN COMMENT 'Push consent opted in and an app device.',
  paid_media_eligible BOOLEAN COMMENT 'May be targeted on ad platforms (GDPR/UK-GDPR explicit opt-in; elsewhere unless do-not-sell) and not suppressed.',
  first_seen_at TIMESTAMP COMMENT 'Earliest creation time across the customer\'s source records.',
  last_seen_at TIMESTAMP COMMENT 'Latest update/activity time across the customer\'s source records.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
  CONSTRAINT pk_customer_profile PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: Customer 360 profile - one row per unified customer (1.53M) with contact details, primary address, household, home/nearest store, loyalty tier and balance, fit profile, app/run-app flags and per-channel contactability. Customer Lake profile table for entity "Retail - StrideForge Customer". Derived from silver customers, customer_addresses, households, loyalty_members, customer_fit_profile, identity_graph and gold customer_consent. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - `customer_identity_map` - all activation identifiers per customer - normalized and SHA-256 hashed email/phone, mobile ad ids, device ids, cookies, push tokens, loyalt
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_identity_map (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  id_type STRING COMMENT 'Identifier type: email, email_sha256, phone_e164, phone_sha256, device_id, maid, cookie_id, push_token, loyalty_id, loyalty_card, ecom_id, pos_id, app_id, fitness_id.',
  id_value STRING NOT NULL COMMENT 'Identifier value (normalized; sha256 hex for hashed types).',
  source_system STRING COMMENT 'Source the identifier came from (ecom, pos, loyalty, app, fitness, ecom_guest, race_partner).',
  is_primary BOOLEAN COMMENT 'Most recently seen identifier of this type for the customer.',
  last_seen_at TIMESTAMP COMMENT 'Last time the identifier was seen in its source.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.'
) COMMENT 'CDP activation: all activation identifiers per customer - normalized and SHA-256 hashed email/phone, mobile ad ids, device ids, cookies, push tokens, loyalty ids/cards and source ids. Derived from silver identity_graph. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - `customer_consent` - contactability gate per customer - email/SMS/push marketing, paid media targeting, profiling, do-not-sell and global suppression (repeated b
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_consent (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  email_marketing BOOLEAN COMMENT 'Latest email marketing consent is opted in and the customer is not globally suppressed.',
  sms_marketing BOOLEAN COMMENT 'Latest SMS marketing consent is opted in.',
  push_marketing BOOLEAN COMMENT 'Latest push consent is opted in.',
  paid_media_targeting BOOLEAN COMMENT 'Allowed for ad-platform targeting: explicit opt-in under GDPR/UK-GDPR, otherwise true unless do-not-sell.',
  personalization_profiling BOOLEAN COMMENT 'Allowed for profiling/personalization: explicit opt-in under GDPR/UK-GDPR, otherwise true.',
  jurisdiction STRING COMMENT 'Privacy regime: GDPR (DE), UK-GDPR, PIPEDA (CA), CCPA (California), US-STATE.',
  do_not_sell BOOLEAN COMMENT 'Customer exercised the CCPA/US do-not-sell (sale of data) opt-out.',
  global_suppression BOOLEAN COMMENT 'Suppress from all sends: spam complaint or 2+ bounces.',
  suppression_reason STRING COMMENT 'spam_complaint or repeated_bounce when globally suppressed.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
  CONSTRAINT pk_customer_consent PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: contactability gate per customer - email/SMS/push marketing, paid media targeting, profiling, do-not-sell and global suppression (repeated bounces / complaints). Derived from silver consent (GDPR/UK-GDPR require explicit opt-in for paid media/profiling; US/CA default allowed unless do-not-sell) and message_events. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - `customer_traits` - behavioural traits per customer - order history (USD), RFM scores and segment, lifecycle stage, preferred channel/store, category and sport
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_traits (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  first_order_date DATE COMMENT 'Date of the first linked order in the data window (Oct 2023 onward).',
  last_order_date DATE COMMENT 'Date of the most recent linked order.',
  days_since_last_order INT COMMENT 'Days between last_order_date and the data as-of date.',
  order_count_lifetime INT COMMENT 'Number of linked orders in the data window.',
  order_count_12m INT COMMENT 'Number of orders in the last 12 months.',
  revenue_lifetime DECIMAL(14,2) COMMENT 'Net merchandise revenue in the data window (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  revenue_12m DECIMAL(14,2) COMMENT 'Net merchandise revenue (subtotal - discount) in the last 12 months (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  aov_lifetime DECIMAL(12,2) COMMENT 'Average order value (revenue_lifetime / order_count_lifetime) (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  margin_12m DECIMAL(14,2) COMMENT 'Gross margin of order lines in the last 12 months (line total - cost).',
  recency_score INT COMMENT 'Recency quintile 1-5 (5 = most recent buyers).',
  frequency_score INT COMMENT 'Frequency quintile 1-5 (5 = most orders).',
  monetary_score INT COMMENT 'Monetary quintile 1-5 (5 = highest revenue).',
  rfm_segment STRING COMMENT 'RFM segment from recency/frequency/monetary quintiles: champions, loyal, promising, needs_attention, at_risk, hibernating, no_purchase.',
  lifecycle_stage STRING COMMENT 'prospect (no linked purchase), new (first order <=90d), active, loyal (>=5 orders in 12m), at_risk (181-365d since last order), lapsed (>365d).',
  preferred_channel STRING COMMENT 'Channel with the most orders: web, app or store.',
  preferred_store_id STRING COMMENT 'Store with the most POS orders.',
  top_category STRING COMMENT 'Category with the most units purchased.',
  top_sport STRING COMMENT 'Sport with the most units purchased.',
  category_affinity MAP<STRING,DOUBLE> COMMENT 'Share of units purchased per category (map category -> 0-1).',
  sport_affinity MAP<STRING,DOUBLE> COMMENT 'Share of units purchased per sport (map sport -> 0-1).',
  full_price_ratio DOUBLE COMMENT 'Share of orders with no markdown and no discount.',
  discount_sensitivity STRING COMMENT 'full_price (>=80% full-price orders), mixed (45-80%), deal_seeker (<45%).',
  return_rate DOUBLE COMMENT 'Returned lines / purchased lines.',
  fit_return_count INT COMMENT 'Number of returns for too_small or too_big.',
  is_gifter BOOLEAN COMMENT 'Bought kids items or both men\'s and women\'s styles.',
  is_omnichannel BOOLEAN COMMENT 'Purchased both online and in store.',
  limited_drop_purchases INT COMMENT 'Number of limited-drop lines purchased.',
  sessions_30d INT COMMENT 'Web/app sessions in the last 30 days.',
  product_views_30d INT COMMENT 'Product views in the last 30 days.',
  email_open_rate_90d DOUBLE COMMENT 'Distinct emails opened / emails sent in the last 90 days.',
  email_click_rate_90d DOUBLE COMMENT 'Distinct emails clicked / emails sent in the last 90 days.',
  run_miles_30d DOUBLE COMMENT 'Miles logged in the run app in the last 30 days.',
  run_miles_90d DOUBLE COMMENT 'Miles logged in the run app in the last 90 days.',
  upcoming_race_date DATE COMMENT 'Date of the next registered race of any distance.',
  upcoming_race_distance_km DOUBLE COMMENT 'Distance (km) of the next registered race.',
  run_club_checkins_90d INT COMMENT 'Store run-club check-ins in the last 90 days.',
  nps_latest INT COMMENT 'Most recent NPS score (0-10).',
  open_service_tickets INT COMMENT 'Tickets still open or pending.',
  review_count INT COMMENT 'Product reviews written.',
  avg_review_rating DOUBLE COMMENT 'Average review rating (1-5).',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
  goal_race_date DATE COMMENT 'Date of the next registered half or full marathon (training goal).',
  goal_race_distance_km DOUBLE COMMENT 'Distance (km) of the goal race (21.1 or 42.2).',
  CONSTRAINT pk_customer_traits PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: behavioural traits per customer - order history (USD), RFM scores and segment, lifecycle stage, preferred channel/store, category and sport affinity maps, discount sensitivity, returns, gifting, digital and email engagement, run miles, upcoming and goal race, run club, NPS, tickets, reviews. Derived from silver orders, order_lines, returns, sessions, message_events, fitness_activities, event_registrations, run_club_attendance, surveys, service_tickets and product_reviews (as of the latest order date). Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - `customer_predictions` - predictive scores per customer - 12-month CLV and tier, churn probability and band, next best category/style, 30-day purchase propensity, pr
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_predictions (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  predicted_clv_12m DECIMAL(14,2) COMMENT 'Predicted 12-month customer lifetime value (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93) = revenue_12m x (1 - churn_probability) x 1.08 (heuristic_v1).',
  clv_tier STRING COMMENT 'platinum (>=900 expected 12m value), gold (>=350), silver (>=100), bronze.',
  churn_probability DOUBLE COMMENT 'Probability the customer does not purchase again (0-1); logistic score on recency, 12m frequency, fit returns and recent sessions (heuristic_v1).',
  churn_risk_band STRING COMMENT 'high (>=0.7), medium (>=0.4), low.',
  next_best_category STRING COMMENT 'Category to promote next (second strongest affinity, else top category).',
  next_best_style_id STRING COMMENT 'Rank-1 recommended style from customer_product_recommendations.',
  next_purchase_prob_30d DOUBLE COMMENT 'Probability of purchasing in the next 30 days (heuristic_v1).',
  predicted_shoe_replacement_date DATE COMMENT 'Date the active run-app shoe reaches ~450 miles at the current weekly pace, else last running-shoe purchase + 180 days.',
  active_shoe_miles DOUBLE COMMENT 'Miles on the customer\'s active registered run-app shoe.',
  drop_hype_score DOUBLE COMMENT '0-1 likelihood to engage with limited drops (drop purchases, app sessions, lifestyle/basketball affinity).',
  reseller_risk_score DOUBLE COMMENT '0-1 likelihood of reselling (multiple sizes of the same limited drop, high drop volume).',
  return_risk_score DOUBLE COMMENT '0-1 likelihood of returning (return rate and fit returns).',
  best_send_hour INT COMMENT 'Hour of day (0-23) with the most email opens; default 19.',
  best_send_day STRING COMMENT 'Weekday with the most email opens; default Tuesday.',
  propensity_apparel DOUBLE COMMENT '0-1 propensity to buy apparel.',
  propensity_trail DOUBLE COMMENT '0-1 propensity to buy trail products.',
  propensity_basketball DOUBLE COMMENT '0-1 propensity to buy basketball products.',
  propensity_kids DOUBLE COMMENT '0-1 propensity to buy kids products.',
  model_version STRING COMMENT 'Version of the scoring logic that produced the row (heuristic_v1 = transparent rules, not a trained model).',
  scored_at TIMESTAMP COMMENT 'When the scores were computed.',
  CONSTRAINT pk_customer_predictions PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: predictive scores per customer - 12-month CLV and tier, churn probability and band, next best category/style, 30-day purchase propensity, predicted shoe replacement date, active shoe miles, drop hype, reseller risk, return risk, best send hour/day, category propensities. model_version heuristic_v1 (transparent rules, not trained models). Derived from gold customer_traits and silver shoe_mileage, order_lines, sessions, message_events. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - `customer_product_recommendations` - top-5 product recommendations per customer in their size, inventory-aware (replenish / back in stock / new arrival / cross-sell)
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_product_recommendations (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  rank INT NOT NULL COMMENT 'Recommendation rank 1-5 (1 = best).',
  sku STRING COMMENT 'Recommended SKU in the customer\'s size (colorway 01).',
  style_id STRING COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  recommendation_type STRING COMMENT 'back_in_stock, replenish (running/trail with 350+ shoe miles), new_arrival or cross_sell.',
  score DOUBLE COMMENT 'Ranking score: 90-day popularity rank in the category, x0.2 if out of stock, +0.5 for a back-in-stock signup.',
  in_stock_in_size BOOLEAN COMMENT 'SKU is available online (DC inventory > 0).',
  reason STRING COMMENT 'Human-readable reason for the recommendation.',
  model_version STRING COMMENT 'Version of the scoring logic that produced the row (heuristic_v1 = transparent rules, not a trained model).',
  generated_at TIMESTAMP COMMENT 'When the recommendations were generated.'
) COMMENT 'CDP activation: top-5 product recommendations per customer in their size, inventory-aware (replenish / back in stock / new arrival / cross-sell). Grain: unified_customer_id x rank. Derived from 90-day best sellers by category and gender (silver order_lines) for the customer"s next best category, filtered to their shoe/apparel size via product_catalog_enriched, excluding styles already owned, boosted by back-in-stock signups. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - `trigger_events` - cart / browse abandonment, back in stock in size, shoe mileage threshold, race training start, heavy-rain trail, tier upgrade, fit return, l
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.trigger_events (
  trigger_id STRING NOT NULL COMMENT 'Unique trigger id.',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  trigger_type STRING COMMENT 'cart_abandoned, browse_abandoned, back_in_stock_in_size, shoe_mileage_threshold, race_training_start, weather_rain, tier_upgrade_near, fit_return, lapse_risk.',
  trigger_ts TIMESTAMP COMMENT 'When the triggering behaviour happened (or the as-of time for state-based triggers).',
  payload MAP<STRING,STRING> COMMENT 'Trigger context (e.g. cart_id/cart_value, miles, goal race date, return reason).',
  sku STRING COMMENT 'Product involved, when applicable.',
  campaign_id STRING COMMENT 'Journey / triggered campaign to start (TRG-...).',
  status STRING COMMENT 'new (contactable) or suppressed (no consented channel).',
  expires_at TIMESTAMP COMMENT 'Trigger is stale after this time (trigger_ts + 7 days).'
) COMMENT 'CDP journey triggers as of the latest data day: cart / browse abandonment, back in stock in size, shoe mileage threshold, race training start, heavy-rain trail, tier upgrade, fit return, lapse risk; status suppressed when not contactable. Derived from silver carts, sessions, web_app_events, shoe_mileage, returns and gold product_catalog_enriched, customer_profile, customer_traits/predictions. Built by job strideforge_medallion, task gold_triggers (notebook gold/03_triggers).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - `product_catalog_enriched` - SKU product feed for personalization - current price and markdown, online/store availability and stock status, rating and fit feedback, new-
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.product_catalog_enriched (
  sku STRING NOT NULL COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).',
  style_id STRING COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  style_name STRING COMMENT 'Product style name (e.g. CloudStride 3 Men\'s).',
  category STRING COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  sport STRING COMMENT 'Sport of the product (Running, Trail Running, Basketball, Training, Lifestyle, Football, Multi-Sport).',
  gender STRING COMMENT 'Men, Women, Unisex or Kids.',
  colorway STRING COMMENT 'Colorway name (e.g. Core Black / Cloud White).',
  size STRING COMMENT 'Size label (US numeric, kids C/Y, alpha XS-XXL, OS).',
  width STRING COMMENT 'Shoe width (D, 2E, B) where applicable.',
  current_price DECIMAL(10,2) COMMENT 'Price in effect today (promo > markdown > list), USD list basis.',
  msrp DECIMAL(10,2) COMMENT 'Manufacturer\'s suggested retail price (USD).',
  is_on_markdown BOOLEAN COMMENT 'A markdown or promo price is in effect.',
  available_qty_online INT COMMENT 'Available units across distribution centres (latest daily snapshot).',
  available_qty_stores INT COMMENT 'Available units across stores (current snapshot).',
  stock_status STRING COMMENT 'in_stock, low_stock (<10 online), out_of_stock, or discontinued (kept so past purchases, returns and reviews still join).',
  avg_rating DOUBLE COMMENT 'Average review rating of the style.',
  review_count INT COMMENT 'Number of reviews of the style.',
  fit_feedback STRING COMMENT 'Most common fit feedback in reviews (runs_small, true_to_size, runs_large).',
  is_new_arrival BOOLEAN COMMENT 'Style launched in the last 60 days.',
  is_limited_drop BOOLEAN COMMENT 'Style is a limited collab drop.',
  image_url STRING COMMENT 'Main product image URL.',
  product_url STRING COMMENT 'Product detail page URL for the SKU.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
  CONSTRAINT pk_product_catalog_enriched PRIMARY KEY (sku)
) COMMENT 'CDP activation: SKU product feed for personalization (all SKUs, including discontinued ones) - current price and markdown, online/store availability and stock status, rating and fit feedback, new-arrival and drop flags, image and product URL. Derived from silver skus, products, price_history, inventory_daily and product_reviews. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10 - `identity_resolution_stats` - Identity resolution run statistics (appended per run): source records, unified customers, anonymous profiles, dedup rate, ids per customer,
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.identity_resolution_stats (
  run_ts TIMESTAMP NOT NULL COMMENT 'When identity resolution statistics were computed.',
  source_records BIGINT COMMENT 'Standardized source profiles considered.',
  unified_customers BIGINT COMMENT 'Unified customers produced.',
  known_customers BIGINT COMMENT 'Unified customers with PII.',
  anonymous_profiles BIGINT COMMENT 'Distinct cookies/devices with activity never linked to a customer.',
  duplicates_collapsed BIGINT COMMENT 'source_records - unified_customers.',
  dedup_rate DOUBLE COMMENT '1 - unified_customers / source_records.',
  avg_ids_per_customer DOUBLE COMMENT 'Identity graph rows per unified customer.',
  multi_source_customer_pct DOUBLE COMMENT 'Share of customers resolved from 2+ source records.',
  deterministic_matches BIGINT COMMENT 'Accepted deterministic match edges.',
  probabilistic_matches BIGINT COMMENT 'Accepted probabilistic match edges.',
  match_rate_by_source MAP<STRING,DOUBLE> COMMENT 'Per source system, share of its records that merged with at least one other record.'
) COMMENT 'Identity resolution run statistics (appended per run): source records, unified customers, anonymous profiles, dedup rate, ids per customer, multi-source share, deterministic/probabilistic matches, match rate by source. Derived from silver customer_source_records, customers, identity_graph, identity_match_pairs and web_app_events. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace Customer Lake objects built by earlier versions (views, or tables without a primary key)
# COMMAND ----------

_CL = ["household_profile", "purchase_events", "purchase_line_items", "return_events", "digital_events",
       "message_engagement_events", "run_activity_events", "loyalty_events", "race_registration_events",
       "support_events", "review_events"]
_info = f"{P['catalog']}.information_schema"
_pk = {r.table_name for r in spark.sql(f"""SELECT table_name FROM {_info}.table_constraints
        WHERE table_schema = '{P['gold']}' AND constraint_type = 'PRIMARY KEY'""").collect()}
for _r in spark.sql(f"SELECT table_name, table_type FROM {_info}.tables WHERE table_schema = '{P['gold']}'").collect():
    if _r.table_name in _CL and (_r.table_type == "VIEW" or _r.table_name not in _pk):
        spark.sql(f"DROP {'VIEW' if _r.table_type == 'VIEW' else 'TABLE'} {P['catalog']}.{P['gold']}.{_r.table_name}")
        print("replacing", _r.table_name)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 11 - `household_profile` - Household entity profile (one row per household)
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.household_profile (
  household_id STRING NOT NULL COMMENT 'Household id (HH + hash of standardized primary street + postal code); joins to household_profile.',
  address_id STRING COMMENT 'Primary address id of the household.',
  city STRING COMMENT 'City of the primary address.',
  state STRING COMMENT 'State / province / region of the primary address.',
  postal_code STRING COMMENT 'Postal code of the primary address (US/DE 5-digit, UK and CA alphanumeric).',
  country STRING COMMENT 'Country code: US, UK, DE, CA.',
  home_store_id STRING COMMENT 'Store the customer shops most (POS orders), else the store where they registered; joins to silver.stores.',
  member_count INT COMMENT 'Customers in the household.',
  kids_count INT COMMENT 'Distinct kids age bands observed (proxy for number of kids).',
  kids_age_bands ARRAY<STRING> COMMENT 'Kids age bands inferred from kids-size purchases in the household (0-5, 6-9, 10-14, 6-14).',
  has_kids BOOLEAN COMMENT 'Household bought kids-size products.',
  email_contactable_members INT COMMENT 'Members contactable by email.',
  any_email_contactable BOOLEAN COMMENT 'At least one member is contactable by email.',
  any_sms_contactable BOOLEAN COMMENT 'At least one member is contactable by SMS.',
  any_paid_media_eligible BOOLEAN COMMENT 'At least one member can be targeted on ad platforms.',
  household_revenue_12m DECIMAL(14,2) COMMENT 'Sum of members\' revenue_12m (USD).',
  household_predicted_clv_12m DECIMAL(14,2) COMMENT 'Sum of members\' predicted_clv_12m (USD).',
  min_member_churn_probability DOUBLE COMMENT 'Lowest churn probability among members (most engaged member).',
  last_order_date DATE COMMENT 'Most recent order by any member.',
  top_sport STRING COMMENT 'Most common top sport among members.',
  best_loyalty_tier_rank INT COMMENT 'Best member tier: 3 Elite, 2 Pro, 1 Rookie, 0 none.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
  CONSTRAINT pk_household_profile PRIMARY KEY (household_id)
) COMMENT 'Customer Lake: Household profile - one row per household with member roll-ups (members, kids, contactability, revenue, CLV, churn, top sport, best tier). Joined many:1 from customer_profile on household_id. Derived from gold customer_profile, customer_traits, customer_predictions and silver households. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 12 - `purchase_events` - Customer Lake event table: orders
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.purchase_events (
  order_id STRING NOT NULL COMMENT 'Event id: order id.',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  order_ts TIMESTAMP COMMENT 'Event timestamp: when the order was placed.',
  channel STRING COMMENT 'Channel of the record (web/app/store for commerce; email/sms/push for messaging; platform for paid).',
  store_id STRING COMMENT 'Store (POS) or pickup store (BOPIS).',
  country STRING COMMENT 'Order country derived from currency.',
  currency STRING COMMENT 'Order currency (USD, CAD, GBP, EUR).',
  revenue_usd DECIMAL(14,2) COMMENT 'Net merchandise revenue (subtotal - discount) (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  total_amount DECIMAL(12,2) COMMENT 'Order total incl. tax and shipping, local currency.',
  discount_amount DECIMAL(12,2) COMMENT 'Order-level promo discount, local currency.',
  is_full_price BOOLEAN COMMENT 'No markdown lines and no promo discount.',
  promo_code STRING COMMENT 'Promo / affiliate code used.',
  fulfillment_type STRING COMMENT 'ship, bopis, ship_from_store or in_store.',
  payment_type STRING COMMENT 'Payment method.',
  utm_source STRING COMMENT 'UTM source of the visit/order (google, meta, tiktok, youtube, email, sms, braze, affiliate, referral); null = direct.',
  utm_medium STRING COMMENT 'UTM medium (cpc, paid_social, paid_video, email, sms, push, organic, affiliate, referral).',
  utm_campaign STRING COMMENT 'UTM campaign (campaign_id for owned/paid campaigns, promo code for affiliates).',
  units INT COMMENT 'Units in the order.',
  categories ARRAY<STRING> COMMENT 'Distinct product categories in the order.',
  sports ARRAY<STRING> COMMENT 'Distinct sports in the order.',
  style_names ARRAY<STRING> COMMENT 'Distinct style names in the order.',
  has_running_shoes BOOLEAN COMMENT 'Order contains running or trail shoes.',
  has_cloudstride_3 BOOLEAN COMMENT 'Order contains a CloudStride 3.',
  has_limited_drop BOOLEAN COMMENT 'Order contains a limited-drop style.',
  CONSTRAINT pk_purchase_events PRIMARY KEY (order_id)
) COMMENT 'Customer Lake event table: identified orders (event id order_id, timestamp order_ts) with USD revenue, channel, promo, UTMs and what was bought (categories, sports, styles, running shoes, CloudStride 3, limited drop). Derived from silver orders + order_lines + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 13 - `purchase_line_items` - Customer Lake: order lines with SKU (joins purchase_events and product_catalog_enriched)
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.purchase_line_items (
  order_line_id STRING NOT NULL COMMENT 'Line id: order_id + "-" + line number.',
  order_id STRING NOT NULL COMMENT 'Order id; joins to purchase_events.order_id (purchase_events 1:many purchase_line_items).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  sku STRING COMMENT 'SKU bought; joins to product_catalog_enriched.sku (many:1) for category, style, price and stock.',
  unit_price_usd DECIMAL(12,2) COMMENT 'Price per unit (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  discount_usd DECIMAL(12,2) COMMENT 'Discount on the line (USD).',
  revenue_usd DECIMAL(14,2) COMMENT 'Net line revenue after discount (USD).',
  CONSTRAINT pk_purchase_line_items PRIMARY KEY (order_line_id)
) COMMENT 'Customer Lake: purchase line items - one row per order line of an identified order, with the SKU and USD amounts. Related 1:many from purchase_events on order_id, and many:1 to product_catalog_enriched on sku. Derived from silver order_lines + orders. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 14 - `return_events` - Customer Lake event table: returns
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.return_events (
  return_id STRING NOT NULL COMMENT 'Event id: return id.',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  return_ts TIMESTAMP COMMENT 'Event timestamp: when the return was requested/processed.',
  order_id STRING COMMENT 'Original order.',
  sku STRING COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).',
  style_id STRING COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  style_name STRING COMMENT 'Product style name (e.g. CloudStride 3 Men\'s).',
  category STRING COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  sport STRING COMMENT 'Sport of the product (Running, Trail Running, Basketball, Training, Lifestyle, Football, Multi-Sport).',
  return_reason STRING COMMENT 'too_small, too_big, comfort, defect, not_as_pictured, changed_mind, other.',
  is_fit_related BOOLEAN COMMENT 'Reason is too_small or too_big.',
  return_type STRING COMMENT 'refund or exchange.',
  return_channel STRING COMMENT 'mail or store.',
  refund_amount DECIMAL(12,2) COMMENT 'Refunded amount, local currency.',
  quantity INT COMMENT 'Units returned.',
  CONSTRAINT pk_return_events PRIMARY KEY (return_id)
) COMMENT 'Customer Lake event table: identified returns (event id return_id, timestamp return_ts) with style, category, reason and fit flag. Derived from silver returns + skus + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 15 - `digital_events` - Customer Lake event table: web and app events
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.digital_events (
  event_id STRING NOT NULL COMMENT 'Unique id of the web/app event (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  event_ts TIMESTAMP COMMENT 'Event timestamp.',
  platform STRING COMMENT 'web, ios or android.',
  event_type STRING COMMENT 'page_view, product_view, search, size_select, add_to_cart, remove_from_cart, checkout_start, purchase, wishlist_add, back_in_stock_signup, app_open, push_open.',
  session_id STRING COMMENT 'Session id; joins to silver.sessions.',
  page_type STRING COMMENT 'Page or app screen (home, category, product, search, checkout, pdp...).',
  sku STRING COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).',
  style_id STRING COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  category STRING COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  search_term STRING COMMENT 'Search query, for search events.',
  selected_size STRING COMMENT 'Size selected on the product page.',
  utm_source STRING COMMENT 'UTM source of the visit/order (google, meta, tiktok, youtube, email, sms, braze, affiliate, referral); null = direct.',
  utm_medium STRING COMMENT 'UTM medium (cpc, paid_social, paid_video, email, sms, push, organic, affiliate, referral).',
  utm_campaign STRING COMMENT 'UTM campaign (campaign_id for owned/paid campaigns, promo code for affiliates).',
  device_type STRING COMMENT 'Device: mobile/desktop/tablet on web, ios/android in the app.',
  CONSTRAINT pk_digital_events PRIMARY KEY (event_id)
) COMMENT 'Customer Lake event table: identified web/app events (event id event_id, timestamp event_ts). Derived from silver web_app_events. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 16 - `message_engagement_events` - Customer Lake event table: email/SMS/push sends and engagement
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.message_engagement_events (
  message_event_id STRING NOT NULL COMMENT 'Unique id of the send or engagement event (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  event_ts TIMESTAMP COMMENT 'Event timestamp.',
  channel STRING COMMENT 'email, sms or push.',
  event_type STRING COMMENT 'send, open, click, bounce, unsubscribe or complaint.',
  campaign_id STRING COMMENT 'Campaign id: EM-/SMS-/PUSH- owned campaigns, TRG- triggered flows, ME-/GO-/TI-/YO- paid campaigns (Meta/Google/TikTok/YouTube).',
  campaign_name STRING COMMENT 'Campaign display name.',
  campaign_objective STRING COMMENT 'engagement, promotion, launch, onboarding, retention, replenishment, win_back.',
  campaign_type STRING COMMENT 'batch or triggered.',
  variant STRING COMMENT 'A/B test variant (A or B).',
  link_url STRING COMMENT 'Clicked URL, for clicks.',
  device_type STRING COMMENT 'Device: mobile/desktop/tablet on web, ios/android in the app.',
  CONSTRAINT pk_message_engagement_events PRIMARY KEY (message_event_id)
) COMMENT 'Customer Lake event table: email/SMS/push sends and engagement (event id message_event_id, timestamp event_ts) with campaign name and objective. Derived from silver message_events + campaigns. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 17 - `run_activity_events` - Customer Lake event table: run-app activities
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.run_activity_events (
  activity_id STRING NOT NULL COMMENT 'Unique id of the run-app activity (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  start_ts TIMESTAMP COMMENT 'Event timestamp: activity start.',
  activity_type STRING COMMENT 'run, trail_run, treadmill_run or walk.',
  distance_km DOUBLE COMMENT 'Distance in km.',
  distance_miles DOUBLE COMMENT 'Distance in miles.',
  duration_seconds INT COMMENT 'Moving time in seconds.',
  avg_pace_sec_per_km DOUBLE COMMENT 'Average pace (seconds per km).',
  elevation_gain_m DOUBLE COMMENT 'Elevation gain in metres.',
  shoe_registration_id STRING COMMENT 'Registered shoe used (joins to silver.shoe_mileage).',
  sku STRING COMMENT 'SKU of the shoe used, when a StrideForge shoe.',
  shoe_total_miles DOUBLE COMMENT 'Total miles on that shoe to date.',
  CONSTRAINT pk_run_activity_events PRIMARY KEY (activity_id)
) COMMENT 'Customer Lake event table: run-app activities (event id activity_id, timestamp start_ts) with shoe and its total mileage. Derived from silver fitness_activities + shoe_mileage. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 18 - `loyalty_events` - Customer Lake event table: loyalty points activity
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.loyalty_events (
  ledger_id STRING NOT NULL COMMENT 'Unique id of the loyalty ledger entry (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  activity_ts TIMESTAMP COMMENT 'Event timestamp.',
  loyalty_id STRING COMMENT 'Loyalty membership id.',
  activity_type STRING COMMENT 'earn, redeem, adjust (return), bonus, expire, opening_balance or tier_change.',
  points INT COMMENT 'Points added (+) or removed (-).',
  reference_id STRING COMMENT 'Related order id, bonus code or new tier.',
  reference_type STRING COMMENT 'order, return, promotion, inactivity or tier.',
  CONSTRAINT pk_loyalty_events PRIMARY KEY (ledger_id)
) COMMENT 'Customer Lake event table: loyalty ledger activity (event id ledger_id, timestamp activity_ts). Derived from silver loyalty_transactions. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 19 - `race_registration_events` - Customer Lake event table: race registrations
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.race_registration_events (
  registration_id STRING NOT NULL COMMENT 'Unique id of the race registration (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  registered_at TIMESTAMP COMMENT 'Event timestamp: when the customer registered.',
  event_id STRING COMMENT 'Id of the race the customer registered for.',
  event_name STRING COMMENT 'Name of the race (e.g. Chicago Marathon 2026).',
  event_type STRING COMMENT 'marathon, half_marathon, 10k or 5k.',
  event_date DATE COMMENT 'Race date.',
  event_city STRING COMMENT 'City where the race takes place.',
  distance_km DOUBLE COMMENT 'Race distance in kilometres (5, 10, 21.1 or 42.2).',
  registration_source STRING COMMENT 'RaceRoster, Active.com, RunSignup or StrideForge App.',
  CONSTRAINT pk_race_registration_events PRIMARY KEY (registration_id)
) COMMENT 'Customer Lake event table: race registrations (event id registration_id, timestamp registered_at) with race date, type and distance. Derived from silver event_registrations. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 20 - `support_events` - Customer Lake event table: support tickets
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.support_events (
  ticket_id STRING NOT NULL COMMENT 'Unique id of the support ticket (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  created_at TIMESTAMP COMMENT 'Event timestamp: ticket created.',
  channel STRING COMMENT 'chat, email, phone or social.',
  category STRING COMMENT 'order_status, returns_refunds, sizing_fit, product_defect, promo_pricing, loyalty_points, account_login.',
  priority STRING COMMENT 'high, normal or low.',
  status STRING COMMENT 'open, pending, solved or closed.',
  sentiment STRING COMMENT 'Rule-based sentiment (negative/neutral).',
  resolution_hours DOUBLE COMMENT 'Hours to resolution (null while open).',
  order_id STRING COMMENT 'Related order, when referenced.',
  CONSTRAINT pk_support_events PRIMARY KEY (ticket_id)
) COMMENT 'Customer Lake event table: service tickets (event id ticket_id, timestamp created_at) with category, status, sentiment. Derived from silver service_tickets. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 21 - `review_events` - Customer Lake event table: product reviews
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.review_events (
  review_id STRING NOT NULL COMMENT 'Unique id of the product review (Customer Lake event ID).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  submitted_at TIMESTAMP COMMENT 'Event timestamp: review submitted.',
  sku STRING COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).',
  style_id STRING COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  style_name STRING COMMENT 'Product style name (e.g. CloudStride 3 Men\'s).',
  category STRING COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  rating INT COMMENT 'Star rating 1-5.',
  fit_rating STRING COMMENT 'Reviewer-selected fit (runs_small, true_to_size, runs_large).',
  ai_sentiment STRING COMMENT 'Sentiment (ai_analyze_sentiment for the 20K most recent reviews, rating-based otherwise).',
  ai_fit_feedback STRING COMMENT 'Fit feedback extracted from the text (ai_classify for recent reviews, keyword rules otherwise).',
  ai_themes ARRAY<STRING> COMMENT 'Themes mentioned: fit, comfort, durability, style, price.',
  verified_purchase BOOLEAN COMMENT 'Reviewer bought the product.',
  CONSTRAINT pk_review_events PRIMARY KEY (review_id)
) COMMENT 'Customer Lake event table: product reviews (event id review_id, timestamp submitted_at) with rating and AI fit/sentiment. Derived from silver product_reviews + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Migrate `customer_traits` (add goal-race columns to tables created by earlier versions)
# COMMAND ----------

_t = f"{P['catalog']}.{P['gold']}.customer_traits"
_cols = {f.name for f in spark.table(_t).schema}
if "goal_race_date" not in _cols:
    spark.sql(f"ALTER TABLE {_t} ADD COLUMNS (goal_race_date DATE COMMENT 'next half/full marathon (training goal)', "
              "goal_race_distance_km DOUBLE)")
    print("added goal_race_* columns")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove tables dropped from the gold layer (audiences and campaign reporting now live outside gold)
# COMMAND ----------

for _t in ("segment_definitions", "segment_membership", "audience_exports", "product_affinity", "drop_calendar",
           "customer_daily_snapshot", "cohort_retention", "channel_kpis_daily", "store_kpis_daily",
           "campaign_attribution", "campaign_performance", "campaign_holdout_results"):
    spark.sql(f"DROP TABLE IF EXISTS {P['catalog']}.{P['gold']}.{_t}")
