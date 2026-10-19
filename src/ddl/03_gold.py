# Databricks notebook source
# MAGIC %md
# MAGIC # DDL - Gold tables
# MAGIC
# MAGIC StrideForge CDP demo - GOLD layer (customer 360, features, audiences, measurement)
# MAGIC Tables tagged cdp_activation = 'true' are consumed by the CDP / activation layer
# MAGIC
# MAGIC _Generated from `03_gold.sql`; parameters come from widgets / job base_parameters._
# COMMAND ----------

# MAGIC %run ../_params

# COMMAND ----------

import re


def ddl(stmt):
    """Run a CREATE ... IF NOT EXISTS statement, then re-apply its COMMENT so existing objects stay in sync with the DDL."""
    run(stmt)
    m = re.search(r"CREATE (TABLE|SCHEMA) IF NOT EXISTS (\S+)", stmt)
    c = re.search(r"\)\s*COMMENT '((?:[^'\\]|\\.)*)'|^CREATE SCHEMA IF NOT EXISTS \S+\s+COMMENT '((?:[^'\\]|\\.)*)'", stmt.strip(), re.S)
    if m and c:
        run(f"COMMENT ON {m.group(1)} {m.group(2)} IS '{c.group(1) or c.group(2)}'")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `${gold}`
# COMMAND ----------

ddl(r"""
CREATE SCHEMA IF NOT EXISTS ${catalog}.${gold}
COMMENT 'StrideForge CDP demo - GOLD: Customer 360, predictive scores, audiences, triggers, Customer Lake event tables and campaign measurement, built from retail_silver by job strideforge_medallion (gold_* tasks). Tables with cdp_activation = true are the activation contract.'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_profile`
# MAGIC
# MAGIC ================= CDP-CONSUMED (activation) =================
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_profile (
  unified_customer_id STRING NOT NULL, first_name STRING, last_name STRING,
  email STRING, phone_e164 STRING, birth_date DATE, age_band STRING, gender STRING,
  city STRING, state STRING, postal_code STRING, country STRING,
  household_id STRING, kids_age_bands ARRAY<STRING>, home_store_id STRING,
  nearest_store_id STRING, nearest_store_distance_km DOUBLE,
  loyalty_id STRING, loyalty_tier STRING, points_balance INT,
  shoe_size_us DOUBLE, shoe_width STRING, pronation_type STRING,
  apparel_top_size STRING, apparel_bottom_size STRING,
  is_known BOOLEAN, has_app BOOLEAN, has_fitness_app BOOLEAN,
  email_contactable BOOLEAN, sms_contactable BOOLEAN, push_contactable BOOLEAN,
  paid_media_eligible BOOLEAN, first_seen_at TIMESTAMP, last_seen_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_customer_profile PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: Customer 360 profile - one row per unified customer (1.53M) with contact details, primary address, household, home/nearest store, loyalty tier and balance, fit profile, app/run-app flags and per-channel contactability. Customer Lake profile table for entity "Retail - StrideForge Customer". Derived from silver customers, customer_addresses, households, loyalty_members, customer_fit_profile, identity_graph and gold customer_consent. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_identity_map`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_identity_map (
  unified_customer_id STRING NOT NULL,
  id_type STRING COMMENT 'email | email_sha256 | phone_e164 | phone_sha256 | device_id | cookie_id | loyalty_id | maid | push_token',
  id_value STRING NOT NULL, source_system STRING, is_primary BOOLEAN,
  last_seen_at TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'CDP activation: all activation identifiers per customer - normalized and SHA-256 hashed email/phone, mobile ad ids, device ids, cookies, push tokens, loyalty ids/cards and source ids. Derived from silver identity_graph. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_consent`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_consent (
  unified_customer_id STRING NOT NULL,
  email_marketing BOOLEAN, sms_marketing BOOLEAN, push_marketing BOOLEAN,
  paid_media_targeting BOOLEAN, personalization_profiling BOOLEAN,
  jurisdiction STRING, do_not_sell BOOLEAN, global_suppression BOOLEAN,
  suppression_reason STRING, updated_at TIMESTAMP,
  CONSTRAINT pk_customer_consent PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: contactability gate per customer - email/SMS/push marketing, paid media targeting, profiling, do-not-sell and global suppression (repeated bounces / complaints). Derived from silver consent (GDPR/UK-GDPR require explicit opt-in for paid media/profiling; US/CA default allowed unless do-not-sell) and message_events. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_traits`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_traits (
  unified_customer_id STRING NOT NULL,
  first_order_date DATE, last_order_date DATE, days_since_last_order INT,
  order_count_lifetime INT, order_count_12m INT, revenue_lifetime DECIMAL(14,2),
  revenue_12m DECIMAL(14,2), aov_lifetime DECIMAL(12,2), margin_12m DECIMAL(14,2),
  recency_score INT, frequency_score INT, monetary_score INT, rfm_segment STRING,
  lifecycle_stage STRING COMMENT 'prospect | new | active | loyal | at_risk | lapsed | win_back',
  preferred_channel STRING, preferred_store_id STRING, top_category STRING,
  top_sport STRING, category_affinity MAP<STRING, DOUBLE>, sport_affinity MAP<STRING, DOUBLE>,
  full_price_ratio DOUBLE, discount_sensitivity STRING COMMENT 'full_price | mixed | deal_seeker',
  return_rate DOUBLE, fit_return_count INT, is_gifter BOOLEAN, is_omnichannel BOOLEAN,
  limited_drop_purchases INT, sessions_30d INT, product_views_30d INT,
  email_open_rate_90d DOUBLE, email_click_rate_90d DOUBLE,
  run_miles_30d DOUBLE, run_miles_90d DOUBLE, upcoming_race_date DATE,
  upcoming_race_distance_km DOUBLE,
  goal_race_date DATE COMMENT 'next half/full marathon (training goal)', goal_race_distance_km DOUBLE,
  run_club_checkins_90d INT,
  nps_latest INT, open_service_tickets INT, review_count INT, avg_review_rating DOUBLE,
  updated_at TIMESTAMP,
  CONSTRAINT pk_customer_traits PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: behavioural traits per customer - order history (USD), RFM scores and segment, lifecycle stage, preferred channel/store, category and sport affinity maps, discount sensitivity, returns, gifting, digital and email engagement, run miles, upcoming and goal race, run club, NPS, tickets, reviews. Derived from silver orders, order_lines, returns, sessions, message_events, fitness_activities, event_registrations, run_club_attendance, surveys, service_tickets and product_reviews (as of the latest order date). Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_predictions`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_predictions (
  unified_customer_id STRING NOT NULL,
  predicted_clv_12m DECIMAL(14,2), clv_tier STRING,
  churn_probability DOUBLE, churn_risk_band STRING,
  next_best_category STRING, next_best_style_id STRING, next_purchase_prob_30d DOUBLE,
  predicted_shoe_replacement_date DATE, active_shoe_miles DOUBLE,
  drop_hype_score DOUBLE, reseller_risk_score DOUBLE, return_risk_score DOUBLE,
  best_send_hour INT, best_send_day STRING, propensity_apparel DOUBLE,
  propensity_trail DOUBLE, propensity_basketball DOUBLE, propensity_kids DOUBLE,
  model_version STRING, scored_at TIMESTAMP,
  CONSTRAINT pk_customer_predictions PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP activation: predictive scores per customer - 12-month CLV and tier, churn probability and band, next best category/style, 30-day purchase propensity, predicted shoe replacement date, active shoe miles, drop hype, reseller risk, return risk, best send hour/day, category propensities. model_version heuristic_v1 (transparent rules, not trained models). Derived from gold customer_traits and silver shoe_mileage, order_lines, sessions, message_events. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_product_recommendations`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_product_recommendations (
  unified_customer_id STRING NOT NULL, rank INT NOT NULL, sku STRING, style_id STRING,
  recommendation_type STRING COMMENT 'replenish | cross_sell | upsell | new_arrival | back_in_stock',
  score DOUBLE, in_stock_in_size BOOLEAN, reason STRING, model_version STRING,
  generated_at TIMESTAMP
) COMMENT 'CDP activation: top-5 product recommendations per customer in their size, inventory-aware (replenish / back in stock / new arrival / cross-sell). Grain: unified_customer_id x rank. Derived from 90-day best sellers by category and gender (silver order_lines) for the customer"s next best category, filtered to their shoe/apparel size via product_catalog_enriched, excluding styles already owned, boosted by back-in-stock signups. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `segment_definitions`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.segment_definitions (
  segment_id STRING NOT NULL, segment_name STRING, description STRING,
  segment_type STRING COMMENT 'static | dynamic | ml | lookalike',
  rule_sql STRING, owner STRING, refresh_schedule STRING, is_active BOOLEAN,
  created_at TIMESTAMP, updated_at TIMESTAMP,
  CONSTRAINT pk_segment_definitions PRIMARY KEY (segment_id)
) COMMENT 'CDP audiences as data: 13 segment definitions with SQL rules (mileage replacement, CS3 fit save, goal race, high value at risk, win-back, sneakerheads, reseller suspects, families, tier upgrade, lookalike seed, suppressions, email fatigue, trail + rain). Defined in notebook gold/03_audiences. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `segment_membership`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.segment_membership (
  segment_id STRING NOT NULL, unified_customer_id STRING NOT NULL,
  entered_at TIMESTAMP, exited_at TIMESTAMP, is_current BOOLEAN,
  is_holdout BOOLEAN, refreshed_at TIMESTAMP
) COMMENT 'CDP audiences: customer x segment membership with a 10% deterministic holdout flag. Derived by evaluating each segment_definitions rule over gold customer_profile + customer_traits + customer_predictions. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `trigger_events`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.trigger_events (
  trigger_id STRING NOT NULL, unified_customer_id STRING NOT NULL,
  trigger_type STRING COMMENT 'cart_abandoned | browse_abandoned | back_in_stock_in_size | shoe_mileage_threshold | race_training_start | weather_rain | weather_cold | tier_upgrade_near | fit_return | post_purchase_cross_sell | drop_early_access | lapse_risk',
  trigger_ts TIMESTAMP, payload MAP<STRING, STRING>, sku STRING, campaign_id STRING,
  status STRING COMMENT 'new | sent | suppressed | expired', expires_at TIMESTAMP
) COMMENT 'CDP journey triggers as of the latest data day: cart / browse abandonment, back in stock in size, shoe mileage threshold, race training start, heavy-rain trail, tier upgrade, fit return, lapse risk; status suppressed when not contactable. Derived from silver carts, sessions, web_app_events, shoe_mileage, returns and gold product_catalog_enriched, segment_membership, customer_traits/predictions. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `product_catalog_enriched`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.product_catalog_enriched (
  sku STRING NOT NULL, style_id STRING, style_name STRING, category STRING,
  sport STRING, gender STRING, colorway STRING, size STRING, width STRING,
  current_price DECIMAL(10,2), msrp DECIMAL(10,2), is_on_markdown BOOLEAN,
  available_qty_online INT, available_qty_stores INT, stock_status STRING COMMENT 'in_stock | low_stock | out_of_stock',
  avg_rating DOUBLE, review_count INT, fit_feedback STRING, is_new_arrival BOOLEAN,
  is_limited_drop BOOLEAN, image_url STRING, product_url STRING, updated_at TIMESTAMP,
  CONSTRAINT pk_product_catalog_enriched PRIMARY KEY (sku)
) COMMENT 'CDP activation: SKU product feed for personalization - current price and markdown, online/store availability and stock status, rating and fit feedback, new-arrival and drop flags, image and product URL. Derived from silver skus, products, price_history, inventory_daily and product_reviews. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `audience_exports`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.audience_exports (
  export_id STRING NOT NULL, segment_id STRING, campaign_id STRING,
  destination STRING COMMENT 'esp | sms | push | meta | google | tiktok | youtube',
  unified_customer_id STRING, matched_id_type STRING, exported_at TIMESTAMP,
  sync_status STRING
) COMMENT 'CDP activation audit trail: which customers of which segment were synced to which destination (ESP, Meta, Google), consent-filtered and excluding holdouts. Derived from gold segment_membership + customer_profile contactability. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_daily_snapshot`
# MAGIC
# MAGIC ================= Analytics / supporting gold =================
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_daily_snapshot (
  snapshot_date DATE NOT NULL, unified_customer_id STRING NOT NULL,
  lifecycle_stage STRING, rfm_segment STRING, loyalty_tier STRING,
  revenue_12m DECIMAL(14,2), order_count_12m INT, churn_probability DOUBLE,
  predicted_clv_12m DECIMAL(14,2), email_contactable BOOLEAN
) COMMENT 'Month-end customer snapshots for the last 6 months: lifecycle, RFM-style segment, tier, 12-month revenue/orders, churn and CLV as of each snapshot. Derived from silver orders and gold customer_profile. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `product_affinity`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.product_affinity (
  antecedent_style_id STRING NOT NULL, consequent_style_id STRING NOT NULL,
  antecedent_category STRING, consequent_category STRING, support DOUBLE,
  confidence DOUBLE, lift DOUBLE, computed_at TIMESTAMP
) COMMENT 'Style-level basket affinity (support, confidence, lift) for cross-sell, pairs seen in >=30 multi-item orders. Derived from silver order_lines. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `drop_calendar`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.drop_calendar (
  drop_id STRING NOT NULL, style_id STRING, drop_name STRING, release_ts TIMESTAMP,
  units_available INT, eligible_segment_id STRING, eligible_customer_count BIGINT,
  early_access_start_ts TIMESTAMP, is_raffle BOOLEAN
) COMMENT 'Limited drops with the eligible audience (sneakerheads) and early-access window. Derived from silver product_drops and gold segment_membership. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_performance`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_performance (
  campaign_id STRING NOT NULL, campaign_name STRING, segment_id STRING,
  channel STRING, report_date DATE, audience_size BIGINT, sends BIGINT,
  delivered BIGINT, opens BIGINT, clicks BIGINT, unsubscribes BIGINT,
  impressions BIGINT, spend DECIMAL(14,2), conversions BIGINT,
  attributed_revenue DECIMAL(14,2), attributed_margin DECIMAL(14,2),
  open_rate DOUBLE, click_rate DOUBLE, conversion_rate DOUBLE, roas DOUBLE
) COMMENT 'Daily campaign KPIs per campaign and channel: audience, sends, delivered, opens, clicks, unsubscribes, impressions, spend, attributed conversions/revenue/margin, rates and ROAS. Derived from silver message_events, paid_media_performance, campaigns, order_lines and gold campaign_attribution. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_attribution`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_attribution (
  order_id STRING NOT NULL, unified_customer_id STRING, order_ts TIMESTAMP,
  order_revenue DECIMAL(12,2), campaign_id STRING, channel STRING,
  touchpoint_ts TIMESTAMP, attribution_model STRING COMMENT 'last_touch | first_touch | linear | time_decay | data_driven',
  attribution_weight DOUBLE, attributed_revenue DECIMAL(12,2)
) COMMENT 'Order-to-campaign attribution for the last 12 months: last-touch and linear models over clicks, UTM visits and paid touchpoints in a 7-day lookback, revenue in USD. Derived from silver orders and marketing_touchpoints. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_holdout_results`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_holdout_results (
  campaign_id STRING NOT NULL, segment_id STRING, measurement_window_days INT,
  test_customers BIGINT, control_customers BIGINT, test_conversion_rate DOUBLE,
  control_conversion_rate DOUBLE, test_revenue_per_customer DECIMAL(12,2),
  control_revenue_per_customer DECIMAL(12,2), incremental_lift_pct DOUBLE,
  incremental_revenue DECIMAL(14,2), p_value DOUBLE, computed_at TIMESTAMP
) COMMENT 'Incrementality for the 20 largest batch email campaigns: recipients vs email-contactable customers not sent, 7-day conversion and revenue, lift and approximate p-value. Derived from silver campaigns, message_events, orders and gold customer_profile. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `cohort_retention`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.cohort_retention (
  cohort_month DATE NOT NULL, acquisition_channel STRING, months_since_acquisition INT,
  cohort_size BIGINT, active_customers BIGINT, retention_rate DOUBLE,
  cumulative_revenue DECIMAL(14,2), revenue_per_customer DECIMAL(12,2)
) COMMENT 'Acquisition cohort retention and cumulative revenue per customer by month since first order and acquisition channel (online/store). Derived from silver orders. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `channel_kpis_daily`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.channel_kpis_daily (
  report_date DATE NOT NULL, channel STRING, sessions BIGINT, orders BIGINT,
  customers BIGINT, new_customers BIGINT, revenue DECIMAL(14,2), aov DECIMAL(12,2),
  conversion_rate DOUBLE, return_rate DOUBLE, cart_abandonment_rate DOUBLE
) COMMENT 'Daily KPIs per sales channel (web, app, store): sessions, orders, customers, new customers, revenue (USD), AOV, conversion, return and cart-abandonment rates. Derived from silver orders, sessions, order_lines, returns and carts. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `store_kpis_daily`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.store_kpis_daily (
  report_date DATE NOT NULL, store_id STRING, region STRING, transactions BIGINT,
  customers BIGINT, identified_customer_pct DOUBLE, revenue DECIMAL(14,2),
  units BIGINT, aov DECIMAL(12,2), returns BIGINT, gait_analyses INT,
  run_club_checkins INT, bopis_orders BIGINT
) COMMENT 'Daily KPIs per store: transactions, identified-customer share, revenue, units, AOV, returns, gait analyses, run-club check-ins, BOPIS pickups. Derived from silver orders, returns, run_club_attendance, stores and bronze gait_analysis_raw. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `identity_resolution_stats`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.identity_resolution_stats (
  run_ts TIMESTAMP NOT NULL, source_records BIGINT, unified_customers BIGINT,
  known_customers BIGINT, anonymous_profiles BIGINT, duplicates_collapsed BIGINT,
  dedup_rate DOUBLE, avg_ids_per_customer DOUBLE, multi_source_customer_pct DOUBLE,
  deterministic_matches BIGINT, probabilistic_matches BIGINT,
  match_rate_by_source MAP<STRING, DOUBLE>
) COMMENT 'Identity resolution run statistics (appended per run): source records, unified customers, anonymous profiles, dedup rate, ids per customer, multi-source share, deterministic/probabilistic matches, match rate by source. Derived from silver customer_source_records, customers, identity_graph, identity_match_pairs and web_app_events. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
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
