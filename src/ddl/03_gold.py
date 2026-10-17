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

# MAGIC %md
# MAGIC ### Create `${gold}`
# COMMAND ----------

run(r"""
CREATE SCHEMA IF NOT EXISTS ${catalog}.${gold}
COMMENT 'StrideForge CDP demo - Customer 360, features, audiences and campaign measurement'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_profile`
# MAGIC
# MAGIC ================= CDP-CONSUMED (activation) =================
# COMMAND ----------

run(r"""
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
) COMMENT 'CDP: Customer 360 profile - one row per unified customer'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_identity_map`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_identity_map (
  unified_customer_id STRING NOT NULL,
  id_type STRING COMMENT 'email | email_sha256 | phone_e164 | phone_sha256 | device_id | cookie_id | loyalty_id | maid | push_token',
  id_value STRING NOT NULL, source_system STRING, is_primary BOOLEAN,
  last_seen_at TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'CDP: all identifiers per customer for activation and ad-platform matching'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_consent`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_consent (
  unified_customer_id STRING NOT NULL,
  email_marketing BOOLEAN, sms_marketing BOOLEAN, push_marketing BOOLEAN,
  paid_media_targeting BOOLEAN, personalization_profiling BOOLEAN,
  jurisdiction STRING, do_not_sell BOOLEAN, global_suppression BOOLEAN,
  suppression_reason STRING, updated_at TIMESTAMP,
  CONSTRAINT pk_customer_consent PRIMARY KEY (unified_customer_id)
) COMMENT 'CDP: contactability gate - every send/sync must filter on this'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_traits`
# COMMAND ----------

run(r"""
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
) COMMENT 'CDP: computed behavioural traits per customer'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_predictions`
# COMMAND ----------

run(r"""
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
) COMMENT 'CDP: ML model scores per customer'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_product_recommendations`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_product_recommendations (
  unified_customer_id STRING NOT NULL, rank INT NOT NULL, sku STRING, style_id STRING,
  recommendation_type STRING COMMENT 'replenish | cross_sell | upsell | new_arrival | back_in_stock',
  score DOUBLE, in_stock_in_size BOOLEAN, reason STRING, model_version STRING,
  generated_at TIMESTAMP
) COMMENT 'CDP: top-N product recommendations, size and inventory aware'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `segment_definitions`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.segment_definitions (
  segment_id STRING NOT NULL, segment_name STRING, description STRING,
  segment_type STRING COMMENT 'static | dynamic | ml | lookalike',
  rule_sql STRING, owner STRING, refresh_schedule STRING, is_active BOOLEAN,
  created_at TIMESTAMP, updated_at TIMESTAMP,
  CONSTRAINT pk_segment_definitions PRIMARY KEY (segment_id)
) COMMENT 'CDP: audience / segment definitions'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `segment_membership`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.segment_membership (
  segment_id STRING NOT NULL, unified_customer_id STRING NOT NULL,
  entered_at TIMESTAMP, exited_at TIMESTAMP, is_current BOOLEAN,
  is_holdout BOOLEAN, refreshed_at TIMESTAMP
) COMMENT 'CDP: customer x segment membership with entry/exit history'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `trigger_events`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.trigger_events (
  trigger_id STRING NOT NULL, unified_customer_id STRING NOT NULL,
  trigger_type STRING COMMENT 'cart_abandoned | browse_abandoned | back_in_stock_in_size | shoe_mileage_threshold | race_training_start | weather_rain | weather_cold | tier_upgrade_near | fit_return | post_purchase_cross_sell | drop_early_access | lapse_risk',
  trigger_ts TIMESTAMP, payload MAP<STRING, STRING>, sku STRING, campaign_id STRING,
  status STRING COMMENT 'new | sent | suppressed | expired', expires_at TIMESTAMP
) COMMENT 'CDP: real-time / near-real-time journey triggers'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `product_catalog_enriched`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.product_catalog_enriched (
  sku STRING NOT NULL, style_id STRING, style_name STRING, category STRING,
  sport STRING, gender STRING, colorway STRING, size STRING, width STRING,
  current_price DECIMAL(10,2), msrp DECIMAL(10,2), is_on_markdown BOOLEAN,
  available_qty_online INT, available_qty_stores INT, stock_status STRING COMMENT 'in_stock | low_stock | out_of_stock',
  avg_rating DOUBLE, review_count INT, fit_feedback STRING, is_new_arrival BOOLEAN,
  is_limited_drop BOOLEAN, image_url STRING, product_url STRING, updated_at TIMESTAMP,
  CONSTRAINT pk_product_catalog_enriched PRIMARY KEY (sku)
) COMMENT 'CDP: product feed for dynamic content / personalization'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `audience_exports`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.audience_exports (
  export_id STRING NOT NULL, segment_id STRING, campaign_id STRING,
  destination STRING COMMENT 'esp | sms | push | meta | google | tiktok | youtube',
  unified_customer_id STRING, matched_id_type STRING, exported_at TIMESTAMP,
  sync_status STRING
) COMMENT 'CDP: audit trail of audiences pushed to destinations'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `customer_daily_snapshot`
# MAGIC
# MAGIC ================= Analytics / supporting gold =================
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.customer_daily_snapshot (
  snapshot_date DATE NOT NULL, unified_customer_id STRING NOT NULL,
  lifecycle_stage STRING, rfm_segment STRING, loyalty_tier STRING,
  revenue_12m DECIMAL(14,2), order_count_12m INT, churn_probability DOUBLE,
  predicted_clv_12m DECIMAL(14,2), email_contactable BOOLEAN
) COMMENT 'Point-in-time customer state for trends and model training'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `product_affinity`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.product_affinity (
  antecedent_style_id STRING NOT NULL, consequent_style_id STRING NOT NULL,
  antecedent_category STRING, consequent_category STRING, support DOUBLE,
  confidence DOUBLE, lift DOUBLE, computed_at TIMESTAMP
) COMMENT 'Basket affinity (e.g. running shoe -> socks / shorts)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `drop_calendar`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.drop_calendar (
  drop_id STRING NOT NULL, style_id STRING, drop_name STRING, release_ts TIMESTAMP,
  units_available INT, eligible_segment_id STRING, eligible_customer_count BIGINT,
  early_access_start_ts TIMESTAMP, is_raffle BOOLEAN
) COMMENT 'Upcoming drops with eligible audience'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_performance`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_performance (
  campaign_id STRING NOT NULL, campaign_name STRING, segment_id STRING,
  channel STRING, report_date DATE, audience_size BIGINT, sends BIGINT,
  delivered BIGINT, opens BIGINT, clicks BIGINT, unsubscribes BIGINT,
  impressions BIGINT, spend DECIMAL(14,2), conversions BIGINT,
  attributed_revenue DECIMAL(14,2), attributed_margin DECIMAL(14,2),
  open_rate DOUBLE, click_rate DOUBLE, conversion_rate DOUBLE, roas DOUBLE
) COMMENT 'Daily campaign KPIs by segment and channel'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_attribution`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_attribution (
  order_id STRING NOT NULL, unified_customer_id STRING, order_ts TIMESTAMP,
  order_revenue DECIMAL(12,2), campaign_id STRING, channel STRING,
  touchpoint_ts TIMESTAMP, attribution_model STRING COMMENT 'last_touch | first_touch | linear | time_decay | data_driven',
  attribution_weight DOUBLE, attributed_revenue DECIMAL(12,2)
) COMMENT 'Multi-touch attribution of orders to campaigns'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_holdout_results`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_holdout_results (
  campaign_id STRING NOT NULL, segment_id STRING, measurement_window_days INT,
  test_customers BIGINT, control_customers BIGINT, test_conversion_rate DOUBLE,
  control_conversion_rate DOUBLE, test_revenue_per_customer DECIMAL(12,2),
  control_revenue_per_customer DECIMAL(12,2), incremental_lift_pct DOUBLE,
  incremental_revenue DECIMAL(14,2), p_value DOUBLE, computed_at TIMESTAMP
) COMMENT 'Test vs control incrementality'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `cohort_retention`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.cohort_retention (
  cohort_month DATE NOT NULL, acquisition_channel STRING, months_since_acquisition INT,
  cohort_size BIGINT, active_customers BIGINT, retention_rate DOUBLE,
  cumulative_revenue DECIMAL(14,2), revenue_per_customer DECIMAL(12,2)
) COMMENT 'Acquisition cohort retention and revenue curves'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `channel_kpis_daily`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.channel_kpis_daily (
  report_date DATE NOT NULL, channel STRING, sessions BIGINT, orders BIGINT,
  customers BIGINT, new_customers BIGINT, revenue DECIMAL(14,2), aov DECIMAL(12,2),
  conversion_rate DOUBLE, return_rate DOUBLE, cart_abandonment_rate DOUBLE
) COMMENT 'Daily KPIs by sales channel'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `store_kpis_daily`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.store_kpis_daily (
  report_date DATE NOT NULL, store_id STRING, region STRING, transactions BIGINT,
  customers BIGINT, identified_customer_pct DOUBLE, revenue DECIMAL(14,2),
  units BIGINT, aov DECIMAL(12,2), returns BIGINT, gait_analyses INT,
  run_club_checkins INT, bopis_orders BIGINT
) COMMENT 'Daily KPIs by store'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `identity_resolution_stats`
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.identity_resolution_stats (
  run_ts TIMESTAMP NOT NULL, source_records BIGINT, unified_customers BIGINT,
  known_customers BIGINT, anonymous_profiles BIGINT, duplicates_collapsed BIGINT,
  dedup_rate DOUBLE, avg_ids_per_customer DOUBLE, multi_source_customer_pct DOUBLE,
  deterministic_matches BIGINT, probabilistic_matches BIGINT,
  match_rate_by_source MAP<STRING, DOUBLE>
) COMMENT 'Identity resolution run statistics'
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
