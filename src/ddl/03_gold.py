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
# MAGIC ### Create `customer_identity_map`
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
# MAGIC ### Create `customer_consent`
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
# MAGIC ### Create `customer_traits`
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
# MAGIC ### Create `customer_predictions`
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
# MAGIC ### Create `customer_product_recommendations`
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
# MAGIC ### Create `segment_definitions`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.segment_definitions (
  segment_id STRING NOT NULL COMMENT 'Audience id from segment_definitions (e.g. SEG-MILEAGE-REPLACE).',
  segment_name STRING COMMENT 'Audience display name.',
  description STRING COMMENT 'What the audience is for.',
  segment_type STRING COMMENT 'static, dynamic, ml or lookalike.',
  rule_sql STRING COMMENT 'SQL WHERE-clause evaluated over profile + traits + predictions to compute membership.',
  owner STRING COMMENT 'Owning team.',
  refresh_schedule STRING COMMENT 'How often membership is recomputed.',
  is_active BOOLEAN COMMENT 'Whether the audience is live.',
  created_at TIMESTAMP COMMENT 'When the definition was created.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
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
  segment_id STRING NOT NULL COMMENT 'Audience id from segment_definitions (e.g. SEG-MILEAGE-REPLACE).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  entered_at TIMESTAMP COMMENT 'When the customer entered the audience.',
  exited_at TIMESTAMP COMMENT 'When the customer left the audience (null while a member).',
  is_current BOOLEAN COMMENT 'Customer is currently a member.',
  is_holdout BOOLEAN COMMENT 'Deterministic 10% holdout (excluded from exports) for lift measurement.',
  refreshed_at TIMESTAMP COMMENT 'When membership was last recomputed.'
) COMMENT 'CDP audiences: customer x segment membership with a 10% deterministic holdout flag. Derived by evaluating each segment_definitions rule over gold customer_profile + customer_traits + customer_predictions. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `trigger_events`
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
) COMMENT 'CDP journey triggers as of the latest data day: cart / browse abandonment, back in stock in size, shoe mileage threshold, race training start, heavy-rain trail, tier upgrade, fit return, lapse risk; status suppressed when not contactable. Derived from silver carts, sessions, web_app_events, shoe_mileage, returns and gold product_catalog_enriched, segment_membership, customer_traits/predictions. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
TBLPROPERTIES ('cdp_activation' = 'true')
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `product_catalog_enriched`
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
  stock_status STRING COMMENT 'in_stock, low_stock (<10 online) or out_of_stock.',
  avg_rating DOUBLE COMMENT 'Average review rating of the style.',
  review_count INT COMMENT 'Number of reviews of the style.',
  fit_feedback STRING COMMENT 'Most common fit feedback in reviews (runs_small, true_to_size, runs_large).',
  is_new_arrival BOOLEAN COMMENT 'Style launched in the last 60 days.',
  is_limited_drop BOOLEAN COMMENT 'Style is a limited collab drop.',
  image_url STRING COMMENT 'Main product image URL.',
  product_url STRING COMMENT 'Product detail page URL for the SKU.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
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
  export_id STRING NOT NULL COMMENT 'Unique export row id.',
  segment_id STRING COMMENT 'Audience id from segment_definitions (e.g. SEG-MILEAGE-REPLACE).',
  campaign_id STRING COMMENT 'Campaign id: EM-/SMS-/PUSH- owned campaigns, TRG- triggered flows, ME-/GO-/TI-/YO- paid campaigns (Meta/Google/TikTok/YouTube).',
  destination STRING COMMENT 'Destination platform: esp, meta, google.',
  unified_customer_id STRING COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  matched_id_type STRING COMMENT 'Identifier sent for matching (email for ESP, email_sha256 for ad platforms).',
  exported_at TIMESTAMP COMMENT 'When the audience was synced.',
  sync_status STRING COMMENT 'Sync outcome (synced).'
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
  snapshot_date DATE NOT NULL COMMENT 'Month-end date the customer state is reconstructed for (last 6 months).',
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  lifecycle_stage STRING COMMENT 'prospect (no linked purchase), new (first order <=90d), active, loyal (>=5 orders in 12m), at_risk (181-365d since last order), lapsed (>365d).',
  rfm_segment STRING COMMENT 'Simplified segment at snapshot: champions (>=5 orders 12m), loyal (>=2), promising (1), hibernating (0).',
  loyalty_tier STRING COMMENT 'Current loyalty tier (not reconstructed historically).',
  revenue_12m DECIMAL(14,2) COMMENT 'Net merchandise revenue (subtotal - discount) in the last 12 months (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  order_count_12m INT COMMENT 'Number of orders in the last 12 months.',
  churn_probability DOUBLE COMMENT 'Probability the customer does not purchase again (0-1); logistic score on recency, 12m frequency, fit returns and recent sessions (heuristic_v1).',
  predicted_clv_12m DECIMAL(14,2) COMMENT 'Predicted 12-month customer lifetime value (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93) = revenue_12m x (1 - churn_probability) x 1.08 (heuristic_v1).',
  email_contactable BOOLEAN COMMENT 'Current email contactability (not reconstructed historically).'
) COMMENT 'Month-end customer snapshots for the last 6 months: lifecycle, RFM-style segment, tier, 12-month revenue/orders, churn and CLV as of each snapshot. Derived from silver orders and gold customer_profile. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `product_affinity`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.product_affinity (
  antecedent_style_id STRING NOT NULL COMMENT 'Style bought first (if-part of the rule).',
  consequent_style_id STRING NOT NULL COMMENT 'Style also bought in the same order (then-part).',
  antecedent_category STRING COMMENT 'Category of the antecedent style.',
  consequent_category STRING COMMENT 'Category of the consequent style.',
  support DOUBLE COMMENT 'Share of multi-item orders containing both styles.',
  confidence DOUBLE COMMENT 'P(consequent | antecedent) within multi-item orders.',
  lift DOUBLE COMMENT 'confidence / P(consequent); >1 means bought together more than by chance.',
  computed_at TIMESTAMP COMMENT 'When the rule was computed.'
) COMMENT 'Style-level basket affinity (support, confidence, lift) for cross-sell, pairs seen in >=30 multi-item orders. Derived from silver order_lines. Built by job strideforge_medallion, task gold_predictions_products (notebook gold/02_predictions_products).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `drop_calendar`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.drop_calendar (
  drop_id STRING NOT NULL COMMENT 'Limited drop id (DRP...).',
  style_id STRING COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  drop_name STRING COMMENT 'Drop / collab style name.',
  release_ts TIMESTAMP COMMENT 'Release date and time.',
  units_available INT COMMENT 'Units allocated to the drop.',
  eligible_segment_id STRING COMMENT 'Audience eligible for early access (SEG-SNEAKERHEADS).',
  eligible_customer_count BIGINT COMMENT 'Current size of the eligible audience.',
  early_access_start_ts TIMESTAMP COMMENT 'Member early-access start (release - 1 day) when enabled.',
  is_raffle BOOLEAN COMMENT 'Drop is sold by raffle.'
) COMMENT 'Limited drops with the eligible audience (sneakerheads) and early-access window. Derived from silver product_drops and gold segment_membership. Built by job strideforge_medallion, task gold_audiences (notebook gold/03_audiences).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_performance`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_performance (
  campaign_id STRING NOT NULL COMMENT 'Campaign id: EM-/SMS-/PUSH- owned campaigns, TRG- triggered flows, ME-/GO-/TI-/YO- paid campaigns (Meta/Google/TikTok/YouTube).',
  campaign_name STRING COMMENT 'Campaign display name.',
  segment_id STRING COMMENT 'Targeted audience (null when not linked).',
  channel STRING COMMENT 'Channel of the record (web/app/store for commerce; email/sms/push for messaging; platform for paid).',
  report_date DATE COMMENT 'Calendar date the metrics are reported for.',
  audience_size BIGINT COMMENT 'Distinct customers sent that day.',
  sends BIGINT COMMENT 'Messages sent.',
  delivered BIGINT COMMENT 'Sends minus bounces.',
  opens BIGINT COMMENT 'Distinct messages opened.',
  clicks BIGINT COMMENT 'Distinct messages clicked (or ad clicks for paid).',
  unsubscribes BIGINT COMMENT 'Unsubscribes attributed to the campaign.',
  impressions BIGINT COMMENT 'Ad impressions (paid only).',
  spend DECIMAL(14,2) COMMENT 'Paid media spend in the account currency.',
  conversions BIGINT COMMENT 'Orders credited by last-touch attribution.',
  attributed_revenue DECIMAL(14,2) COMMENT 'Last-touch attributed revenue (USD).',
  attributed_margin DECIMAL(14,2) COMMENT 'Last-touch attributed gross margin.',
  open_rate DOUBLE COMMENT 'opens / delivered.',
  click_rate DOUBLE COMMENT 'clicks / delivered (owned) or clicks / impressions (paid).',
  conversion_rate DOUBLE COMMENT 'conversions / delivered (owned) or conversions / clicks (paid).',
  roas DOUBLE COMMENT 'attributed_revenue / spend (paid only).'
) COMMENT 'Daily campaign KPIs per campaign and channel: audience, sends, delivered, opens, clicks, unsubscribes, impressions, spend, attributed conversions/revenue/margin, rates and ROAS. Derived from silver message_events, paid_media_performance, campaigns, order_lines and gold campaign_attribution. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_attribution`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_attribution (
  order_id STRING NOT NULL COMMENT 'Order id: SF<country>... for e-commerce orders, TX... for store transactions.',
  unified_customer_id STRING COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  order_ts TIMESTAMP COMMENT 'Order timestamp.',
  order_revenue DECIMAL(12,2) COMMENT 'Order net revenue (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  campaign_id STRING COMMENT 'Campaign id: EM-/SMS-/PUSH- owned campaigns, TRG- triggered flows, ME-/GO-/TI-/YO- paid campaigns (Meta/Google/TikTok/YouTube).',
  channel STRING COMMENT 'Channel of the credited touchpoint (email, sms, push, search, paid_social, platform...).',
  touchpoint_ts TIMESTAMP COMMENT 'Time of the credited touchpoint (within 7 days before the order).',
  attribution_model STRING COMMENT 'last_touch (100% to the last touch) or linear (equal split across touches).',
  attribution_weight DOUBLE COMMENT 'Share of the order credited to this touchpoint.',
  attributed_revenue DECIMAL(12,2) COMMENT 'order_revenue x attribution_weight.'
) COMMENT 'Order-to-campaign attribution for the last 12 months: last-touch and linear models over clicks, UTM visits and paid touchpoints in a 7-day lookback, revenue in USD. Derived from silver orders and marketing_touchpoints. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `campaign_holdout_results`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.campaign_holdout_results (
  campaign_id STRING NOT NULL COMMENT 'Campaign id: EM-/SMS-/PUSH- owned campaigns, TRG- triggered flows, ME-/GO-/TI-/YO- paid campaigns (Meta/Google/TikTok/YouTube).',
  segment_id STRING COMMENT 'Audience (null: all email-contactable customers).',
  measurement_window_days INT COMMENT 'Days after send counted for conversion (7).',
  test_customers BIGINT COMMENT 'Customers who received the campaign.',
  control_customers BIGINT COMMENT 'Email-contactable customers who did not receive it.',
  test_conversion_rate DOUBLE COMMENT 'Share of test customers ordering in the window.',
  control_conversion_rate DOUBLE COMMENT 'Share of control customers ordering in the window.',
  test_revenue_per_customer DECIMAL(12,2) COMMENT 'Revenue per test customer in the window (USD).',
  control_revenue_per_customer DECIMAL(12,2) COMMENT 'Revenue per control customer (USD).',
  incremental_lift_pct DOUBLE COMMENT '(test - control) / control conversion, in %.',
  incremental_revenue DECIMAL(14,2) COMMENT '(test - control revenue per customer) x test customers.',
  p_value DOUBLE COMMENT 'Approximate two-sided p-value of the conversion difference (two-proportion z-test).',
  computed_at TIMESTAMP COMMENT 'When results were computed.'
) COMMENT 'Incrementality for the 20 largest batch email campaigns: recipients vs email-contactable customers not sent, 7-day conversion and revenue, lift and approximate p-value. Derived from silver campaigns, message_events, orders and gold customer_profile. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `cohort_retention`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.cohort_retention (
  cohort_month DATE NOT NULL COMMENT 'Month of the customer\'s first order.',
  acquisition_channel STRING COMMENT 'Channel of the first order: online or store.',
  months_since_acquisition INT COMMENT 'Months between first order and the activity month.',
  cohort_size BIGINT COMMENT 'Customers acquired in the cohort.',
  active_customers BIGINT COMMENT 'Cohort customers ordering in that month.',
  retention_rate DOUBLE COMMENT 'active_customers / cohort_size.',
  cumulative_revenue DECIMAL(14,2) COMMENT 'Cohort revenue to date (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  revenue_per_customer DECIMAL(12,2) COMMENT 'cumulative_revenue / cohort_size.'
) COMMENT 'Acquisition cohort retention and cumulative revenue per customer by month since first order and acquisition channel (online/store). Derived from silver orders. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `channel_kpis_daily`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.channel_kpis_daily (
  report_date DATE NOT NULL COMMENT 'Calendar date the metrics are reported for.',
  channel STRING COMMENT 'Sales channel: web, app or store.',
  sessions BIGINT COMMENT 'Web/app sessions (analytics window only; null for store).',
  orders BIGINT COMMENT 'Orders.',
  customers BIGINT COMMENT 'Distinct identified customers ordering.',
  new_customers BIGINT COMMENT 'Customers placing their first order that day.',
  revenue DECIMAL(14,2) COMMENT 'Net revenue (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  aov DECIMAL(12,2) COMMENT 'Revenue / orders.',
  conversion_rate DOUBLE COMMENT 'orders / sessions.',
  return_rate DOUBLE COMMENT 'Returned lines / lines for orders placed that day.',
  cart_abandonment_rate DOUBLE COMMENT 'Abandoned carts / carts created (web+app).'
) COMMENT 'Daily KPIs per sales channel (web, app, store): sessions, orders, customers, new customers, revenue (USD), AOV, conversion, return and cart-abandonment rates. Derived from silver orders, sessions, order_lines, returns and carts. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `store_kpis_daily`
# COMMAND ----------

ddl(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${gold}.store_kpis_daily (
  report_date DATE NOT NULL COMMENT 'Calendar date the metrics are reported for.',
  store_id STRING COMMENT 'Store id (ST....).',
  region STRING COMMENT 'Store region (US-West, US-East, US-Central, US-South, UK & Ireland, DACH, Canada).',
  transactions BIGINT COMMENT 'Store transactions.',
  customers BIGINT COMMENT 'Distinct identified customers.',
  identified_customer_pct DOUBLE COMMENT 'Share of transactions linked to a customer.',
  revenue DECIMAL(14,2) COMMENT 'Net revenue (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  units BIGINT COMMENT 'Units sold.',
  aov DECIMAL(12,2) COMMENT 'Revenue / transactions.',
  returns BIGINT COMMENT 'Returns processed for store purchases.',
  gait_analyses INT COMMENT 'Gait-lab scans performed.',
  run_club_checkins INT COMMENT 'Run-club check-ins.',
  bopis_orders BIGINT COMMENT 'Online orders picked up in store.'
) COMMENT 'Daily KPIs per store: transactions, identified-customer share, revenue, units, AOV, returns, gait analyses, run-club check-ins, BOPIS pickups. Derived from silver orders, returns, run_club_attendance, stores and bronze gait_analysis_raw. Built by job strideforge_medallion, task gold_measurement (notebook gold/04_measurement).'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `identity_resolution_stats`
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
