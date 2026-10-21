# Databricks notebook source
# MAGIC %md
# MAGIC # Create the Customer Lake (CDP) views
# MAGIC
# MAGIC **What it does:** Creates the `retail_cdp` schema with every object the Customer Lake entity uses: 1:1 views over the gold tables and the product-enriched event views. All views read the gold tables directly (no view on a view).
# MAGIC
# MAGIC - **Reads:** gold tables
# MAGIC - **Writes:** `retail_cdp` schema and views
# MAGIC - **Runs in:** `strideforge_bronze_setup` -> `ddl_cdp`; `strideforge_medallion` -> `ensure_cdp_views`
# MAGIC
# MAGIC Views are created once (`IF NOT EXISTS`) so Customer Lake keeps pointing at the same objects. To change a view, drop it first.
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Create the CDP schema
# COMMAND ----------

run(r"""
CREATE SCHEMA IF NOT EXISTS ${catalog}.${cdp}
COMMENT 'StrideForge CDP demo - CDP: the objects the Customer Lake entity points at. 1:1 views over retail_gold tables plus product-enriched event views; created by notebook ddl/04_cdp. Data is refreshed in retail_gold by job strideforge_medallion.'
""")
run(r"COMMENT ON SCHEMA ${catalog}.${cdp} IS 'StrideForge CDP demo - CDP: the objects the Customer Lake entity points at. 1:1 views over retail_gold tables plus product-enriched event views; created by notebook ddl/04_cdp. Data is refreshed in retail_gold by job strideforge_medallion.'")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - `customer_profile` - Customer profile (entity profile table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.customer_profile (
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  first_name COMMENT 'Best first name across sources (survivorship: ecom > loyalty > app > POS > run app > race > guest).',
  last_name COMMENT 'Best last name across sources (same survivorship).',
  email COMMENT 'Best email (lower-cased) across sources.',
  phone_e164 COMMENT 'Most recently updated phone number, normalized to E.164 (+1..., +44..., +49...).',
  birth_date COMMENT 'Birth date from the highest-priority source that has one.',
  age_band COMMENT 'Age band from birth_date: 16-24, 25-34, 35-44, 45-54, 55+, unknown.',
  gender COMMENT 'F, M, X (non-binary) or null when no source captured it.',
  city COMMENT 'City of the primary address.',
  state COMMENT 'State / province / region of the primary address.',
  postal_code COMMENT 'Postal code of the primary address (US/DE 5-digit, UK and CA alphanumeric).',
  country COMMENT 'Country code: US, UK, DE, CA.',
  household_id COMMENT 'Household id (HH + hash of standardized primary street + postal code); joins to household_profile.',
  kids_age_bands COMMENT 'Kids age bands inferred from kids-size purchases in the household (0-5, 6-9, 10-14, 6-14).',
  home_store_id COMMENT 'Store the customer shops most (POS orders), else the store where they registered; joins to silver.stores.',
  nearest_store_id COMMENT 'Home store if known, else a flagship/standard store in the customer\'s city.',
  nearest_store_distance_km COMMENT 'Distance to nearest_store_id (not computed in this demo; customer coordinates are not geocoded).',
  loyalty_id COMMENT 'Primary loyalty membership id (LY...).',
  loyalty_tier COMMENT 'Loyalty tier on the 12-month points window: Rookie, Pro (>=300 pts), Elite (>=1000 pts).',
  points_balance COMMENT 'Current loyalty points balance.',
  shoe_size_us COMMENT 'Shoe size (US) from the latest gait-lab scan, else the most common kept footwear size purchased.',
  shoe_width COMMENT 'Shoe width (D, 2E for men; B for women).',
  pronation_type COMMENT 'neutral, overpronation or supination (gait lab).',
  apparel_top_size COMMENT 'Most purchased tops size (XS-XXL).',
  apparel_bottom_size COMMENT 'Most purchased bottoms size (XS-XXL).',
  is_known COMMENT 'True for identified customers (all rows here; anonymous cookies are not profiled).',
  has_app COMMENT 'Has at least one StrideForge app device.',
  has_fitness_app COMMENT 'Has a StrideForge Run app account.',
  email_contactable COMMENT 'True when email marketing consent is opted in, the customer is not suppressed and an email is on file.',
  sms_contactable COMMENT 'SMS consent opted in and a phone on file.',
  push_contactable COMMENT 'Push consent opted in and an app device.',
  paid_media_eligible COMMENT 'May be targeted on ad platforms (GDPR/UK-GDPR explicit opt-in; elsewhere unless do-not-sell) and not suppressed.',
  first_seen_at COMMENT 'Earliest creation time across the customer\'s source records.',
  last_seen_at COMMENT 'Latest update/activity time across the customer\'s source records.',
  updated_at COMMENT 'When the job last rebuilt this row.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.customer_profile (same rows and columns), created by notebook ddl/04_cdp. Source: CDP activation: Customer 360 profile - one row per unified customer (1.53M) with contact details, primary address, household, home/nearest store, loyalty tier and balance, fit profile, app/run-app flags and per-channel contactability. Customer Lake profile table for entity "Retail - StrideForge Customer". Derived from silver customers, customer_addresses, households, loyalty_members, customer_fit_profile, identity_graph and gold customer_consent. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT unified_customer_id, first_name, last_name, email, phone_e164, birth_date,
       age_band, gender, city, state, postal_code, country,
       household_id, kids_age_bands, home_store_id, nearest_store_id, nearest_store_distance_km, loyalty_id,
       loyalty_tier, points_balance, shoe_size_us, shoe_width, pronation_type, apparel_top_size,
       apparel_bottom_size, is_known, has_app, has_fitness_app, email_contactable, sms_contactable,
       push_contactable, paid_media_eligible, first_seen_at, last_seen_at, updated_at
FROM ${catalog}.${gold}.customer_profile
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - `customer_consent` - Consent per customer (1:1)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.customer_consent (
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  email_marketing COMMENT 'Latest email marketing consent is opted in and the customer is not globally suppressed.',
  sms_marketing COMMENT 'Latest SMS marketing consent is opted in.',
  push_marketing COMMENT 'Latest push consent is opted in.',
  paid_media_targeting COMMENT 'Allowed for ad-platform targeting: explicit opt-in under GDPR/UK-GDPR, otherwise true unless do-not-sell.',
  personalization_profiling COMMENT 'Allowed for profiling/personalization: explicit opt-in under GDPR/UK-GDPR, otherwise true.',
  jurisdiction COMMENT 'Privacy regime: GDPR (DE), UK-GDPR, PIPEDA (CA), CCPA (California), US-STATE.',
  do_not_sell COMMENT 'Customer exercised the CCPA/US do-not-sell (sale of data) opt-out.',
  global_suppression COMMENT 'Suppress from all sends: spam complaint or 2+ bounces.',
  suppression_reason COMMENT 'spam_complaint or repeated_bounce when globally suppressed.',
  updated_at COMMENT 'When the job last rebuilt this row.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.customer_consent (same rows and columns), created by notebook ddl/04_cdp. Source: CDP activation: contactability gate per customer - email/SMS/push marketing, paid media targeting, profiling, do-not-sell and global suppression (repeated bounces / complaints). Derived from silver consent (GDPR/UK-GDPR require explicit opt-in for paid media/profiling; US/CA default allowed unless do-not-sell) and message_events. Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT unified_customer_id, email_marketing, sms_marketing, push_marketing, paid_media_targeting, personalization_profiling,
       jurisdiction, do_not_sell, global_suppression, suppression_reason, updated_at
FROM ${catalog}.${gold}.customer_consent
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - `customer_traits` - Behaviour traits per customer (1:1)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.customer_traits (
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  first_order_date COMMENT 'Date of the first linked order in the data window (Oct 2023 onward).',
  last_order_date COMMENT 'Date of the most recent linked order.',
  days_since_last_order COMMENT 'Days between last_order_date and the data as-of date.',
  order_count_lifetime COMMENT 'Number of linked orders in the data window.',
  order_count_12m COMMENT 'Number of orders in the last 12 months.',
  revenue_lifetime COMMENT 'Net merchandise revenue in the data window (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  revenue_12m COMMENT 'Net merchandise revenue (subtotal - discount) in the last 12 months (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  aov_lifetime COMMENT 'Average order value (revenue_lifetime / order_count_lifetime) (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).',
  margin_12m COMMENT 'Gross margin of order lines in the last 12 months (line total - cost).',
  recency_score COMMENT 'Recency quintile 1-5 (5 = most recent buyers).',
  frequency_score COMMENT 'Frequency quintile 1-5 (5 = most orders).',
  monetary_score COMMENT 'Monetary quintile 1-5 (5 = highest revenue).',
  rfm_segment COMMENT 'RFM segment from recency/frequency/monetary quintiles: champions, loyal, promising, needs_attention, at_risk, hibernating, no_purchase.',
  lifecycle_stage COMMENT 'prospect (no linked purchase), new (first order <=90d), active, loyal (>=5 orders in 12m), at_risk (181-365d since last order), lapsed (>365d).',
  preferred_channel COMMENT 'Channel with the most orders: web, app or store.',
  preferred_store_id COMMENT 'Store with the most POS orders.',
  top_category COMMENT 'Category with the most units purchased.',
  top_sport COMMENT 'Sport with the most units purchased.',
  category_affinity COMMENT 'Share of units purchased per category (map category -> 0-1).',
  sport_affinity COMMENT 'Share of units purchased per sport (map sport -> 0-1).',
  full_price_ratio COMMENT 'Share of orders with no markdown and no discount.',
  discount_sensitivity COMMENT 'full_price (>=80% full-price orders), mixed (45-80%), deal_seeker (<45%).',
  return_rate COMMENT 'Returned lines / purchased lines.',
  fit_return_count COMMENT 'Number of returns for too_small or too_big.',
  is_gifter COMMENT 'Bought kids items or both men\'s and women\'s styles.',
  is_omnichannel COMMENT 'Purchased both online and in store.',
  limited_drop_purchases COMMENT 'Number of limited-drop lines purchased.',
  sessions_30d COMMENT 'Web/app sessions in the last 30 days.',
  product_views_30d COMMENT 'Product views in the last 30 days.',
  email_open_rate_90d COMMENT 'Distinct emails opened / emails sent in the last 90 days.',
  email_click_rate_90d COMMENT 'Distinct emails clicked / emails sent in the last 90 days.',
  run_miles_30d COMMENT 'Miles logged in the run app in the last 30 days.',
  run_miles_90d COMMENT 'Miles logged in the run app in the last 90 days.',
  upcoming_race_date COMMENT 'Date of the next registered race of any distance.',
  upcoming_race_distance_km COMMENT 'Distance (km) of the next registered race.',
  run_club_checkins_90d COMMENT 'Store run-club check-ins in the last 90 days.',
  nps_latest COMMENT 'Most recent NPS score (0-10).',
  open_service_tickets COMMENT 'Tickets still open or pending.',
  review_count COMMENT 'Product reviews written.',
  avg_review_rating COMMENT 'Average review rating (1-5).',
  updated_at COMMENT 'When the job last rebuilt this row.',
  goal_race_date COMMENT 'Date of the next registered half or full marathon (training goal).',
  goal_race_distance_km COMMENT 'Distance (km) of the goal race (21.1 or 42.2).'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.customer_traits (same rows and columns), created by notebook ddl/04_cdp. Source: CDP activation: behavioural traits per customer - order history (USD), RFM scores and segment, lifecycle stage, preferred channel/store, category and sport affinity maps, discount sensitivity, returns, gifting, digital and email engagement, run miles, upcoming and goal race, run club, NPS, tickets, reviews. Derived from silver orders, order_lines, returns, sessions, message_events, fitness_activities, event_registrations, run_club_attendance, surveys, service_tickets and product_reviews (as of the latest order date). Built by job strideforge_medallion, task gold_customer_360 (notebook gold/01_customer_360).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT unified_customer_id, first_order_date, last_order_date, days_since_last_order, order_count_lifetime, order_count_12m,
       revenue_lifetime, revenue_12m, aov_lifetime, margin_12m, recency_score, frequency_score,
       monetary_score, rfm_segment, lifecycle_stage, preferred_channel, preferred_store_id, top_category,
       top_sport, category_affinity, sport_affinity, full_price_ratio, discount_sensitivity, return_rate,
       fit_return_count, is_gifter, is_omnichannel, limited_drop_purchases, sessions_30d, product_views_30d,
       email_open_rate_90d, email_click_rate_90d, run_miles_30d, run_miles_90d, upcoming_race_date, upcoming_race_distance_km,
       run_club_checkins_90d, nps_latest, open_service_tickets, review_count, avg_review_rating, updated_at,
       goal_race_date, goal_race_distance_km
FROM ${catalog}.${gold}.customer_traits
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - `trigger_events` - Journey triggers (event table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.trigger_events (
  trigger_id COMMENT 'Unique trigger id.',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  trigger_type COMMENT 'cart_abandoned, browse_abandoned, back_in_stock_in_size, shoe_mileage_threshold, race_training_start, weather_rain, tier_upgrade_near, fit_return, lapse_risk.',
  trigger_ts COMMENT 'When the triggering behaviour happened (or the as-of time for state-based triggers).',
  payload COMMENT 'Trigger context (e.g. cart_id/cart_value, miles, goal race date, return reason).',
  sku COMMENT 'Product involved, when applicable.',
  campaign_id COMMENT 'Journey / triggered campaign to start (TRG-...).',
  status COMMENT 'new (contactable) or suppressed (no consented channel).',
  expires_at COMMENT 'Trigger is stale after this time (trigger_ts + 7 days).'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.trigger_events (same rows and columns), created by notebook ddl/04_cdp. Source: CDP journey triggers as of the latest data day: cart / browse abandonment, back in stock in size, shoe mileage threshold, race training start, heavy-rain trail, tier upgrade, fit return, lapse risk; status suppressed when not contactable. Derived from silver carts, sessions, web_app_events, shoe_mileage, returns and gold product_catalog_enriched, customer_profile, customer_traits/predictions. Built by job strideforge_medallion, task gold_triggers (notebook gold/03_triggers).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT trigger_id, unified_customer_id, trigger_type, trigger_ts, payload, sku,
       campaign_id, status, expires_at
FROM ${catalog}.${gold}.trigger_events
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - `support_events` - Support tickets (event table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.support_events (
  ticket_id COMMENT 'Unique id of the support ticket (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  created_at COMMENT 'Event timestamp: ticket created.',
  channel COMMENT 'chat, email, phone or social.',
  category COMMENT 'order_status, returns_refunds, sizing_fit, product_defect, promo_pricing, loyalty_points, account_login.',
  priority COMMENT 'high, normal or low.',
  status COMMENT 'open, pending, solved or closed.',
  sentiment COMMENT 'Rule-based sentiment (negative/neutral).',
  resolution_hours COMMENT 'Hours to resolution (null while open).',
  order_id COMMENT 'Related order, when referenced.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.support_events (same rows and columns), created by notebook ddl/04_cdp. Source: Customer Lake event table: service tickets (event id ticket_id, timestamp created_at) with category, status, sentiment. Derived from silver service_tickets. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT ticket_id, unified_customer_id, created_at, channel, category, priority,
       status, sentiment, resolution_hours, order_id
FROM ${catalog}.${gold}.support_events
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - `review_events` - Product reviews (event table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.review_events (
  review_id COMMENT 'Unique id of the product review (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  submitted_at COMMENT 'Event timestamp: review submitted.',
  sku COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).',
  style_id COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  style_name COMMENT 'Product style name (e.g. CloudStride 3 Men\'s).',
  category COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  rating COMMENT 'Star rating 1-5.',
  fit_rating COMMENT 'Reviewer-selected fit (runs_small, true_to_size, runs_large).',
  ai_sentiment COMMENT 'Sentiment (ai_analyze_sentiment for the 20K most recent reviews, rating-based otherwise).',
  ai_fit_feedback COMMENT 'Fit feedback extracted from the text (ai_classify for recent reviews, keyword rules otherwise).',
  ai_themes COMMENT 'Themes mentioned: fit, comfort, durability, style, price.',
  verified_purchase COMMENT 'Reviewer bought the product.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.review_events (same rows and columns), created by notebook ddl/04_cdp. Source: Customer Lake event table: product reviews (event id review_id, timestamp submitted_at) with rating and AI fit/sentiment. Derived from silver product_reviews + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT review_id, unified_customer_id, submitted_at, sku, style_id, style_name,
       category, rating, fit_rating, ai_sentiment, ai_fit_feedback, ai_themes,
       verified_purchase
FROM ${catalog}.${gold}.review_events
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - `race_registration_events` - Race registrations (event table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.race_registration_events (
  registration_id COMMENT 'Unique id of the race registration (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  registered_at COMMENT 'Event timestamp: when the customer registered.',
  event_id COMMENT 'Id of the race the customer registered for.',
  event_name COMMENT 'Name of the race (e.g. Chicago Marathon 2026).',
  event_type COMMENT 'marathon, half_marathon, 10k or 5k.',
  event_date COMMENT 'Race date.',
  event_city COMMENT 'City where the race takes place.',
  distance_km COMMENT 'Race distance in kilometres (5, 10, 21.1 or 42.2).',
  registration_source COMMENT 'RaceRoster, Active.com, RunSignup or StrideForge App.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.race_registration_events (same rows and columns), created by notebook ddl/04_cdp. Source: Customer Lake event table: race registrations (event id registration_id, timestamp registered_at) with race date, type and distance. Derived from silver event_registrations. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT registration_id, unified_customer_id, registered_at, event_id, event_name, event_type,
       event_date, event_city, distance_km, registration_source
FROM ${catalog}.${gold}.race_registration_events
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - `message_engagement_events` - Email / SMS / push engagement (event table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.message_engagement_events (
  message_event_id COMMENT 'Unique id of the send or engagement event (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  event_ts COMMENT 'Event timestamp.',
  channel COMMENT 'email, sms or push.',
  event_type COMMENT 'send, open, click, bounce, unsubscribe or complaint.',
  campaign_id COMMENT 'Campaign id: EM-/SMS-/PUSH- owned campaigns, TRG- triggered flows, ME-/GO-/TI-/YO- paid campaigns (Meta/Google/TikTok/YouTube).',
  campaign_name COMMENT 'Campaign display name.',
  campaign_objective COMMENT 'engagement, promotion, launch, onboarding, retention, replenishment, win_back.',
  campaign_type COMMENT 'batch or triggered.',
  variant COMMENT 'A/B test variant (A or B).',
  link_url COMMENT 'Clicked URL, for clicks.',
  device_type COMMENT 'Device: mobile/desktop/tablet on web, ios/android in the app.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.message_engagement_events (same rows and columns), created by notebook ddl/04_cdp. Source: Customer Lake event table: email/SMS/push sends and engagement (event id message_event_id, timestamp event_ts) with campaign name and objective. Derived from silver message_events + campaigns. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT message_event_id, unified_customer_id, event_ts, channel, event_type, campaign_id,
       campaign_name, campaign_objective, campaign_type, variant, link_url, device_type
FROM ${catalog}.${gold}.message_engagement_events
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10 - `loyalty_events` - Loyalty points activity (event table)
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.loyalty_events (
  ledger_id COMMENT 'Unique id of the loyalty ledger entry (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  activity_ts COMMENT 'Event timestamp.',
  loyalty_id COMMENT 'Loyalty membership id.',
  activity_type COMMENT 'earn, redeem, adjust (return), bonus, expire, opening_balance or tier_change.',
  points COMMENT 'Points added (+) or removed (-).',
  reference_id COMMENT 'Related order id, bonus code or new tier.',
  reference_type COMMENT 'order, return, promotion, inactivity or tier.'
)
COMMENT 'Customer Lake view: 1:1 copy of retail_gold.loyalty_events (same rows and columns), created by notebook ddl/04_cdp. Source: Customer Lake event table: loyalty ledger activity (event id ledger_id, timestamp activity_ts). Derived from silver loyalty_transactions. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/04_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT ledger_id, unified_customer_id, activity_ts, loyalty_id, activity_type, points,
       reference_id, reference_type
FROM ${catalog}.${gold}.loyalty_events
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 11 - `purchase_item_events` - Customer Lake event table (view): one row per order line with order and product details
# MAGIC Created once (`IF NOT EXISTS`) so the view keeps its identity for Customer Lake. To change it, drop it first.
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.purchase_item_events (
  order_line_id COMMENT 'Event id: order line id (order_id + "-" + line number).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  order_ts COMMENT 'Event timestamp: when the order was placed.',
  order_id COMMENT 'Order id; joins to purchase_events.order_id.',
  channel COMMENT 'Sales channel: web, app or store.',
  store_id COMMENT 'Store (POS) or pickup store (BOPIS).',
  country COMMENT 'Order country derived from currency: US, UK, DE, CA.',
  fulfillment_type COMMENT 'ship, bopis, ship_from_store or in_store.',
  promo_code COMMENT 'Promo / affiliate code used on the order.',
  utm_source COMMENT 'UTM source of the order (google, meta, tiktok, youtube, email, sms, braze, affiliate, referral); null = direct.',
  utm_campaign COMMENT 'UTM campaign (campaign_id for owned/paid campaigns, promo code for affiliates).',
  sku COMMENT 'SKU bought = style_id-colorway-size (e.g. RUN0123-01-105).',
  style_id COMMENT 'Product style id (e.g. RUN0123).',
  style_name COMMENT 'Product name (e.g. CloudStride 3 Men\'s).',
  category COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  sport COMMENT 'Sport of the product (Running, Trail Running, Basketball, Training, Lifestyle, Football, Multi-Sport).',
  gender COMMENT 'Product gender: Men, Women, Unisex or Kids.',
  colorway COMMENT 'Colorway name (e.g. Core Black / Cloud White).',
  size COMMENT 'Size bought (US numeric, kids C/Y, alpha XS-XXL, OS).',
  is_limited_drop COMMENT 'Product is a limited collab drop.',
  msrp COMMENT 'Manufacturer\'s suggested retail price (USD).',
  unit_price_usd COMMENT 'Price paid per unit (USD).',
  discount_usd COMMENT 'Discount on the line (USD).',
  revenue_usd COMMENT 'Net line revenue after discount (USD).',
  order_revenue_usd COMMENT 'Net merchandise revenue of the whole order (subtotal - discount, USD); repeated on each line.',
  order_total_amount COMMENT 'Order total incl. tax and shipping, local currency; repeated on each line.',
  order_discount_amount COMMENT 'Order-level promo discount, local currency; repeated on each line.',
  currency COMMENT 'Order currency (USD, CAD, GBP, EUR).',
  order_units COMMENT 'Units in the whole order.',
  is_full_price COMMENT 'Order had no markdown lines and no promo discount.',
  payment_type COMMENT 'Payment method of the order.',
  utm_medium COMMENT 'UTM medium of the order (cpc, paid_social, paid_video, email, sms, push, organic, affiliate, referral).'
)
COMMENT 'Customer Lake event table (view): one row per order line of an identified order (event id order_line_id, timestamp order_ts), flattened with order details (channel, store, promo, UTMs, order totals and flags) and product details (name, category, sport, gender, colorway, size, limited drop). Use it to filter purchases by product name or attributes. View over gold purchase_line_items + purchase_events + product_catalog_enriched; created by notebook ddl/04_cdp (schema retail_cdp).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT li.order_line_id, li.unified_customer_id, pe.order_ts, li.order_id, pe.channel, pe.store_id, pe.country,
       pe.fulfillment_type, pe.promo_code, pe.utm_source, pe.utm_campaign,
       li.sku, pc.style_id, pc.style_name, pc.category, pc.sport, pc.gender, pc.colorway, pc.size, pc.is_limited_drop, pc.msrp,
       li.unit_price_usd, li.discount_usd, li.revenue_usd,
       pe.revenue_usd AS order_revenue_usd, pe.total_amount AS order_total_amount, pe.discount_amount AS order_discount_amount,
       pe.currency, pe.units AS order_units, pe.is_full_price, pe.payment_type, pe.utm_medium
FROM ${catalog}.${gold}.purchase_line_items li
JOIN ${catalog}.${gold}.purchase_events pe USING (order_id)
LEFT JOIN ${catalog}.${gold}.product_catalog_enriched pc USING (sku)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 12 - `digital_product_events` - Customer Lake event table (view): web/app events with product details
# MAGIC Created once (`IF NOT EXISTS`) so the view keeps its identity for Customer Lake. To change it, drop it first.
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.digital_product_events (
  event_id COMMENT 'Event id: unique id of the web/app event.',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  event_ts COMMENT 'Event timestamp.',
  platform COMMENT 'web, ios or android.',
  event_type COMMENT 'page_view, product_view, search, size_select, add_to_cart, remove_from_cart, checkout_start, purchase, wishlist_add, back_in_stock_signup, app_open, push_open.',
  session_id COMMENT 'Session id; joins to silver.sessions.',
  page_type COMMENT 'Page or app screen (home, category, product, search, checkout, pdp...).',
  sku COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105). Null for events without a product.',
  style_id COMMENT 'Product style id (e.g. RUN0123).',
  style_name COMMENT 'Product name (e.g. CloudStride 3 Men\'s).',
  category COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  sport COMMENT 'Sport of the product (Running, Trail Running, Basketball, Training, Lifestyle, Football, Multi-Sport).',
  gender COMMENT 'Product gender: Men, Women, Unisex or Kids.',
  colorway COMMENT 'Colorway name (e.g. Core Black / Cloud White).',
  size COMMENT 'Size of the SKU.',
  selected_size COMMENT 'Size selected on the product page.',
  is_limited_drop COMMENT 'Product is a limited collab drop.',
  current_price COMMENT 'Product price today (USD), not at the time of the event.',
  is_on_markdown COMMENT 'Product is on markdown or promo today.',
  stock_status COMMENT 'Product stock today: in_stock, low_stock, out_of_stock or discontinued.',
  search_term COMMENT 'Search query, for search events.',
  utm_source COMMENT 'UTM source of the visit (google, meta, tiktok, youtube, email, sms, braze, affiliate, referral); null = direct.',
  utm_campaign COMMENT 'UTM campaign (campaign_id for owned/paid campaigns, promo code for affiliates).',
  device_type COMMENT 'Device: mobile/desktop/tablet on web, ios/android in the app.'
)
COMMENT 'Customer Lake event table (view): identified web/app events (event id event_id, timestamp event_ts) with product name, category, sport, gender, size, limited-drop flag and today\'s price, markdown and stock. Use it to filter browsing and cart activity by product. View over gold digital_events + product_catalog_enriched; created by notebook ddl/04_cdp (schema retail_cdp).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT de.event_id, de.unified_customer_id, de.event_ts, de.platform, de.event_type, de.session_id, de.page_type,
       de.sku, coalesce(pc.style_id, de.style_id) AS style_id, pc.style_name, coalesce(pc.category, de.category) AS category,
       pc.sport, pc.gender, pc.colorway, pc.size, de.selected_size, pc.is_limited_drop, pc.current_price, pc.is_on_markdown,
       pc.stock_status, de.search_term, de.utm_source, de.utm_campaign, de.device_type
FROM ${catalog}.${gold}.digital_events de
LEFT JOIN ${catalog}.${gold}.product_catalog_enriched pc USING (sku)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 13 - `return_item_events` - Customer Lake event table (view): returns with product and original order details
# MAGIC Created once (`IF NOT EXISTS`) so the view keeps its identity for Customer Lake. To change it, drop it first.
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.return_item_events (
  return_id COMMENT 'Event id: return id.',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  return_ts COMMENT 'Event timestamp: when the return was requested/processed.',
  order_id COMMENT 'Original order; joins to purchase_events.order_id.',
  order_channel COMMENT 'Channel of the original order: web, app or store.',
  sku COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105).',
  style_name COMMENT 'Product name (e.g. CloudStride 3 Men\'s).',
  category COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  sport COMMENT 'Sport of the product (Running, Trail Running, Basketball, Training, Lifestyle, Football, Multi-Sport).',
  gender COMMENT 'Product gender: Men, Women, Unisex or Kids.',
  colorway COMMENT 'Colorway name.',
  size COMMENT 'Size returned.',
  return_reason COMMENT 'too_small, too_big, comfort, defect, not_as_pictured, changed_mind, other.',
  is_fit_related COMMENT 'Reason is too_small or too_big.',
  return_type COMMENT 'refund or exchange.',
  return_channel COMMENT 'mail or store.',
  quantity COMMENT 'Units returned.',
  refund_usd COMMENT 'Refunded amount (USD; fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).'
)
COMMENT 'Customer Lake event table (view): identified returns (event id return_id, timestamp return_ts) with product name, category, sport, gender, colorway and size, the original order channel and the refund in USD. View over gold return_events + product_catalog_enriched + purchase_events; created by notebook ddl/04_cdp (schema retail_cdp).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
WITH fx AS (SELECT * FROM VALUES ('USD', 1.0), ('CAD', 1.35), ('GBP', 0.80), ('EUR', 0.93) AS t(currency, rate))
SELECT re.return_id, re.unified_customer_id, re.return_ts, re.order_id, pe.channel AS order_channel, re.sku,
       re.style_name, re.category, re.sport, pc.gender, pc.colorway, pc.size, re.return_reason, re.is_fit_related,
       re.return_type, re.return_channel, re.quantity, CAST(re.refund_amount / fx.rate AS DECIMAL(12, 2)) AS refund_usd
FROM ${catalog}.${gold}.return_events re
LEFT JOIN ${catalog}.${gold}.product_catalog_enriched pc USING (sku)
LEFT JOIN ${catalog}.${gold}.purchase_events pe USING (order_id)
LEFT JOIN fx USING (currency)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 14 - `run_shoe_events` - Customer Lake event table (view): run-app activities with the shoe model used
# MAGIC Created once (`IF NOT EXISTS`) so the view keeps its identity for Customer Lake. To change it, drop it first.
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.run_shoe_events (
  activity_id COMMENT 'Event id: unique id of the run-app activity.',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  start_ts COMMENT 'Event timestamp: activity start.',
  activity_type COMMENT 'run, trail_run, treadmill_run or walk.',
  distance_km COMMENT 'Distance in km.',
  distance_miles COMMENT 'Distance in miles.',
  duration_seconds COMMENT 'Moving time in seconds.',
  avg_pace_sec_per_km COMMENT 'Average pace (seconds per km).',
  elevation_gain_m COMMENT 'Elevation gain in metres.',
  shoe_registration_id COMMENT 'Registered shoe used (joins to silver.shoe_mileage).',
  shoe_sku COMMENT 'SKU of the shoe used, when a StrideForge shoe.',
  shoe_name COMMENT 'Model of the shoe used (e.g. CloudStride 2 Men\'s).',
  shoe_category COMMENT 'Category of the shoe used (Running, Trail ...).',
  shoe_gender COMMENT 'Gender of the shoe model.',
  shoe_size COMMENT 'Size of the shoe used.',
  shoe_total_miles COMMENT 'Total miles on that shoe to date (replacement is due around 400-500).'
)
COMMENT 'Customer Lake event table (view): run-app activities (event id activity_id, timestamp start_ts) with the model, category and size of the shoe used and its total miles. Use it to filter runs by shoe model, e.g. for replacement campaigns. View over gold run_activity_events + product_catalog_enriched; created by notebook ddl/04_cdp (schema retail_cdp).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT ra.activity_id, ra.unified_customer_id, ra.start_ts, ra.activity_type, ra.distance_km, ra.distance_miles,
       ra.duration_seconds, ra.avg_pace_sec_per_km, ra.elevation_gain_m, ra.shoe_registration_id, ra.sku AS shoe_sku,
       pc.style_name AS shoe_name, pc.category AS shoe_category, pc.gender AS shoe_gender, pc.size AS shoe_size,
       ra.shoe_total_miles
FROM ${catalog}.${gold}.run_activity_events ra
LEFT JOIN ${catalog}.${gold}.product_catalog_enriched pc ON pc.sku = ra.sku
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 15 - `customer_top_recommendation` - Customer Lake 1:1 (view): each customer's #1 recommended product with name, image and link
# MAGIC Created once (`IF NOT EXISTS`) so the view keeps its identity for Customer Lake. To change it, drop it first.
# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.customer_top_recommendation (
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile. One row per customer with a recommendation.',
  rec_sku COMMENT 'Recommended SKU in the customer\'s size.',
  rec_style_id COMMENT 'Recommended product style id.',
  rec_product_name COMMENT 'Recommended product name (e.g. CloudStride 3 Women\'s).',
  rec_category COMMENT 'Category of the recommended product.',
  rec_sport COMMENT 'Sport of the recommended product.',
  rec_current_price COMMENT 'Price of the recommended product today (USD).',
  rec_msrp COMMENT 'List price of the recommended product (USD).',
  rec_is_on_markdown COMMENT 'Recommended product is on markdown or promo today.',
  rec_stock_status COMMENT 'Stock of the recommended product today: in_stock, low_stock or out_of_stock.',
  rec_image_url COMMENT 'Image URL of the recommended product (map it to an email or push template field).',
  rec_product_url COMMENT 'Product page URL of the recommended SKU (map it to an email or push template field).',
  rec_type COMMENT 'Why it is recommended: replenish, back_in_stock, new_arrival or cross_sell.',
  rec_reason COMMENT 'Human-readable reason (e.g. Top seller in Running in your size).',
  rec_score COMMENT 'Recommendation score (higher is better).',
  rec_generated_at COMMENT 'When the recommendation was generated.'
)
COMMENT 'Customer Lake 1:1 table (view): one row per customer with their #1 product recommendation - product name, category, sport, price, markdown, stock, image URL and product URL. Join 1:1 on unified_customer_id; its columns can be bulk-created as attributes and mapped to sync fields for personalised messages. View over gold customer_product_recommendations (rank 1) + product_catalog_enriched; created by notebook ddl/04_cdp (schema retail_cdp).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT r.unified_customer_id, r.sku AS rec_sku, r.style_id AS rec_style_id, pc.style_name AS rec_product_name,
       pc.category AS rec_category, pc.sport AS rec_sport, pc.current_price AS rec_current_price, pc.msrp AS rec_msrp,
       pc.is_on_markdown AS rec_is_on_markdown, pc.stock_status AS rec_stock_status, pc.image_url AS rec_image_url,
       pc.product_url AS rec_product_url, r.recommendation_type AS rec_type, r.reason AS rec_reason, r.score AS rec_score,
       r.generated_at AS rec_generated_at
FROM ${catalog}.${gold}.customer_product_recommendations r
LEFT JOIN ${catalog}.${gold}.product_catalog_enriched pc USING (sku)
WHERE r.rank = 1
""")
