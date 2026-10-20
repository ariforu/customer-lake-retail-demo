# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 05 - Customer Lake tables
# MAGIC
# MAGIC **What it does:** Publishes the household table and nine customer event tables that Customer Lake entities point at, then checks Customer Lake's rules (unique event id, TIMESTAMP column).
# MAGIC
# MAGIC - **Reads:** gold Customer 360 tables; silver orders, returns, events, messages, run, loyalty, race, support and review tables
# MAGIC - **Writes:** `household_profile` and `*_events` tables/views
# MAGIC - **Runs in:** `strideforge_medallion` -> `gold_customer_lake`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Household entity profile (one row per household)
# COMMAND ----------

run(r"""
CREATE OR REPLACE TABLE ${catalog}.${gold}.household_profile
COMMENT 'Customer Lake: Household profile - one row per household with member roll-ups (members, kids, contactability, revenue, CLV, churn, top sport, best tier). Joined many:1 from customer_profile on household_id. Derived from gold customer_profile, customer_traits, customer_predictions and silver households. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
WITH m AS (
  SELECT p.household_id, p.unified_customer_id, p.city, p.state, p.postal_code, p.country, p.home_store_id,
         p.email_contactable, p.sms_contactable, p.paid_media_eligible, p.loyalty_tier,
         t.revenue_12m, t.top_sport, t.lifecycle_stage, t.last_order_date, pr.predicted_clv_12m, pr.churn_probability
  FROM ${catalog}.${gold}.customer_profile p
  JOIN ${catalog}.${gold}.customer_traits t USING (unified_customer_id)
  JOIN ${catalog}.${gold}.customer_predictions pr USING (unified_customer_id)
  WHERE p.household_id IS NOT NULL)
SELECT m.household_id,
       first(h.address_id) AS address_id, mode(m.city) AS city, mode(m.state) AS state, mode(m.postal_code) AS postal_code,
       mode(m.country) AS country, mode(m.home_store_id) AS home_store_id,
       CAST(count(*) AS INT) AS member_count, first(h.kids_count) AS kids_count, first(h.kids_age_bands) AS kids_age_bands,
       first(h.kids_count) > 0 AS has_kids,
       CAST(count_if(m.email_contactable) AS INT) AS email_contactable_members, max(m.email_contactable) AS any_email_contactable,
       max(m.sms_contactable) AS any_sms_contactable, max(m.paid_media_eligible) AS any_paid_media_eligible,
       CAST(sum(m.revenue_12m) AS DECIMAL(14, 2)) AS household_revenue_12m,
       CAST(sum(m.predicted_clv_12m) AS DECIMAL(14, 2)) AS household_predicted_clv_12m,
       round(min(m.churn_probability), 4) AS min_member_churn_probability,
       max(m.last_order_date) AS last_order_date, mode(m.top_sport) AS top_sport,
       max(CASE m.loyalty_tier WHEN 'Elite' THEN 3 WHEN 'Pro' THEN 2 WHEN 'Rookie' THEN 1 ELSE 0 END) AS best_loyalty_tier_rank,
       current_timestamp() AS updated_at
FROM m LEFT JOIN ${catalog}.${silver}.households h USING (household_id)
GROUP BY m.household_id
""")
# COMMAND ----------

run(r"""
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN household_id SET NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC Column descriptions for `household_profile` (CTAS does not carry column comments)
# COMMAND ----------

for _stmt in r"""
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN household_id COMMENT 'Household id (HH + hash of standardized primary street + postal code); joins to household_profile.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN address_id COMMENT 'Primary address id of the household.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN city COMMENT 'City of the primary address.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN state COMMENT 'State / province / region of the primary address.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN postal_code COMMENT 'Postal code of the primary address (US/DE 5-digit, UK and CA alphanumeric).';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN country COMMENT 'Country code: US, UK, DE, CA.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN home_store_id COMMENT 'Store the customer shops most (POS orders), else the store where they registered; joins to silver.stores.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN member_count COMMENT 'Customers in the household.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN kids_count COMMENT 'Distinct kids age bands observed (proxy for number of kids).';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN kids_age_bands COMMENT 'Kids age bands inferred from kids-size purchases in the household (0-5, 6-9, 10-14, 6-14).';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN has_kids COMMENT 'Household bought kids-size products.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN email_contactable_members COMMENT 'Members contactable by email.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN any_email_contactable COMMENT 'At least one member is contactable by email.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN any_sms_contactable COMMENT 'At least one member is contactable by SMS.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN any_paid_media_eligible COMMENT 'At least one member can be targeted on ad platforms.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN household_revenue_12m COMMENT 'Sum of members\' revenue_12m (USD).';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN household_predicted_clv_12m COMMENT 'Sum of members\' predicted_clv_12m (USD).';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN min_member_churn_probability COMMENT 'Lowest churn probability among members (most engaged member).';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN last_order_date COMMENT 'Most recent order by any member.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN top_sport COMMENT 'Most common top sport among members.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN best_loyalty_tier_rank COMMENT 'Best member tier: 3 Elite, 2 Pro, 1 Rookie, 0 none.';
ALTER TABLE ${catalog}.${gold}.household_profile ALTER COLUMN updated_at COMMENT 'When the job last rebuilt this row.';
""".strip().split(";\n"):
    run(_stmt.rstrip(";"))
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Purchase events (order level, enriched with what was bought)
# COMMAND ----------

run(r"""
CREATE OR REPLACE TABLE ${catalog}.${gold}.purchase_events
COMMENT 'Customer Lake event table: identified orders (event id order_id, timestamp order_ts) with USD revenue, channel, promo, UTMs and what was bought (categories, sports, styles, running shoes, CloudStride 3, limited drop). Derived from silver orders + order_lines + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
WITH fx AS (SELECT * FROM VALUES ('USD', 1.0), ('CAD', 1.35), ('GBP', 0.80), ('EUR', 0.93) AS t(currency, rate)),
l AS (
  SELECT l.order_id, collect_set(l.category) AS categories, collect_set(p.sport) AS sports,
         collect_set(p.style_name) AS style_names, max(p.is_limited_drop) AS has_limited_drop,
         max(p.style_name LIKE 'CloudStride 3%') AS has_cloudstride_3,
         max(l.category IN ('Running', 'Trail')) AS has_running_shoes, sum(l.quantity) AS units
  FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.products p ON p.style_id = l.style_id
  WHERE l.unified_customer_id IS NOT NULL GROUP BY 1)
SELECT o.order_id, o.unified_customer_id, o.order_ts, o.channel, o.store_id,
       CASE o.currency WHEN 'USD' THEN 'US' WHEN 'CAD' THEN 'CA' WHEN 'GBP' THEN 'UK' WHEN 'EUR' THEN 'DE' END AS country, o.currency,
       CAST((o.subtotal_amount - o.discount_amount) / fx.rate AS DECIMAL(14, 2)) AS revenue_usd, o.total_amount,
       o.discount_amount, o.is_full_price, o.promo_code, o.fulfillment_type, o.payment_type,
       o.utm_source, o.utm_medium, o.utm_campaign, CAST(l.units AS INT) AS units, l.categories, l.sports, l.style_names,
       l.has_running_shoes, l.has_cloudstride_3, l.has_limited_drop
FROM ${catalog}.${silver}.orders o
JOIN fx USING (currency) JOIN l USING (order_id)
WHERE o.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC Column descriptions for `purchase_events` (CTAS does not carry column comments)
# COMMAND ----------

for _stmt in r"""
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN order_id COMMENT 'Event id: order id.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN order_ts COMMENT 'Event timestamp: when the order was placed.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN channel COMMENT 'Channel of the record (web/app/store for commerce; email/sms/push for messaging; platform for paid).';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN store_id COMMENT 'Store (POS) or pickup store (BOPIS).';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN country COMMENT 'Order country derived from currency.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN currency COMMENT 'Order currency (USD, CAD, GBP, EUR).';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN revenue_usd COMMENT 'Net merchandise revenue (subtotal - discount) (USD; local currencies converted at fixed demo FX: CAD 1.35, GBP 0.80, EUR 0.93).';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN total_amount COMMENT 'Order total incl. tax and shipping, local currency.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN discount_amount COMMENT 'Order-level promo discount, local currency.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN is_full_price COMMENT 'No markdown lines and no promo discount.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN promo_code COMMENT 'Promo / affiliate code used.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN fulfillment_type COMMENT 'ship, bopis, ship_from_store or in_store.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN payment_type COMMENT 'Payment method.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN utm_source COMMENT 'UTM source of the visit/order (google, meta, tiktok, youtube, email, sms, braze, affiliate, referral); null = direct.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN utm_medium COMMENT 'UTM medium (cpc, paid_social, paid_video, email, sms, push, organic, affiliate, referral).';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN utm_campaign COMMENT 'UTM campaign (campaign_id for owned/paid campaigns, promo code for affiliates).';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN units COMMENT 'Units in the order.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN categories COMMENT 'Distinct product categories in the order.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN sports COMMENT 'Distinct sports in the order.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN style_names COMMENT 'Distinct style names in the order.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN has_running_shoes COMMENT 'Order contains running or trail shoes.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN has_cloudstride_3 COMMENT 'Order contains a CloudStride 3.';
ALTER TABLE ${catalog}.${gold}.purchase_events ALTER COLUMN has_limited_drop COMMENT 'Order contains a limited-drop style.';
""".strip().split(";\n"):
    run(_stmt.rstrip(";"))
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Return events
# COMMAND ----------

run(r"""
CREATE OR REPLACE TABLE ${catalog}.${gold}.return_events
COMMENT 'Customer Lake event table: identified returns (event id return_id, timestamp return_ts) with style, category, reason and fit flag. Derived from silver returns + skus + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT r.return_id, r.unified_customer_id, r.return_ts, r.order_id, r.sku, k.style_id, p.style_name, p.category, p.sport,
       r.return_reason, r.is_fit_related, r.return_type, r.return_channel, r.refund_amount, r.quantity
FROM ${catalog}.${silver}.returns r
LEFT JOIN ${catalog}.${silver}.skus k USING (sku)
LEFT JOIN ${catalog}.${silver}.products p ON p.style_id = k.style_id
WHERE r.unified_customer_id IS NOT NULL AND r.return_ts IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC Column descriptions for `return_events` (CTAS does not carry column comments)
# COMMAND ----------

for _stmt in r"""
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN return_id COMMENT 'Event id: return id.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN return_ts COMMENT 'Event timestamp: when the return was requested/processed.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN order_id COMMENT 'Original order.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN sku COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN style_id COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN style_name COMMENT 'Product style name (e.g. CloudStride 3 Men\'s).';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN category COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN sport COMMENT 'Sport of the product (Running, Trail Running, Basketball, Training, Lifestyle, Football, Multi-Sport).';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN return_reason COMMENT 'too_small, too_big, comfort, defect, not_as_pictured, changed_mind, other.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN is_fit_related COMMENT 'Reason is too_small or too_big.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN return_type COMMENT 'refund or exchange.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN return_channel COMMENT 'mail or store.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN refund_amount COMMENT 'Refunded amount, local currency.';
ALTER TABLE ${catalog}.${gold}.return_events ALTER COLUMN quantity COMMENT 'Units returned.';
""".strip().split(";\n"):
    run(_stmt.rstrip(";"))
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Digital (web + app) events
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.digital_events (
  event_id COMMENT 'Unique id of the web/app event (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  event_ts COMMENT 'Event timestamp.',
  platform COMMENT 'web, ios or android.',
  event_type COMMENT 'page_view, product_view, search, size_select, add_to_cart, remove_from_cart, checkout_start, purchase, wishlist_add, back_in_stock_signup, app_open, push_open.',
  session_id COMMENT 'Session id; joins to silver.sessions.',
  page_type COMMENT 'Page or app screen (home, category, product, search, checkout, pdp...).',
  sku COMMENT 'SKU = style_id-colorway-size (e.g. RUN0123-01-105 is colorway 01, US size 10.5).',
  style_id COMMENT 'Product style id (e.g. RUN0123); joins to silver.products / product_catalog_enriched.style_id.',
  category COMMENT 'Product category (Running, Trail, Basketball, Training, Lifestyle, Football, Kids Footwear, Apparel, Kids Apparel, Accessories).',
  search_term COMMENT 'Search query, for search events.',
  selected_size COMMENT 'Size selected on the product page.',
  utm_source COMMENT 'UTM source of the visit/order (google, meta, tiktok, youtube, email, sms, braze, affiliate, referral); null = direct.',
  utm_medium COMMENT 'UTM medium (cpc, paid_social, paid_video, email, sms, push, organic, affiliate, referral).',
  utm_campaign COMMENT 'UTM campaign (campaign_id for owned/paid campaigns, promo code for affiliates).',
  device_type COMMENT 'Device: mobile/desktop/tablet on web, ios/android in the app.'
)
COMMENT 'Customer Lake event table (view): identified web/app events (event id event_id, timestamp event_ts). View over silver web_app_events where unified_customer_id is known. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT event_id, unified_customer_id, event_ts, platform, event_type, session_id, page_type, sku, style_id, category,
       search_term, selected_size, utm_source, utm_medium, utm_campaign, device_type
FROM ${catalog}.${silver}.web_app_events WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Message engagement events (email / SMS / push sends, opens, clicks, bounces, unsubscribes)
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.message_engagement_events (
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
COMMENT 'Customer Lake event table (view): email/SMS/push sends and engagement (event id message_event_id, timestamp event_ts) with campaign name and objective. View over silver message_events + campaigns. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT m.message_event_id, m.unified_customer_id, m.event_ts, m.channel, m.event_type, m.campaign_id, c.campaign_name,
       c.objective AS campaign_objective, c.campaign_type, m.variant, m.link_url, m.device_type
FROM ${catalog}.${silver}.message_events m
LEFT JOIN ${catalog}.${silver}.campaigns c USING (campaign_id)
WHERE m.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Run-app activity, loyalty, race registration, support and review events
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.run_activity_events (
  activity_id COMMENT 'Unique id of the run-app activity (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  start_ts COMMENT 'Event timestamp: activity start.',
  activity_type COMMENT 'run, trail_run, treadmill_run or walk.',
  distance_km COMMENT 'Distance in km.',
  distance_miles COMMENT 'Distance in miles.',
  duration_seconds COMMENT 'Moving time in seconds.',
  avg_pace_sec_per_km COMMENT 'Average pace (seconds per km).',
  elevation_gain_m COMMENT 'Elevation gain in metres.',
  shoe_registration_id COMMENT 'Registered shoe used (joins to silver.shoe_mileage).',
  sku COMMENT 'SKU of the shoe used, when a StrideForge shoe.',
  shoe_total_miles COMMENT 'Total miles on that shoe to date.'
)
COMMENT 'Customer Lake event table (view): run-app activities (event id activity_id, timestamp start_ts) with shoe and its total mileage. View over silver fitness_activities + shoe_mileage. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT a.activity_id, a.unified_customer_id, a.start_ts, a.activity_type, a.distance_km, a.distance_miles, a.duration_seconds,
       a.avg_pace_sec_per_km, a.elevation_gain_m, a.shoe_registration_id, a.sku, m.total_miles AS shoe_total_miles
FROM ${catalog}.${silver}.fitness_activities a
LEFT JOIN ${catalog}.${silver}.shoe_mileage m USING (shoe_registration_id)
WHERE a.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.loyalty_events (
  ledger_id COMMENT 'Unique id of the loyalty ledger entry (Customer Lake event ID).',
  unified_customer_id COMMENT 'Unified customer id assigned by identity resolution (silver.identity_graph); joins to customer_profile.',
  activity_ts COMMENT 'Event timestamp.',
  loyalty_id COMMENT 'Loyalty membership id.',
  activity_type COMMENT 'earn, redeem, adjust (return), bonus, expire, opening_balance or tier_change.',
  points COMMENT 'Points added (+) or removed (-).',
  reference_id COMMENT 'Related order id, bonus code or new tier.',
  reference_type COMMENT 'order, return, promotion, inactivity or tier.'
)
COMMENT 'Customer Lake event table (view): loyalty ledger activity (event id ledger_id, timestamp activity_ts). View over silver loyalty_transactions. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT ledger_id, unified_customer_id, activity_ts, loyalty_id, activity_type, points, reference_id, reference_type
FROM ${catalog}.${silver}.loyalty_transactions WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.race_registration_events (
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
COMMENT 'Customer Lake event table (view): race registrations (event id registration_id, timestamp registered_at) with race date, type and distance. View over silver event_registrations. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT registration_id, unified_customer_id, registered_at, event_id, event_name, event_type, event_date, event_city,
       distance_km, registration_source
FROM ${catalog}.${silver}.event_registrations WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.support_events (
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
COMMENT 'Customer Lake event table (view): service tickets (event id ticket_id, timestamp created_at) with category, status, sentiment. View over silver service_tickets. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT ticket_id, unified_customer_id, created_at, channel, category, priority, status, sentiment, resolution_hours, order_id
FROM ${catalog}.${silver}.service_tickets WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
CREATE OR REPLACE VIEW ${catalog}.${gold}.review_events (
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
COMMENT 'Customer Lake event table (view): product reviews (event id review_id, timestamp submitted_at) with rating and AI fit/sentiment. View over silver product_reviews + products. Built by job strideforge_medallion, task gold_customer_lake (notebook gold/05_customer_lake).'
AS
SELECT r.review_id, r.unified_customer_id, r.submitted_at, r.sku, r.style_id, p.style_name, p.category, r.rating,
       r.fit_rating, r.ai_sentiment, r.ai_fit_feedback, r.ai_themes, r.verified_purchase
FROM ${catalog}.${silver}.product_reviews r
LEFT JOIN ${catalog}.${silver}.products p USING (style_id)
WHERE r.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Validate Customer Lake requirements
# COMMAND ----------

G = f"{P['catalog']}.{P['gold']}"
EVENTS = {"purchase_events": ("order_id", "order_ts"), "return_events": ("return_id", "return_ts"),
          "digital_events": ("event_id", "event_ts"), "message_engagement_events": ("message_event_id", "event_ts"),
          "run_activity_events": ("activity_id", "start_ts"), "loyalty_events": ("ledger_id", "activity_ts"),
          "race_registration_events": ("registration_id", "registered_at"), "support_events": ("ticket_id", "created_at"),
          "review_events": ("review_id", "submitted_at")}
problems = []
for t, (eid, ts) in EVENTS.items():
    df = spark.table(f"{G}.{t}")
    ts_type = dict(df.dtypes)[ts]
    r = df.selectExpr("count(*) AS n", f"count(DISTINCT {eid}) AS d", "count(unified_customer_id) AS k").first()
    ok = r.n == r.d and r.k == r.n and ts_type in ("timestamp", "timestamp_ntz")
    print(f"{t:28s} rows={r.n:>12,d} distinct_ids={r.d:>12,d} ts={ts_type:10s} {'OK' if ok else 'PROBLEM'}")
    if not ok:
        problems.append(t)
hp = spark.table(f"{G}.household_profile").selectExpr("count(*) n", "count(DISTINCT household_id) d").first()
print(f"{'household_profile':28s} rows={hp.n:>12,d} distinct_ids={hp.d:>12,d}")
if hp.n != hp.d:
    problems.append("household_profile")
assert not problems, f"Customer Lake requirement violations: {problems}"
