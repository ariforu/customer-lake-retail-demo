# Databricks notebook source
# MAGIC %md
# MAGIC # Create bronze tables
# MAGIC
# MAGIC **What it does:** Creates the bronze schema and its 42 empty raw tables (one per source feed).
# MAGIC
# MAGIC - **Reads:** -
# MAGIC - **Writes:** `retail_bronze` schema and tables
# MAGIC - **Runs in:** `strideforge_bronze_setup` -> `ddl_bronze`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Create the bronze schema
# COMMAND ----------

run(r"""
CREATE SCHEMA IF NOT EXISTS ${catalog}.${bronze}
COMMENT 'StrideForge CDP demo - raw data landed from source systems (append-only)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - `pos_customers_raw` - POS customer registrations captured at store checkout
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.pos_customers_raw (
  pos_customer_id STRING, store_id STRING, first_name STRING, last_name STRING,
  email STRING, phone STRING, address_line1 STRING, city STRING, state STRING,
  postal_code STRING, country STRING, birth_date STRING, loyalty_card_number STRING,
  created_at STRING, updated_at STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'POS customer registrations captured at store checkout'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - `pos_transactions_raw` - POS transaction headers
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.pos_transactions_raw (
  transaction_id STRING, store_id STRING, register_id STRING, associate_id STRING,
  pos_customer_id STRING, loyalty_card_number STRING, transaction_ts STRING,
  subtotal_amount DECIMAL(12,2), discount_amount DECIMAL(12,2), tax_amount DECIMAL(12,2),
  total_amount DECIMAL(12,2), currency STRING, payment_type STRING, promo_code STRING,
  transaction_type STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'POS transaction headers'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - `pos_transaction_lines_raw` - POS transaction line items
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.pos_transaction_lines_raw (
  transaction_id STRING, line_number INT, sku STRING, quantity INT,
  unit_price DECIMAL(12,2), discount_amount DECIMAL(12,2), line_total DECIMAL(12,2),
  is_markdown BOOLEAN,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'POS transaction line items'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - `pos_returns_raw` - In-store returns and exchanges
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.pos_returns_raw (
  return_id STRING, original_transaction_id STRING, store_id STRING, sku STRING,
  quantity INT, refund_amount DECIMAL(12,2), return_reason_code STRING,
  return_reason_text STRING, return_type STRING, return_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'In-store returns and exchanges'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - `ecom_customers_raw` - E-commerce platform customer accounts
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.ecom_customers_raw (
  ecom_customer_id STRING, email STRING, first_name STRING, last_name STRING,
  phone STRING, gender STRING, birth_date STRING, default_address STRING,
  accepts_marketing BOOLEAN, account_status STRING, created_at STRING, updated_at STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'E-commerce platform customer accounts'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - `ecom_orders_raw` - E-commerce order headers (web + app checkout)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.ecom_orders_raw (
  order_id STRING, ecom_customer_id STRING, guest_email STRING, order_ts STRING,
  order_status STRING, subtotal_amount DECIMAL(12,2), discount_amount DECIMAL(12,2),
  shipping_amount DECIMAL(12,2), tax_amount DECIMAL(12,2), total_amount DECIMAL(12,2),
  currency STRING, payment_type STRING, promo_code STRING, fulfillment_type STRING,
  pickup_store_id STRING, shipping_address STRING, device_type STRING,
  utm_source STRING, utm_medium STRING, utm_campaign STRING, session_id STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'E-commerce order headers (web + app checkout)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - `ecom_order_lines_raw` - E-commerce order line items
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.ecom_order_lines_raw (
  order_id STRING, line_number INT, sku STRING, quantity INT,
  unit_price DECIMAL(12,2), discount_amount DECIMAL(12,2), line_total DECIMAL(12,2),
  is_markdown BOOLEAN,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'E-commerce order line items'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - `ecom_returns_raw` - Online returns (mail-in / return-to-store)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.ecom_returns_raw (
  return_id STRING, order_id STRING, line_number INT, sku STRING, quantity INT,
  refund_amount DECIMAL(12,2), return_reason_code STRING, return_reason_text STRING,
  return_type STRING, return_channel STRING, requested_ts STRING, received_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Online returns (mail-in / return-to-store)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10 - `ecom_carts_raw` - Cart add/remove events including abandoned carts
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.ecom_carts_raw (
  cart_id STRING, session_id STRING, ecom_customer_id STRING, cookie_id STRING,
  sku STRING, quantity INT, unit_price DECIMAL(12,2), action STRING, event_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Cart add/remove events including abandoned carts'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 11 - `app_users_raw` - Mobile app user registrations
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.app_users_raw (
  app_user_id STRING, device_id STRING, email STRING, phone STRING,
  first_name STRING, last_name STRING, platform STRING, app_version STRING,
  install_ts STRING, last_open_ts STRING, push_enabled BOOLEAN, location_enabled BOOLEAN,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Mobile app user registrations'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 12 - `app_events_raw` - Mobile app behavioural events (properties as raw JSON string)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.app_events_raw (
  event_id STRING, app_user_id STRING, device_id STRING, session_id STRING,
  event_name STRING, event_ts STRING, screen_name STRING, sku STRING,
  properties STRING, platform STRING, geo_lat DOUBLE, geo_lon DOUBLE,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Mobile app behavioural events (properties as raw JSON string)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 13 - `app_push_tokens_raw` - Push notification tokens by device
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.app_push_tokens_raw (
  device_id STRING, app_user_id STRING, push_token STRING, platform STRING,
  token_status STRING, updated_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Push notification tokens by device'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 14 - `web_clickstream_raw` - Website clickstream events (page_view, product_view, search, size_select, add_to_cart, wishlist_add, checkout_start, purchase)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.web_clickstream_raw (
  event_id STRING, cookie_id STRING, session_id STRING, ecom_customer_id STRING,
  event_type STRING, event_ts STRING, page_url STRING, page_type STRING,
  referrer_url STRING, sku STRING, search_term STRING, selected_size STRING,
  utm_source STRING, utm_medium STRING, utm_campaign STRING, utm_content STRING,
  device_type STRING, browser STRING, ip_geo_city STRING, ip_geo_region STRING,
  ip_geo_postal_code STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Website clickstream events (page_view, product_view, search, size_select, add_to_cart, wishlist_add, checkout_start, purchase)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 15 - `web_sessions_raw` - Website sessions
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.web_sessions_raw (
  session_id STRING, cookie_id STRING, ecom_customer_id STRING,
  session_start_ts STRING, session_end_ts STRING, landing_page STRING,
  exit_page STRING, page_views INT, device_type STRING, traffic_source STRING,
  utm_campaign STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Website sessions'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 16 - `loyalty_members_raw` - Loyalty program members (tiers: Rookie / Pro / Elite)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.loyalty_members_raw (
  loyalty_id STRING, loyalty_card_number STRING, email STRING, phone STRING,
  first_name STRING, last_name STRING, birth_date STRING, postal_code STRING,
  tier STRING, points_balance INT, enrolled_ts STRING, enrollment_channel STRING,
  status STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Loyalty program members (tiers: Rookie / Pro / Elite)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 17 - `loyalty_points_ledger_raw` - Loyalty points earn / redeem / expire / tier-change ledger
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.loyalty_points_ledger_raw (
  ledger_id STRING, loyalty_id STRING, activity_type STRING, points INT,
  reference_id STRING, reference_type STRING, activity_ts STRING, expiry_date STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Loyalty points earn / redeem / expire / tier-change ledger'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 18 - `fitness_activities_raw` - Activities logged in StrideForge Run app
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.fitness_activities_raw (
  activity_id STRING, fitness_user_id STRING, email STRING, activity_type STRING,
  start_ts STRING, duration_seconds INT, distance_km DOUBLE, elevation_gain_m DOUBLE,
  avg_pace_sec_per_km DOUBLE, avg_heart_rate INT, shoe_registration_id STRING,
  start_lat DOUBLE, start_lon DOUBLE,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Activities logged in StrideForge Run app'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 19 - `fitness_shoe_registrations_raw` - Shoes registered by users in the run app for mileage tracking
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.fitness_shoe_registrations_raw (
  shoe_registration_id STRING, fitness_user_id STRING, sku STRING, style_name STRING,
  nickname STRING, registered_ts STRING, retired_ts STRING, order_reference STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Shoes registered by users in the run app for mileage tracking'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 20 - `event_registrations_raw` - Race / event registrations (marathons, 5Ks, store events)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.event_registrations_raw (
  registration_id STRING, event_id STRING, event_name STRING, event_type STRING,
  event_date STRING, event_city STRING, distance_km DOUBLE, email STRING,
  first_name STRING, last_name STRING, registered_ts STRING, registration_source STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Race / event registrations (marathons, 5Ks, store events)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 21 - `run_club_checkins_raw` - Store-hosted run club check-ins
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.run_club_checkins_raw (
  checkin_id STRING, store_id STRING, club_session_id STRING, loyalty_id STRING,
  email STRING, checkin_ts STRING, run_distance_km DOUBLE,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Store-hosted run club check-ins'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 22 - `esp_sends_raw` - Email / SMS / push sends from the ESP
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.esp_sends_raw (
  send_id STRING, campaign_id STRING, campaign_name STRING, channel STRING,
  recipient_email STRING, recipient_phone STRING, push_token STRING,
  template_id STRING, variant STRING, send_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Email / SMS / push sends from the ESP'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 23 - `esp_engagement_raw` - Delivery / open / click / bounce / complaint events (CDP write-back lands here)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.esp_engagement_raw (
  engagement_id STRING, send_id STRING, campaign_id STRING, channel STRING,
  recipient_email STRING, recipient_phone STRING, event_type STRING, event_ts STRING,
  link_url STRING, device_type STRING, bounce_type STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Delivery / open / click / bounce / complaint events (CDP write-back lands here)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 24 - `esp_unsubscribes_raw` - Unsubscribe / STOP events
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.esp_unsubscribes_raw (
  unsubscribe_id STRING, channel STRING, recipient_email STRING, recipient_phone STRING,
  campaign_id STRING, reason STRING, unsubscribe_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Unsubscribe / STOP events'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 25 - `consent_events_raw` - Consent management platform events (GDPR / CCPA)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.consent_events_raw (
  consent_event_id STRING, subject_identifier STRING, identifier_type STRING,
  purpose STRING, channel STRING, status STRING, jurisdiction STRING,
  capture_source STRING, policy_version STRING, event_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Consent management platform events (GDPR / CCPA)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 26 - `paid_media_daily_raw` - Daily paid media performance (Meta, Google, TikTok, YouTube)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.paid_media_daily_raw (
  platform STRING, account_id STRING, campaign_id STRING, campaign_name STRING,
  ad_group_id STRING, ad_id STRING, audience_name STRING, report_date STRING,
  impressions BIGINT, clicks BIGINT, spend DECIMAL(14,2), video_views BIGINT,
  platform_conversions BIGINT, platform_conversion_value DECIMAL(14,2), currency STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Daily paid media performance (Meta, Google, TikTok, YouTube)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 27 - `paid_media_conversions_raw` - Platform-reported conversions (CDP write-back / CAPI)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.paid_media_conversions_raw (
  conversion_id STRING, platform STRING, campaign_id STRING, ad_id STRING,
  click_id STRING, hashed_email STRING, order_id STRING, conversion_ts STRING,
  conversion_value DECIMAL(12,2),
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Platform-reported conversions (CDP write-back / CAPI)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 28 - `affiliate_codes_raw` - Affiliate / athlete / creator promo codes
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.affiliate_codes_raw (
  promo_code STRING, partner_id STRING, partner_name STRING, partner_type STRING,
  sport STRING, discount_pct DOUBLE, valid_from STRING, valid_to STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Affiliate / athlete / creator promo codes'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 29 - `affiliate_redemptions_raw` - Promo code redemptions from affiliate network
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.affiliate_redemptions_raw (
  redemption_id STRING, promo_code STRING, order_id STRING, redemption_ts STRING,
  order_amount DECIMAL(12,2), commission_amount DECIMAL(12,2),
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Promo code redemptions from affiliate network'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 30 - `service_tickets_raw` - Customer service tickets
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.service_tickets_raw (
  ticket_id STRING, requester_email STRING, requester_phone STRING, channel STRING,
  subject STRING, description STRING, category STRING, priority STRING, status STRING,
  order_reference STRING, created_ts STRING, resolved_ts STRING, agent_id STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Customer service tickets'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 31 - `csat_surveys_raw` - Post-ticket CSAT surveys
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.csat_surveys_raw (
  survey_response_id STRING, ticket_id STRING, respondent_email STRING,
  csat_score INT, comment STRING, response_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Post-ticket CSAT surveys'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 32 - `nps_surveys_raw` - Relationship / transactional NPS surveys
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.nps_surveys_raw (
  survey_response_id STRING, respondent_email STRING, touchpoint STRING,
  nps_score INT, comment STRING, response_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Relationship / transactional NPS surveys'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 33 - `product_reviews_raw` - Product ratings and free-text reviews
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.product_reviews_raw (
  review_id STRING, sku STRING, style_id STRING, reviewer_email STRING,
  reviewer_nickname STRING, rating INT, title STRING, review_text STRING,
  fit_rating STRING, verified_purchase BOOLEAN, submitted_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Product ratings and free-text reviews'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 34 - `products_raw` - PIM product styles (Running, Trail, Basketball, Training, Lifestyle, Apparel, Accessories)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.products_raw (
  style_id STRING, style_name STRING, division STRING, category STRING,
  sub_category STRING, sport STRING, gender STRING, technology STRING,
  description STRING, msrp DECIMAL(10,2), unit_cost DECIMAL(10,2), launch_date STRING,
  lifecycle_status STRING, is_limited_drop BOOLEAN, collab_partner STRING,
  image_url STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'PIM product styles (Running, Trail, Basketball, Training, Lifestyle, Apparel, Accessories)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 35 - `skus_raw` - SKUs (style x colorway x size x width)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.skus_raw (
  sku STRING, style_id STRING, colorway STRING, color_family STRING,
  size STRING, size_system STRING, width STRING, upc STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'SKUs (style x colorway x size x width)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 36 - `product_drops_raw` - Limited-edition drop / launch calendar
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.product_drops_raw (
  drop_id STRING, style_id STRING, drop_name STRING, release_ts STRING,
  units_available INT, release_channel STRING, is_raffle BOOLEAN,
  member_early_access BOOLEAN,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Limited-edition drop / launch calendar'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 37 - `price_history_raw` - Price and markdown history
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.price_history_raw (
  sku STRING, price_type STRING, price DECIMAL(10,2), effective_from STRING,
  effective_to STRING, channel STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Price and markdown history'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 38 - `inventory_snapshots_raw` - Daily inventory snapshots by store / DC
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.inventory_snapshots_raw (
  snapshot_date STRING, location_id STRING, location_type STRING, sku STRING,
  on_hand_qty INT, on_order_qty INT, reserved_qty INT,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Daily inventory snapshots by store / DC'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 39 - `stores_raw` - Store master (flagship, standard, outlet, shop-in-shop)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.stores_raw (
  store_id STRING, store_name STRING, store_type STRING, address_line1 STRING,
  city STRING, state STRING, postal_code STRING, country STRING,
  latitude DOUBLE, longitude DOUBLE, region STRING, open_date STRING,
  sq_ft INT, has_gait_lab BOOLEAN, hosts_run_club BOOLEAN,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Store master (flagship, standard, outlet, shop-in-shop)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 40 - `gait_analysis_raw` - In-store 3D foot scan and gait analysis results
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.gait_analysis_raw (
  analysis_id STRING, store_id STRING, loyalty_id STRING, email STRING,
  analysis_ts STRING, foot_length_left_mm DOUBLE, foot_length_right_mm DOUBLE,
  foot_width_mm DOUBLE, arch_type STRING, pronation_type STRING,
  recommended_size STRING, recommended_width STRING, recommended_styles STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'In-store 3D foot scan and gait analysis results'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 41 - `weather_daily_by_zip_raw` - Daily weather actuals and forecast by postal code
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.weather_daily_by_zip_raw (
  postal_code STRING, weather_date STRING, temp_high_c DOUBLE, temp_low_c DOUBLE,
  precipitation_mm DOUBLE, snowfall_mm DOUBLE, condition STRING, is_forecast BOOLEAN,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Daily weather actuals and forecast by postal code'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 42 - `sports_calendar_raw` - School sport seasons, major races, retail moments (back-to-school, New Year)
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.sports_calendar_raw (
  calendar_event_id STRING, event_name STRING, event_category STRING, sport STRING,
  region STRING, start_date STRING, end_date STRING, description STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'School sport seasons, major races, retail moments (back-to-school, New Year)'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 43 - `zip_demographics_raw` - Census-style demographics by postal code
# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze}.zip_demographics_raw (
  postal_code STRING, city STRING, state STRING, population INT,
  median_household_income INT, median_age DOUBLE, pct_households_with_kids DOUBLE,
  urbanicity STRING,
  _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING
) COMMENT 'Census-style demographics by postal code'
""")
