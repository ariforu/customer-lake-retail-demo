-- StrideForge CDP demo - SILVER layer (cleansed, conformed, identity-resolved)

CREATE SCHEMA IF NOT EXISTS ${catalog}.${silver}
COMMENT 'StrideForge CDP demo - cleansed, standardized, identity-resolved entities';

-- ---------------- Identity ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.customer_source_records (
  source_record_key STRING NOT NULL COMMENT 'source_system || source_id',
  source_system STRING, source_id STRING,
  first_name STRING, last_name STRING, full_name_normalized STRING,
  email STRING, email_normalized STRING, email_sha256 STRING,
  phone_e164 STRING, phone_sha256 STRING,
  birth_date DATE, gender STRING,
  address_line1 STRING, city STRING, state STRING, postal_code STRING, country STRING,
  address_standardized STRING, loyalty_id STRING, device_id STRING, cookie_id STRING,
  created_at TIMESTAMP, updated_at TIMESTAMP,
  CONSTRAINT pk_customer_source_records PRIMARY KEY (source_record_key)
) COMMENT 'All source customer profiles in one standardized shape - input to identity resolution';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.identity_match_pairs (
  record_key_a STRING, record_key_b STRING, match_rule STRING,
  match_type STRING COMMENT 'deterministic | probabilistic',
  match_score DOUBLE, is_accepted BOOLEAN, evaluated_at TIMESTAMP
) COMMENT 'Candidate record pairs with match rule and score';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.identity_graph (
  unified_customer_id STRING NOT NULL, source_record_key STRING NOT NULL,
  source_system STRING, id_type STRING COMMENT 'email | phone | loyalty_id | device_id | cookie_id | pos_id | ecom_id | app_id | fitness_id',
  id_value STRING, is_primary BOOLEAN, first_seen TIMESTAMP, last_seen TIMESTAMP,
  resolved_at TIMESTAMP
) COMMENT 'Resolved identity graph: every identifier mapped to unified_customer_id';

-- ---------------- Customer ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.customers (
  unified_customer_id STRING NOT NULL,
  first_name STRING, last_name STRING, email STRING, phone_e164 STRING,
  birth_date DATE, age_band STRING, gender STRING, household_id STRING,
  primary_address_id STRING, home_store_id STRING, loyalty_id STRING,
  is_known BOOLEAN COMMENT 'false for anonymous (cookie/device only) profiles',
  source_record_count INT, first_seen_at TIMESTAMP, last_seen_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_customers PRIMARY KEY (unified_customer_id)
) COMMENT 'Golden customer record after survivorship rules';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.customer_addresses (
  address_id STRING NOT NULL, unified_customer_id STRING, address_type STRING,
  address_line1 STRING, city STRING, state STRING, postal_code STRING, country STRING,
  latitude DOUBLE, longitude DOUBLE, is_primary BOOLEAN, is_valid BOOLEAN,
  source_system STRING, updated_at TIMESTAMP
) COMMENT 'Standardized and geocoded customer addresses';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.households (
  household_id STRING NOT NULL, address_id STRING, member_count INT,
  adult_count INT, kids_count INT, kids_age_bands ARRAY<STRING>,
  updated_at TIMESTAMP
) COMMENT 'Households inferred from shared address / surname';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.customer_fit_profile (
  unified_customer_id STRING NOT NULL, shoe_size_us DOUBLE, shoe_width STRING,
  arch_type STRING, pronation_type STRING, apparel_top_size STRING,
  apparel_bottom_size STRING, size_source STRING COMMENT 'gait_lab | purchase_history | returns | declared',
  size_confidence DOUBLE, last_gait_analysis_at TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'Customer fit profile from gait lab, purchases and returns';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.consent (
  unified_customer_id STRING NOT NULL, channel STRING COMMENT 'email | sms | push | paid_media | profiling',
  purpose STRING, status STRING COMMENT 'opted_in | opted_out | not_set',
  jurisdiction STRING, policy_version STRING, capture_source STRING,
  effective_at TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'Latest consent state per customer x channel x purpose';

-- ---------------- Product / location ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.products (
  style_id STRING NOT NULL, style_name STRING, division STRING, category STRING,
  sub_category STRING, sport STRING, gender STRING, technology ARRAY<STRING>,
  description STRING, msrp DECIMAL(10,2), unit_cost DECIMAL(10,2),
  margin_pct DOUBLE, launch_date DATE, lifecycle_status STRING,
  is_limited_drop BOOLEAN, collab_partner STRING, image_url STRING,
  CONSTRAINT pk_products PRIMARY KEY (style_id)
) COMMENT 'Product styles';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.skus (
  sku STRING NOT NULL, style_id STRING, colorway STRING, color_family STRING,
  size STRING, size_us DOUBLE, width STRING, upc STRING,
  CONSTRAINT pk_skus PRIMARY KEY (sku)
) COMMENT 'Sellable SKUs';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.product_drops (
  drop_id STRING NOT NULL, style_id STRING, drop_name STRING, release_ts TIMESTAMP,
  units_available INT, release_channel STRING, is_raffle BOOLEAN,
  member_early_access BOOLEAN
) COMMENT 'Limited-edition drops';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.price_history (
  sku STRING NOT NULL, channel STRING, price_type STRING COMMENT 'list | markdown | promo',
  price DECIMAL(10,2), effective_from DATE, effective_to DATE
) COMMENT 'SCD2-style price history';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.inventory_daily (
  snapshot_date DATE NOT NULL, location_id STRING, location_type STRING, sku STRING,
  on_hand_qty INT, on_order_qty INT, reserved_qty INT, available_qty INT
) COMMENT 'Daily available inventory by location and SKU';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.stores (
  store_id STRING NOT NULL, store_name STRING, store_type STRING,
  address_line1 STRING, city STRING, state STRING, postal_code STRING, country STRING,
  latitude DOUBLE, longitude DOUBLE, region STRING, open_date DATE, sq_ft INT,
  has_gait_lab BOOLEAN, hosts_run_club BOOLEAN,
  CONSTRAINT pk_stores PRIMARY KEY (store_id)
) COMMENT 'Store master';

-- ---------------- Commerce ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.orders (
  order_id STRING NOT NULL, unified_customer_id STRING, source_system STRING,
  channel STRING COMMENT 'store | web | app | marketplace',
  store_id STRING, order_ts TIMESTAMP, order_date DATE, order_status STRING,
  subtotal_amount DECIMAL(12,2), discount_amount DECIMAL(12,2),
  shipping_amount DECIMAL(12,2), tax_amount DECIMAL(12,2), total_amount DECIMAL(12,2),
  currency STRING, payment_type STRING, promo_code STRING, fulfillment_type STRING,
  item_count INT, is_full_price BOOLEAN, utm_source STRING, utm_medium STRING,
  utm_campaign STRING, session_id STRING,
  CONSTRAINT pk_orders PRIMARY KEY (order_id)
) COMMENT 'Unified orders across POS, web and app';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.order_lines (
  order_id STRING NOT NULL, line_number INT NOT NULL, unified_customer_id STRING,
  sku STRING, style_id STRING, category STRING, quantity INT,
  unit_price DECIMAL(12,2), discount_amount DECIMAL(12,2), line_total DECIMAL(12,2),
  line_margin DECIMAL(12,2), is_markdown BOOLEAN, order_ts TIMESTAMP
) COMMENT 'Unified order lines';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.returns (
  return_id STRING NOT NULL, order_id STRING, line_number INT, unified_customer_id STRING,
  sku STRING, quantity INT, refund_amount DECIMAL(12,2),
  return_reason STRING COMMENT 'too_small | too_big | comfort | defect | not_as_pictured | changed_mind | other',
  return_reason_text STRING, return_type STRING COMMENT 'refund | exchange',
  return_channel STRING, return_ts TIMESTAMP, is_fit_related BOOLEAN
) COMMENT 'Unified returns with standardized reasons';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.carts (
  cart_id STRING NOT NULL, unified_customer_id STRING, session_id STRING,
  cart_created_ts TIMESTAMP, last_activity_ts TIMESTAMP, item_count INT,
  cart_value DECIMAL(12,2), skus ARRAY<STRING>, cart_status STRING COMMENT 'active | converted | abandoned',
  converted_order_id STRING
) COMMENT 'Cart lifecycle including abandoned carts';

-- ---------------- Behaviour ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.web_app_events (
  event_id STRING NOT NULL, unified_customer_id STRING, anonymous_id STRING,
  session_id STRING, platform STRING COMMENT 'web | ios | android',
  event_type STRING, event_ts TIMESTAMP, event_date DATE, page_type STRING,
  sku STRING, style_id STRING, category STRING, search_term STRING,
  selected_size STRING, utm_source STRING, utm_medium STRING, utm_campaign STRING,
  device_type STRING, geo_region STRING, geo_postal_code STRING
) COMMENT 'Unified digital events with identity stitched';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.sessions (
  session_id STRING NOT NULL, unified_customer_id STRING, anonymous_id STRING,
  platform STRING, session_start_ts TIMESTAMP, session_end_ts TIMESTAMP,
  duration_seconds INT, page_views INT, product_views INT, add_to_carts INT,
  converted BOOLEAN, traffic_source STRING, utm_campaign STRING, landing_page STRING
) COMMENT 'Sessionized digital activity';

-- ---------------- Loyalty ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.loyalty_members (
  loyalty_id STRING NOT NULL, unified_customer_id STRING, tier STRING,
  points_balance INT, lifetime_points INT, enrolled_at TIMESTAMP,
  enrollment_channel STRING, tier_expiry_date DATE, status STRING
) COMMENT 'Loyalty members';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.loyalty_transactions (
  ledger_id STRING NOT NULL, loyalty_id STRING, unified_customer_id STRING,
  activity_type STRING COMMENT 'earn | redeem | expire | bonus | tier_change',
  points INT, reference_id STRING, reference_type STRING, activity_ts TIMESTAMP
) COMMENT 'Loyalty points ledger';

-- ---------------- Sport & community ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.fitness_activities (
  activity_id STRING NOT NULL, unified_customer_id STRING, activity_type STRING,
  start_ts TIMESTAMP, activity_date DATE, duration_seconds INT, distance_km DOUBLE,
  distance_miles DOUBLE, elevation_gain_m DOUBLE, avg_pace_sec_per_km DOUBLE,
  avg_heart_rate INT, shoe_registration_id STRING, sku STRING
) COMMENT 'Run-app activities linked to customers and shoes';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.shoe_mileage (
  shoe_registration_id STRING NOT NULL, unified_customer_id STRING, sku STRING,
  style_id STRING, purchase_order_id STRING, registered_at TIMESTAMP,
  retired_at TIMESTAMP, activity_count INT, total_miles DOUBLE,
  last_activity_at TIMESTAMP, avg_weekly_miles DOUBLE, is_active BOOLEAN
) COMMENT 'Cumulative mileage per registered shoe (drives replacement trigger)';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.event_registrations (
  registration_id STRING NOT NULL, unified_customer_id STRING, event_id STRING,
  event_name STRING, event_type STRING, event_date DATE, event_city STRING,
  distance_km DOUBLE, registered_at TIMESTAMP, registration_source STRING
) COMMENT 'Race and event registrations';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.run_club_attendance (
  checkin_id STRING NOT NULL, unified_customer_id STRING, store_id STRING,
  club_session_id STRING, checkin_ts TIMESTAMP, run_distance_km DOUBLE
) COMMENT 'Run club attendance';

-- ---------------- Marketing ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.campaigns (
  campaign_id STRING NOT NULL, campaign_name STRING, objective STRING,
  campaign_type STRING COMMENT 'batch | triggered | always_on | paid',
  channel STRING, segment_id STRING, offer_type STRING, offer_value STRING,
  start_date DATE, end_date DATE, budget DECIMAL(14,2), has_holdout BOOLEAN,
  holdout_pct DOUBLE, owner STRING,
  CONSTRAINT pk_campaigns PRIMARY KEY (campaign_id)
) COMMENT 'Campaign master (owned + paid)';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.message_events (
  message_event_id STRING NOT NULL, send_id STRING, campaign_id STRING,
  unified_customer_id STRING, channel STRING COMMENT 'email | sms | push',
  event_type STRING COMMENT 'send | deliver | open | click | bounce | unsubscribe | complaint',
  event_ts TIMESTAMP, variant STRING, link_url STRING, device_type STRING
) COMMENT 'Unified owned-channel message events';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.paid_media_performance (
  platform STRING, campaign_id STRING, ad_group_id STRING, ad_id STRING,
  audience_name STRING, report_date DATE, impressions BIGINT, clicks BIGINT,
  spend DECIMAL(14,2), video_views BIGINT, platform_conversions BIGINT,
  platform_conversion_value DECIMAL(14,2)
) COMMENT 'Daily paid media performance (normalized across platforms)';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.affiliate_redemptions (
  redemption_id STRING NOT NULL, promo_code STRING, partner_id STRING,
  partner_name STRING, partner_type STRING, order_id STRING,
  unified_customer_id STRING, redemption_ts TIMESTAMP,
  order_amount DECIMAL(12,2), commission_amount DECIMAL(12,2)
) COMMENT 'Affiliate / influencer code redemptions';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.marketing_touchpoints (
  touchpoint_id STRING NOT NULL, unified_customer_id STRING, touchpoint_ts TIMESTAMP,
  channel STRING, campaign_id STRING, touchpoint_type STRING COMMENT 'impression | click | open | visit',
  source STRING, medium STRING
) COMMENT 'Customer-level marketing touchpoints for attribution';

-- ---------------- Service & VoC ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.service_tickets (
  ticket_id STRING NOT NULL, unified_customer_id STRING, channel STRING,
  subject STRING, description STRING, category STRING, priority STRING,
  status STRING, order_id STRING, created_at TIMESTAMP, resolved_at TIMESTAMP,
  resolution_hours DOUBLE, sentiment STRING
) COMMENT 'Customer service tickets';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.surveys (
  survey_response_id STRING NOT NULL, unified_customer_id STRING,
  survey_type STRING COMMENT 'csat | nps', touchpoint STRING, ticket_id STRING,
  score INT, nps_category STRING COMMENT 'promoter | passive | detractor',
  comment STRING, sentiment STRING, response_ts TIMESTAMP
) COMMENT 'CSAT and NPS responses';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.product_reviews (
  review_id STRING NOT NULL, unified_customer_id STRING, sku STRING, style_id STRING,
  rating INT, title STRING, review_text STRING, fit_rating STRING,
  verified_purchase BOOLEAN, submitted_at TIMESTAMP,
  ai_sentiment STRING, ai_themes ARRAY<STRING> COMMENT 'e.g. fit, comfort, durability, style, price',
  ai_fit_feedback STRING COMMENT 'runs_small | true_to_size | runs_large'
) COMMENT 'Product reviews enriched with AI Functions';

-- ---------------- External ----------------
CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.weather_daily (
  postal_code STRING NOT NULL, weather_date DATE NOT NULL, temp_high_c DOUBLE,
  temp_low_c DOUBLE, precipitation_mm DOUBLE, snowfall_mm DOUBLE,
  condition STRING, is_forecast BOOLEAN, is_rain_day BOOLEAN, is_cold_snap BOOLEAN
) COMMENT 'Daily weather by postal code with trigger flags';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.sports_calendar (
  calendar_event_id STRING NOT NULL, event_name STRING, event_category STRING,
  sport STRING, region STRING, start_date DATE, end_date DATE, description STRING
) COMMENT 'Sports seasons, races and retail moments';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver}.zip_demographics (
  postal_code STRING NOT NULL, city STRING, state STRING, population INT,
  median_household_income INT, income_band STRING, median_age DOUBLE,
  pct_households_with_kids DOUBLE, urbanicity STRING
) COMMENT 'Demographics by postal code';
