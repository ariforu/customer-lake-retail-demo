# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 03 - Journey triggers
# MAGIC
# MAGIC **What it does:** Finds today's journey triggers (abandoned cart, worn-out shoes, rain on trail runners ...). Audiences themselves are built in Customer Lake.
# MAGIC
# MAGIC - **Reads:** gold Customer 360, scores and product feed; silver carts, sessions, returns, shoe mileage, weather
# MAGIC - **Writes:** `trigger_events`
# MAGIC - **Runs in:** `strideforge_medallion` -> `gold_triggers`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - One row per customer with profile, traits and scores
# COMMAND ----------

S, G = f"{a.catalog}.{a.silver}", f"{a.catalog}.{a.gold}"
sql = spark.sql

ASOF = f"(SELECT max(order_date) FROM {S}.orders)"

sql(f"""CREATE OR REPLACE TEMP VIEW seg_base AS
SELECT p.*, t.* EXCEPT (unified_customer_id, updated_at), pr.* EXCEPT (unified_customer_id)
FROM {G}.customer_profile p JOIN {G}.customer_traits t USING (unified_customer_id)
JOIN {G}.customer_predictions pr USING (unified_customer_id)""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Find today's journey triggers
# COMMAND ----------

sql(f"""INSERT OVERWRITE {G}.trigger_events BY NAME
WITH asof AS (SELECT {ASOF} AS d, CAST({ASOF} AS TIMESTAMP) + INTERVAL 1 DAY AS ts),
t AS (
  SELECT unified_customer_id, 'cart_abandoned' AS trigger_type, last_activity_ts AS trigger_ts,
         map('cart_id', cart_id, 'cart_value', CAST(cart_value AS STRING), 'items', CAST(item_count AS STRING)) AS payload,
         skus[0] AS sku, 'TRG-CART-ABANDON' AS campaign_id
  FROM {S}.carts WHERE cart_status = 'abandoned' AND unified_customer_id IS NOT NULL AND cart_value > 0
    AND last_activity_ts > (SELECT ts FROM asof) - INTERVAL 3 DAYS
  UNION ALL
  SELECT unified_customer_id, 'browse_abandoned', session_end_ts, map('session_id', session_id, 'product_views', CAST(product_views AS STRING)),
         NULL, 'TRG-BROWSE-ABANDON'
  FROM {S}.sessions WHERE unified_customer_id IS NOT NULL AND product_views >= 3 AND add_to_carts = 0 AND NOT converted
    AND session_start_ts > (SELECT ts FROM asof) - INTERVAL 2 DAYS
  UNION ALL
  SELECT e.unified_customer_id, 'back_in_stock_in_size', (SELECT ts FROM asof), map('size', f.size, 'stock_status', f.stock_status), e.sku,
         'TRG-BACK-IN-STOCK'
  FROM (SELECT DISTINCT unified_customer_id, sku FROM {S}.web_app_events WHERE event_type = 'back_in_stock_signup'
        AND unified_customer_id IS NOT NULL) e
  JOIN {G}.product_catalog_enriched f ON f.sku = e.sku AND f.stock_status IN ('in_stock', 'low_stock')
  UNION ALL
  SELECT unified_customer_id, 'shoe_mileage_threshold', last_activity_at, map('shoe_registration_id', shoe_registration_id,
         'miles', CAST(total_miles AS STRING)), sku, 'TRG-MILEAGE-REPLACE'
  FROM {S}.shoe_mileage WHERE is_active AND total_miles >= 400 AND unified_customer_id IS NOT NULL
    AND last_activity_at > (SELECT ts FROM asof) - INTERVAL 14 DAYS
  UNION ALL
  SELECT unified_customer_id, 'race_training_start', (SELECT ts FROM asof),
         map('goal_race_date', CAST(goal_race_date AS STRING), 'distance_km', CAST(goal_race_distance_km AS STRING)),
         NULL, 'TRG-RACE-16WK'
  FROM seg_base   -- a 16-week training block starts for the goal race
  WHERE goal_race_date BETWEEN (SELECT d FROM asof) + INTERVAL 105 DAYS AND (SELECT d FROM asof) + INTERVAL 125 DAYS
  UNION ALL
  SELECT unified_customer_id, 'weather_rain', (SELECT ts FROM asof), map('postal_code', postal_code), NULL, 'TRG-RAIN-TRAIL'
  FROM seg_base   -- trail runners with >=8mm rain forecast at home in the next 3 days
  WHERE coalesce(sport_affinity['Trail Running'], 0) >= 0.2 AND postal_code IN
    (SELECT postal_code FROM {S}.weather_daily WHERE is_forecast AND precipitation_mm >= 8
     AND weather_date > (SELECT d FROM asof) AND weather_date <= (SELECT d FROM asof) + INTERVAL 3 DAYS)
  UNION ALL
  SELECT unified_customer_id, 'tier_upgrade_near', (SELECT ts FROM asof), map('tier', 'Pro'), NULL, 'TRG-TIER-UPGRADE'
  FROM seg_base WHERE loyalty_tier = 'Pro' AND revenue_12m BETWEEN 800 AND 999
  UNION ALL
  SELECT r.unified_customer_id, 'fit_return', r.return_ts, map('return_id', r.return_id, 'reason', r.return_reason), r.sku, 'TRG-FIT-SAVE'
  FROM {S}.returns r WHERE r.is_fit_related AND r.unified_customer_id IS NOT NULL AND r.return_ts > (SELECT ts FROM asof) - INTERVAL 7 DAYS
  UNION ALL
  SELECT unified_customer_id, 'lapse_risk', (SELECT ts FROM asof), map('churn_probability', CAST(churn_probability AS STRING)), NULL, 'TRG-WINBACK-90'
  FROM seg_base WHERE churn_probability BETWEEN 0.6 AND 0.8 AND days_since_last_order BETWEEN 150 AND 180)
SELECT concat('TG', upper(substr(sha2(concat_ws('|', unified_customer_id, trigger_type, CAST(trigger_ts AS STRING), coalesce(sku, '')), 256), 1, 16))) AS trigger_id,
       t.unified_customer_id, trigger_type, trigger_ts, payload, sku, campaign_id,
       CASE WHEN p.email_contactable OR p.push_contactable OR p.sms_contactable THEN 'new' ELSE 'suppressed' END AS status,
       trigger_ts + INTERVAL 7 DAYS AS expires_at
FROM t JOIN {G}.customer_profile p USING (unified_customer_id)""")
# COMMAND ----------

print("triggers:", spark.table(f"{G}.trigger_events").count(), flush=True)
