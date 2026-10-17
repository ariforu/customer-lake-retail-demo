# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 03 - Audiences, triggers & exports
# MAGIC
# MAGIC Gold audiences: segment definitions (rules as data) -> membership, journey triggers, drop calendar, audience exports.
# COMMAND ----------

for _k, _d in [("catalog", "customer_lake_si"), ("bronze", "retail_bronze"), ("silver", "retail_silver"),
               ("gold", "retail_gold"), ("truth", "retail_synthetic_truth")]:
    dbutils.widgets.text(_k, _d)


class _Args:
    pass


a = _Args()
for _k in ["catalog", "bronze", "silver", "gold", "truth"]:
    setattr(a, _k, dbutils.widgets.get(_k))
S, G = f"{a.catalog}.{a.silver}", f"{a.catalog}.{a.gold}"
sql = spark.sql

ASOF = f"(SELECT max(order_date) FROM {S}.orders)"

# Rules are WHERE-clauses evaluated against seg_base (profile + traits + predictions + consent, one row per customer)
SEGMENTS = [
    ("SEG-MILEAGE-REPLACE", "Runners due for new shoes", "Active registered shoe past 400 miles; email-contactable",
     "ml", "active_shoe_miles >= 400 AND email_contactable"),
    ("SEG-CS3-FIT-SAVE", "CloudStride 3 fit-return save", "Refunded a CloudStride 3 for 'too small' in the last 120 days",
     "dynamic", f"""unified_customer_id IN (SELECT r.unified_customer_id FROM {S}.returns r JOIN {S}.skus k USING (sku)
         JOIN {S}.products p USING (style_id) WHERE p.style_name LIKE 'CloudStride 3%' AND r.return_reason = 'too_small'
         AND r.return_type = 'refund' AND r.return_ts > {ASOF} - INTERVAL 120 DAYS)"""),
    ("SEG-MARATHON-16WK", "Goal race in 8-20 weeks", "Registered for a half/full marathon 56-140 days out (training block)",
     "dynamic", f"upcoming_race_distance_km >= 21 AND upcoming_race_date BETWEEN {ASOF} + INTERVAL 56 DAYS AND {ASOF} + INTERVAL 140 DAYS"),
    ("SEG-HIGH-CLV-AT-RISK", "High value at risk", "Platinum/gold CLV with churn probability above 0.5",
     "ml", "clv_tier IN ('platinum', 'gold') AND churn_probability > 0.5"),
    ("SEG-LAPSED-WINBACK", "Lapsed win-back (discount OK)", "Lapsed >365 days, not a full-price buyer, email-contactable",
     "dynamic", "lifecycle_stage = 'lapsed' AND coalesce(discount_sensitivity, 'mixed') <> 'full_price' AND email_contactable"),
    ("SEG-SNEAKERHEADS", "Sneakerheads", "High drop-hype score, low reseller risk",
     "ml", "drop_hype_score >= 0.5 AND reseller_risk_score < 0.5"),
    ("SEG-RESELLER-SUSPECTS", "Reseller suspects (suppress from drops)", "Bought 3+ sizes of one limited drop",
     "ml", "reseller_risk_score >= 0.6"),
    ("SEG-BTS-FAMILIES", "Back-to-school families", "Household with kids purchases",
     "dynamic", "size(kids_age_bands) > 0"),
    ("SEG-TIER-UPGRADE", "Close to Elite", "Pro members within $200 of the Elite threshold",
     "dynamic", "loyalty_tier = 'Pro' AND revenue_12m BETWEEN 800 AND 999"),
    ("SEG-LOOKALIKE-SEED", "Lookalike seed (top 5% CLV)", "Top 5% predicted CLV and paid-media eligible",
     "lookalike", f"paid_media_eligible AND predicted_clv_12m >= (SELECT percentile(predicted_clv_12m, 0.95) FROM {G}.customer_predictions)"),
    ("SEG-RECENT-BUYERS-SUPPRESS", "Recent purchasers (paid suppression)", "Ordered in the last 14 days",
     "dynamic", f"last_order_date > {ASOF} - INTERVAL 14 DAYS"),
    ("SEG-EMAIL-FATIGUED", "Email fatigued", "Contactable but opened <5% of emails in 90 days",
     "dynamic", "email_contactable AND email_open_rate_90d < 0.05"),
    ("SEG-TRAIL-RAIN", "Trail runners + heavy rain in 3 days", "Trail affinity and >=8mm rain forecast in home postal code",
     "dynamic", f"""coalesce(sport_affinity['Trail Running'], 0) >= 0.2 AND postal_code IN
         (SELECT postal_code FROM {S}.weather_daily WHERE is_forecast AND precipitation_mm >= 8 AND weather_date <= {ASOF} + INTERVAL 3 DAYS)"""),
]

sql(f"""CREATE OR REPLACE TEMP VIEW seg_base AS
SELECT p.*, t.* EXCEPT (unified_customer_id, updated_at), pr.* EXCEPT (unified_customer_id)
FROM {G}.customer_profile p JOIN {G}.customer_traits t USING (unified_customer_id)
JOIN {G}.customer_predictions pr USING (unified_customer_id)""")

defs = spark.createDataFrame(
    [(sid, name, desc, typ, rule) for sid, name, desc, typ, rule in SEGMENTS],
    "segment_id string, segment_name string, description string, segment_type string, rule_sql string")
defs.createOrReplaceTempView("seg_defs")
sql(f"""INSERT OVERWRITE {G}.segment_definitions BY NAME
SELECT segment_id, segment_name, description, segment_type, rule_sql, 'cdp_team' AS owner, 'daily' AS refresh_schedule,
       true AS is_active, current_timestamp() AS created_at, current_timestamp() AS updated_at FROM seg_defs""")

members = " UNION ALL ".join(
    f"SELECT '{sid}' AS segment_id, unified_customer_id FROM seg_base WHERE {rule}" for sid, _, _, _, rule in SEGMENTS)
sql(f"""INSERT OVERWRITE {G}.segment_membership BY NAME
SELECT segment_id, unified_customer_id, current_timestamp() AS entered_at, CAST(NULL AS TIMESTAMP) AS exited_at,
       true AS is_current, pmod(xxhash64(unified_customer_id, segment_id), 10) = 0 AS is_holdout,
       current_timestamp() AS refreshed_at
FROM ({members})""")
for r in sql(f"SELECT segment_id, count(*) n FROM {G}.segment_membership GROUP BY 1 ORDER BY 1").collect():
    print(f"  {r.segment_id:28s} {r.n:>10,d}", flush=True)
# COMMAND ----------

# ---------------------------------------------------------------- journey triggers (as of the latest data day)
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
  JOIN {G}.product_catalog_enriched f ON f.sku = e.sku AND f.stock_status <> 'out_of_stock'
  UNION ALL
  SELECT unified_customer_id, 'shoe_mileage_threshold', last_activity_at, map('shoe_registration_id', shoe_registration_id,
         'miles', CAST(total_miles AS STRING)), sku, 'TRG-MILEAGE-REPLACE'
  FROM {S}.shoe_mileage WHERE is_active AND total_miles >= 400 AND unified_customer_id IS NOT NULL
    AND last_activity_at > (SELECT ts FROM asof) - INTERVAL 14 DAYS
  UNION ALL
  SELECT unified_customer_id, 'race_training_start', (SELECT ts FROM asof), map('event', event_name, 'event_date', CAST(event_date AS STRING)),
         NULL, 'TRG-RACE-16WK'
  FROM {S}.event_registrations WHERE unified_customer_id IS NOT NULL AND distance_km >= 21
    AND event_date BETWEEN (SELECT d FROM asof) + INTERVAL 105 DAYS AND (SELECT d FROM asof) + INTERVAL 125 DAYS
  UNION ALL
  SELECT m.unified_customer_id, 'weather_rain', (SELECT ts FROM asof), map('postal_code', p.postal_code), NULL, 'TRG-RAIN-TRAIL'
  FROM {G}.segment_membership m JOIN {G}.customer_profile p USING (unified_customer_id) WHERE m.segment_id = 'SEG-TRAIL-RAIN'
  UNION ALL
  SELECT unified_customer_id, 'tier_upgrade_near', (SELECT ts FROM asof), map('tier', 'Pro'), NULL, 'TRG-TIER-UPGRADE'
  FROM {G}.segment_membership WHERE segment_id = 'SEG-TIER-UPGRADE'
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

# ---------------------------------------------------------------- drop calendar
sql(f"""INSERT OVERWRITE {G}.drop_calendar BY NAME
SELECT d.drop_id, d.style_id, d.drop_name, d.release_ts, d.units_available, 'SEG-SNEAKERHEADS' AS eligible_segment_id,
       (SELECT count(*) FROM {G}.segment_membership WHERE segment_id = 'SEG-SNEAKERHEADS') AS eligible_customer_count,
       CASE WHEN d.member_early_access THEN d.release_ts - INTERVAL 1 DAY END AS early_access_start_ts, d.is_raffle
FROM {S}.product_drops d""")
# COMMAND ----------

# ---------------------------------------------------------------- audience exports (consent-filtered syncs)
sql(f"""INSERT OVERWRITE {G}.audience_exports BY NAME
WITH x AS (
  SELECT 'SEG-MILEAGE-REPLACE' AS seg, 'TRG-MILEAGE-REPLACE' AS camp, 'esp' AS dest, 'email' AS idt
  UNION ALL SELECT 'SEG-CS3-FIT-SAVE', 'TRG-FIT-SAVE', 'esp', 'email'
  UNION ALL SELECT 'SEG-LOOKALIKE-SEED', 'META-LAL-SEED', 'meta', 'email_sha256'
  UNION ALL SELECT 'SEG-RECENT-BUYERS-SUPPRESS', 'GOOGLE-SUPPRESS', 'google', 'email_sha256')
SELECT concat('EX', upper(substr(sha2(concat_ws('|', x.seg, x.dest, m.unified_customer_id), 256), 1, 14))) AS export_id,
       x.seg AS segment_id, x.camp AS campaign_id, x.dest AS destination, m.unified_customer_id, x.idt AS matched_id_type,
       CAST({ASOF} AS TIMESTAMP) + INTERVAL 1 DAY + INTERVAL 6 HOURS AS exported_at, 'synced' AS sync_status
FROM x JOIN {G}.segment_membership m ON m.segment_id = x.seg AND NOT m.is_holdout
JOIN {G}.customer_profile p USING (unified_customer_id)
WHERE (x.dest = 'esp' AND p.email_contactable) OR (x.dest IN ('meta', 'google') AND p.paid_media_eligible)""")
print("triggers:", spark.table(f"{G}.trigger_events").count(), " exports:", spark.table(f"{G}.audience_exports").count(), flush=True)
