# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 05 - Loyalty and sport
# MAGIC
# MAGIC **What it does:** Links loyalty points, run-app activity, shoe mileage, race sign-ups and run-club visits to customers.
# MAGIC
# MAGIC - **Reads:** bronze loyalty, run-app, race and run-club tables; `identity_graph`
# MAGIC - **Writes:** `loyalty_transactions`, `loyalty_members`, `fitness_activities`, `shoe_mileage`, `event_registrations`, `run_club_attendance`
# MAGIC - **Runs in:** `strideforge_medallion` -> `silver_loyalty_sport`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Lookup: identifier -> customer
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW idmap AS
SELECT id_type, id_value, min(unified_customer_id) AS unified_customer_id
FROM ${catalog}.${silver}.identity_graph GROUP BY id_type, id_value
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Loyalty points activity
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.loyalty_transactions BY NAME
SELECT l.ledger_id, l.loyalty_id, m.unified_customer_id, l.activity_type, l.points, l.reference_id, l.reference_type,
       CAST(l.activity_ts AS TIMESTAMP) AS activity_ts
FROM ${catalog}.${bronze}.loyalty_points_ledger_raw l
LEFT JOIN idmap m ON m.id_type = 'loyalty_id' AND m.id_value = l.loyalty_id
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Loyalty members with tier and lifetime points
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.loyalty_members BY NAME
WITH lt AS (SELECT loyalty_id, CAST(sum(CASE WHEN points > 0 THEN points ELSE 0 END) AS INT) AS lifetime_points,
                   max(CASE WHEN activity_type = 'tier_change' THEN activity_ts END) AS last_tier_change
            FROM ${catalog}.${silver}.loyalty_transactions GROUP BY 1)
SELECT r.loyalty_id, m.unified_customer_id, r.tier, r.points_balance, coalesce(lt.lifetime_points, 0) AS lifetime_points,
       CAST(r.enrolled_ts AS TIMESTAMP) AS enrolled_at, r.enrollment_channel,
       date_add(to_date(coalesce(lt.last_tier_change, CAST(r.enrolled_ts AS TIMESTAMP))), 365) AS tier_expiry_date, r.status
FROM ${catalog}.${bronze}.loyalty_members_raw r
LEFT JOIN lt USING (loyalty_id)
LEFT JOIN idmap m ON m.id_type = 'loyalty_id' AND m.id_value = r.loyalty_id
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Run-app activities
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.fitness_activities BY NAME
SELECT a.activity_id, m.unified_customer_id, a.activity_type, CAST(a.start_ts AS TIMESTAMP) AS start_ts,
       to_date(CAST(a.start_ts AS TIMESTAMP)) AS activity_date, a.duration_seconds, a.distance_km,
       round(a.distance_km * 0.621371, 2) AS distance_miles, a.elevation_gain_m, a.avg_pace_sec_per_km, a.avg_heart_rate,
       a.shoe_registration_id, s.sku
FROM ${catalog}.${bronze}.fitness_activities_raw a
LEFT JOIN ${catalog}.${bronze}.fitness_shoe_registrations_raw s USING (shoe_registration_id)
LEFT JOIN idmap m ON m.id_type = 'fitness_id' AND m.id_value = a.fitness_user_id
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Miles on each registered shoe
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.shoe_mileage BY NAME
WITH act AS (SELECT shoe_registration_id, count(*) AS n, sum(distance_miles) AS miles, max(start_ts) AS last_ts
             FROM ${catalog}.${silver}.fitness_activities WHERE shoe_registration_id IS NOT NULL GROUP BY 1),
asof AS (SELECT max(start_ts) AS t FROM ${catalog}.${silver}.fitness_activities)
SELECT s.shoe_registration_id, m.unified_customer_id, s.sku, k.style_id, s.order_reference AS purchase_order_id,
       CAST(s.registered_ts AS TIMESTAMP) AS registered_at, CAST(s.retired_ts AS TIMESTAMP) AS retired_at,
       CAST(coalesce(act.n, 0) AS INT) AS activity_count, round(coalesce(act.miles, 0), 1) AS total_miles,
       act.last_ts AS last_activity_at,
       round(coalesce(act.miles, 0) / greatest(1, datediff(coalesce(CAST(s.retired_ts AS TIMESTAMP), (SELECT t FROM asof)),
                                                            CAST(s.registered_ts AS TIMESTAMP)) / 7.0), 1) AS avg_weekly_miles,
       s.retired_ts IS NULL AS is_active
FROM ${catalog}.${bronze}.fitness_shoe_registrations_raw s
LEFT JOIN act USING (shoe_registration_id)
LEFT JOIN ${catalog}.${silver}.skus k ON k.sku = s.sku
LEFT JOIN idmap m ON m.id_type = 'fitness_id' AND m.id_value = s.fitness_user_id
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Race sign-ups (duplicates removed)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.event_registrations BY NAME
SELECT e.registration_id, m.unified_customer_id, e.event_id, e.event_name, e.event_type, to_date(e.event_date) AS event_date,
       e.event_city, e.distance_km, CAST(e.registered_ts AS TIMESTAMP) AS registered_at, e.registration_source
FROM ${catalog}.${bronze}.event_registrations_raw e
LEFT JOIN idmap m ON m.id_type = 'email' AND m.id_value = ${catalog}.${silver}.norm_email(e.email)
-- partners resend re-registrations for the same person + race: keep the first registration
QUALIFY row_number() OVER (PARTITION BY e.registration_id ORDER BY CAST(e.registered_ts AS TIMESTAMP)) = 1
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Run-club visits
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.run_club_attendance BY NAME
SELECT c.checkin_id, coalesce(m1.unified_customer_id, m2.unified_customer_id) AS unified_customer_id, c.store_id,
       c.club_session_id, CAST(c.checkin_ts AS TIMESTAMP) AS checkin_ts, c.run_distance_km
FROM ${catalog}.${bronze}.run_club_checkins_raw c
LEFT JOIN idmap m1 ON m1.id_type = 'loyalty_id' AND m1.id_value = c.loyalty_id
LEFT JOIN idmap m2 ON m2.id_type = 'email' AND m2.id_value = ${catalog}.${silver}.norm_email(c.email)
""")
