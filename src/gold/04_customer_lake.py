# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 04 - Customer Lake tables
# MAGIC
# MAGIC **What it does:** Fills the household table, purchase line items and nine customer event tables that Customer Lake entities point at, then checks Customer Lake's rules (unique ids, TIMESTAMP column). The tables and their descriptions are created by the gold DDL notebook.
# MAGIC
# MAGIC - **Reads:** gold Customer 360 tables; silver orders, order lines, returns, events, messages, run, loyalty, race, support and review tables
# MAGIC - **Writes:** `household_profile`, `purchase_line_items` and the `*_events` tables
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
INSERT OVERWRITE ${catalog}.${gold}.household_profile BY NAME
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

# MAGIC %md
# MAGIC ### Step 2 - Purchase events (order level, enriched with what was bought)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.purchase_events BY NAME
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
# MAGIC ### Step 3 - Purchase line items (one row per order line, with the SKU)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.purchase_line_items BY NAME
WITH fx AS (SELECT * FROM VALUES ('USD', 1.0), ('CAD', 1.35), ('GBP', 0.80), ('EUR', 0.93) AS t(currency, rate))
SELECT concat(l.order_id, '-', l.line_number) AS order_line_id, l.order_id, o.unified_customer_id, l.sku,
       CAST(l.unit_price / fx.rate AS DECIMAL(12, 2)) AS unit_price_usd,
       CAST(l.discount_amount / fx.rate AS DECIMAL(12, 2)) AS discount_usd,
       CAST(l.line_total / fx.rate AS DECIMAL(14, 2)) AS revenue_usd
FROM ${catalog}.${silver}.order_lines l
JOIN ${catalog}.${silver}.orders o USING (order_id)
JOIN fx ON fx.currency = o.currency
WHERE o.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Return events
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.return_events BY NAME
SELECT r.return_id, r.unified_customer_id, r.return_ts, r.order_id, r.sku, k.style_id, p.style_name, p.category, p.sport,
       r.return_reason, r.is_fit_related, r.return_type, r.return_channel, r.refund_amount, r.quantity
FROM ${catalog}.${silver}.returns r
LEFT JOIN ${catalog}.${silver}.skus k USING (sku)
LEFT JOIN ${catalog}.${silver}.products p ON p.style_id = k.style_id
WHERE r.unified_customer_id IS NOT NULL AND r.return_ts IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Digital (web + app) events
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.digital_events BY NAME
SELECT event_id, unified_customer_id, event_ts, platform, event_type, session_id, page_type, sku, style_id, category,
       search_term, selected_size, utm_source, utm_medium, utm_campaign, device_type
FROM ${catalog}.${silver}.web_app_events WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Message engagement events (email / SMS / push sends, opens, clicks, bounces, unsubscribes)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.message_engagement_events BY NAME
SELECT m.message_event_id, m.unified_customer_id, m.event_ts, m.channel, m.event_type, m.campaign_id, c.campaign_name,
       c.objective AS campaign_objective, c.campaign_type, m.variant, m.link_url, m.device_type
FROM ${catalog}.${silver}.message_events m
LEFT JOIN ${catalog}.${silver}.campaigns c USING (campaign_id)
WHERE m.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Run-app activity, loyalty, race registration, support and review events
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.run_activity_events BY NAME
SELECT a.activity_id, a.unified_customer_id, a.start_ts, a.activity_type, a.distance_km, a.distance_miles, a.duration_seconds,
       a.avg_pace_sec_per_km, a.elevation_gain_m, a.shoe_registration_id, a.sku, m.total_miles AS shoe_total_miles
FROM ${catalog}.${silver}.fitness_activities a
LEFT JOIN ${catalog}.${silver}.shoe_mileage m USING (shoe_registration_id)
WHERE a.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.loyalty_events BY NAME
SELECT ledger_id, unified_customer_id, activity_ts, loyalty_id, activity_type, points, reference_id, reference_type
FROM ${catalog}.${silver}.loyalty_transactions WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.race_registration_events BY NAME
SELECT registration_id, unified_customer_id, registered_at, event_id, event_name, event_type, event_date, event_city,
       distance_km, registration_source
FROM ${catalog}.${silver}.event_registrations WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.support_events BY NAME
SELECT ticket_id, unified_customer_id, created_at, channel, category, priority, status, sentiment, resolution_hours, order_id
FROM ${catalog}.${silver}.service_tickets WHERE unified_customer_id IS NOT NULL
""")
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.review_events BY NAME
SELECT r.review_id, r.unified_customer_id, r.submitted_at, r.sku, r.style_id, p.style_name, p.category, r.rating,
       r.fit_rating, r.ai_sentiment, r.ai_fit_feedback, r.ai_themes, r.verified_purchase
FROM ${catalog}.${silver}.product_reviews r
LEFT JOIN ${catalog}.${silver}.products p USING (style_id)
WHERE r.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Validate Customer Lake requirements
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
for t, key in {"household_profile": "household_id", "purchase_line_items": "order_line_id"}.items():
    r = spark.table(f"{G}.{t}").selectExpr("count(*) n", f"count(DISTINCT {key}) d").first()
    print(f"{t:28s} rows={r.n:>12,d} distinct_ids={r.d:>12,d}")
    if r.n != r.d:
        problems.append(t)
assert not problems, f"Customer Lake requirement violations: {problems}"
