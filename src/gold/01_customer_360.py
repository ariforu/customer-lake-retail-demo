# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 01 - Customer 360
# MAGIC
# MAGIC **What it does:** Builds the one-row-per-customer view the CDP uses: who they are, whether we may contact them, all their identifiers, and what they have done. Also records identity-resolution statistics for the run.
# MAGIC
# MAGIC - **Reads:** silver customers, consent, orders, events, loyalty, sport and service tables; identity graph and match pairs
# MAGIC - **Writes:** `customer_consent`, `customer_profile`, `customer_identity_map`, `customer_traits`, `identity_resolution_stats`
# MAGIC - **Runs in:** `strideforge_medallion` -> `gold_customer_360`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Set the 'as of' date (latest order day)
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW asof AS
SELECT max(order_date) AS d, CAST(max(order_date) AS TIMESTAMP) + INTERVAL 1 DAY AS ts FROM ${catalog}.${silver}.orders
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Lookup: currency exchange rates
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW fx AS
SELECT * FROM VALUES ('USD', 1.0), ('CAD', 1.35), ('GBP', 0.80), ('EUR', 0.93) AS t(currency, rate)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - May we contact them? (per channel)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.customer_consent BY NAME
WITH c AS (
  SELECT unified_customer_id,
         max(CASE WHEN channel = 'email' AND purpose = 'marketing' THEN status END) AS email_s,
         max(CASE WHEN channel = 'sms' THEN status END) AS sms_s,
         max(CASE WHEN channel = 'push' THEN status END) AS push_s,
         max(CASE WHEN channel = 'paid_media' THEN status END) AS paid_s,
         max(CASE WHEN channel = 'profiling' THEN status END) AS prof_s,
         max(jurisdiction) AS jurisdiction,
         max(CASE WHEN purpose = 'sale_of_data' AND status = 'opted_out' THEN 1 ELSE 0 END) = 1 AS do_not_sell
  FROM ${catalog}.${silver}.consent GROUP BY 1),
sup AS (
  SELECT unified_customer_id, CASE WHEN count_if(event_type = 'complaint') > 0 THEN 'spam_complaint' ELSE 'repeated_bounce' END AS reason
  FROM ${catalog}.${silver}.message_events WHERE event_type IN ('bounce', 'complaint') AND unified_customer_id IS NOT NULL
  GROUP BY 1 HAVING count_if(event_type = 'complaint') > 0 OR count_if(event_type = 'bounce') >= 2)
SELECT cu.unified_customer_id,
       coalesce(c.email_s = 'opted_in', false) AND sup.reason IS NULL AS email_marketing,
       coalesce(c.sms_s = 'opted_in', false) AS sms_marketing,
       coalesce(c.push_s = 'opted_in', false) AS push_marketing,
       CASE WHEN c.jurisdiction IN ('GDPR', 'UK-GDPR') THEN coalesce(c.paid_s = 'opted_in', false)
            ELSE NOT coalesce(c.do_not_sell, false) END AS paid_media_targeting,
       CASE WHEN c.jurisdiction IN ('GDPR', 'UK-GDPR') THEN coalesce(c.prof_s = 'opted_in', false) ELSE true END AS personalization_profiling,
       c.jurisdiction, coalesce(c.do_not_sell, false) AS do_not_sell, sup.reason IS NOT NULL AS global_suppression,
       sup.reason AS suppression_reason, current_timestamp() AS updated_at
FROM ${catalog}.${silver}.customers cu LEFT JOIN c USING (unified_customer_id) LEFT JOIN sup USING (unified_customer_id)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Customer profile (one row per customer)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.customer_profile BY NAME
WITH ids AS (SELECT unified_customer_id, max(id_type = 'app_id') AS has_app, max(id_type = 'fitness_id') AS has_fit
             FROM ${catalog}.${silver}.identity_graph GROUP BY 1),
city_store AS (SELECT city, country, min(store_id) AS store_id FROM ${catalog}.${silver}.stores
               WHERE store_type IN ('flagship', 'standard') GROUP BY 1, 2)
SELECT c.unified_customer_id, c.first_name, c.last_name, c.email, c.phone_e164, c.birth_date, c.age_band, c.gender,
       a.city, a.state, a.postal_code, a.country, c.household_id, h.kids_age_bands, c.home_store_id,
       coalesce(c.home_store_id, cs.store_id) AS nearest_store_id, CAST(NULL AS DOUBLE) AS nearest_store_distance_km,
       lm.loyalty_id, lm.tier AS loyalty_tier, lm.points_balance, f.shoe_size_us, f.shoe_width, f.pronation_type,
       f.apparel_top_size, f.apparel_bottom_size, c.is_known, coalesce(ids.has_app, false) AS has_app,
       coalesce(ids.has_fit, false) AS has_fitness_app,
       coalesce(k.email_marketing, false) AND c.email IS NOT NULL AS email_contactable,
       coalesce(k.sms_marketing, false) AND c.phone_e164 IS NOT NULL AS sms_contactable,
       coalesce(k.push_marketing, false) AND coalesce(ids.has_app, false) AS push_contactable,
       coalesce(k.paid_media_targeting, false) AND NOT coalesce(k.global_suppression, false) AS paid_media_eligible,
       c.first_seen_at, c.last_seen_at, current_timestamp() AS updated_at
FROM ${catalog}.${silver}.customers c
LEFT JOIN ${catalog}.${silver}.customer_addresses a ON a.address_id = c.primary_address_id
LEFT JOIN ${catalog}.${silver}.households h ON h.household_id = c.household_id
LEFT JOIN (SELECT * FROM ${catalog}.${silver}.loyalty_members WHERE unified_customer_id IS NOT NULL
           QUALIFY row_number() OVER (PARTITION BY unified_customer_id ORDER BY lifetime_points DESC) = 1) lm
  ON lm.unified_customer_id = c.unified_customer_id
LEFT JOIN ${catalog}.${silver}.customer_fit_profile f ON f.unified_customer_id = c.unified_customer_id
LEFT JOIN ${catalog}.${gold}.customer_consent k ON k.unified_customer_id = c.unified_customer_id
LEFT JOIN ids ON ids.unified_customer_id = c.unified_customer_id
LEFT JOIN city_store cs ON cs.city = a.city AND cs.country = a.country
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - All identifiers per customer (incl. hashed email/phone)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.customer_identity_map BY NAME
WITH g AS (
  SELECT unified_customer_id, id_type, id_value, source_system, is_primary, last_seen
  FROM ${catalog}.${silver}.identity_graph
  WHERE id_type IN ('email', 'phone', 'device_id', 'cookie_id', 'loyalty_id', 'loyalty_card', 'push_token', 'ecom_id', 'pos_id', 'app_id', 'fitness_id'))
SELECT unified_customer_id, CASE id_type WHEN 'phone' THEN 'phone_e164' ELSE id_type END AS id_type, id_value, source_system,
       is_primary, last_seen AS last_seen_at, current_timestamp() AS updated_at FROM g
UNION ALL
SELECT unified_customer_id, 'email_sha256', sha2(id_value, 256), source_system, is_primary, last_seen, current_timestamp()
FROM g WHERE id_type = 'email'
UNION ALL
SELECT unified_customer_id, 'phone_sha256', sha2(id_value, 256), source_system, is_primary, last_seen, current_timestamp()
FROM g WHERE id_type = 'phone'
UNION ALL
SELECT unified_customer_id, 'maid', upper(id_value), source_system, is_primary, last_seen, current_timestamp()
FROM g WHERE id_type = 'device_id'
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Helper: orders in USD
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW o AS
SELECT o.*, CAST((o.subtotal_amount - o.discount_amount) / fx.rate AS DECIMAL(14, 2)) AS net_usd
FROM ${catalog}.${silver}.orders o JOIN fx USING (currency) WHERE o.unified_customer_id IS NOT NULL
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Helper: order totals per customer
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW t_orders AS
SELECT unified_customer_id, min(order_date) AS first_order_date, max(order_date) AS last_order_date,
       count(*) AS n_life, count_if(order_date > (SELECT d FROM asof) - INTERVAL 365 DAYS) AS n_12m,
       sum(net_usd) AS rev_life, sum(CASE WHEN order_date > (SELECT d FROM asof) - INTERVAL 365 DAYS THEN net_usd ELSE 0 END) AS rev_12m,
       avg(CASE WHEN is_full_price THEN 1.0 ELSE 0 END) AS full_price_ratio, mode(channel) AS preferred_channel,
       mode(CASE WHEN channel = 'store' THEN store_id END) AS preferred_store_id,
       count(DISTINCT CASE WHEN channel = 'store' THEN 'store' ELSE 'online' END) > 1 AS is_omnichannel
FROM o GROUP BY 1
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Helper: what each customer buys
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW t_lines AS
WITH l AS (SELECT l.*, p.sport, p.gender AS pgender, p.is_limited_drop, ol.currency
           FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.products p ON p.style_id = l.style_id
           JOIN ${catalog}.${silver}.orders ol USING (order_id) WHERE l.unified_customer_id IS NOT NULL),
cat AS (SELECT unified_customer_id, category, sum(quantity) q FROM l GROUP BY 1, 2),
sp AS (SELECT unified_customer_id, sport, sum(quantity) q FROM l GROUP BY 1, 2),
r AS (SELECT unified_customer_id, count(*) AS n_ret, count_if(is_fit_related) AS n_fit FROM ${catalog}.${silver}.returns
      WHERE unified_customer_id IS NOT NULL GROUP BY 1)
SELECT a.unified_customer_id, a.n_lines, a.margin_12m, a.drops, a.gifter, coalesce(r.n_ret, 0) / a.n_lines AS return_rate,
       coalesce(r.n_fit, 0) AS fit_return_count, c.top_category, c.category_affinity, s.top_sport, s.sport_affinity
FROM (SELECT unified_customer_id, count(*) AS n_lines,
             sum(CASE WHEN order_ts > (SELECT ts FROM asof) - INTERVAL 365 DAYS THEN line_margin ELSE 0 END) AS margin_12m,
             count_if(is_limited_drop) AS drops,
             (count_if(category IN ('Kids Footwear', 'Kids Apparel')) > 0 OR count(DISTINCT pgender) FILTER (WHERE pgender IN ('Men', 'Women')) > 1) AS gifter
      FROM l GROUP BY 1) a
LEFT JOIN r USING (unified_customer_id)
LEFT JOIN (SELECT unified_customer_id, max_by(category, q) AS top_category,
                  map_from_entries(collect_list(struct(category, round(q / tot, 3)))) AS category_affinity
           FROM (SELECT *, sum(q) OVER (PARTITION BY unified_customer_id) AS tot FROM cat) GROUP BY 1) c USING (unified_customer_id)
LEFT JOIN (SELECT unified_customer_id, max_by(sport, q) AS top_sport,
                  map_from_entries(collect_list(struct(sport, round(q / tot, 3)))) AS sport_affinity
           FROM (SELECT *, sum(q) OVER (PARTITION BY unified_customer_id) AS tot FROM sp) GROUP BY 1) s USING (unified_customer_id)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - Behaviour traits (RFM, lifecycle, affinities, running ...)
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.customer_traits BY NAME
WITH dig AS (SELECT unified_customer_id, count(*) AS sessions_30d, sum(product_views) AS pv_30d FROM ${catalog}.${silver}.sessions
             WHERE unified_customer_id IS NOT NULL AND session_start_ts > (SELECT ts FROM asof) - INTERVAL 30 DAYS GROUP BY 1),
em AS (SELECT unified_customer_id, count(DISTINCT CASE WHEN event_type = 'send' THEN send_id END) AS sends,
              count(DISTINCT CASE WHEN event_type = 'open' THEN send_id END) AS opens,
              count(DISTINCT CASE WHEN event_type = 'click' THEN send_id END) AS clicks
       FROM ${catalog}.${silver}.message_events
       WHERE channel = 'email' AND unified_customer_id IS NOT NULL AND event_ts > (SELECT ts FROM asof) - INTERVAL 90 DAYS GROUP BY 1),
run AS (SELECT unified_customer_id, sum(CASE WHEN start_ts > (SELECT ts FROM asof) - INTERVAL 30 DAYS THEN distance_miles END) AS m30,
               sum(CASE WHEN start_ts > (SELECT ts FROM asof) - INTERVAL 90 DAYS THEN distance_miles END) AS m90
        FROM ${catalog}.${silver}.fitness_activities WHERE unified_customer_id IS NOT NULL
          AND start_ts > (SELECT ts FROM asof) - INTERVAL 90 DAYS GROUP BY 1),
race AS (SELECT unified_customer_id,
                 min_by(event_date, event_date) AS race_date, min_by(distance_km, event_date) AS race_km,
                 min(CASE WHEN coalesce(distance_km, 0) >= 21 THEN event_date END) AS goal_date,
                 min_by(CASE WHEN coalesce(distance_km, 0) >= 21 THEN distance_km END,
                        CASE WHEN coalesce(distance_km, 0) >= 21 THEN event_date END) AS goal_km
         FROM ${catalog}.${silver}.event_registrations WHERE unified_customer_id IS NOT NULL AND event_date > (SELECT d FROM asof) GROUP BY 1),
club AS (SELECT unified_customer_id, count(*) AS n FROM ${catalog}.${silver}.run_club_attendance
         WHERE unified_customer_id IS NOT NULL AND checkin_ts > (SELECT ts FROM asof) - INTERVAL 90 DAYS GROUP BY 1),
nps AS (SELECT unified_customer_id, max_by(score, response_ts) AS nps FROM ${catalog}.${silver}.surveys
        WHERE survey_type = 'nps' AND unified_customer_id IS NOT NULL GROUP BY 1),
tk AS (SELECT unified_customer_id, count_if(status IN ('open', 'pending')) AS open_t FROM ${catalog}.${silver}.service_tickets
       WHERE unified_customer_id IS NOT NULL GROUP BY 1),
rv AS (SELECT unified_customer_id, count(*) AS n, avg(rating) AS r FROM ${catalog}.${silver}.product_reviews
       WHERE unified_customer_id IS NOT NULL GROUP BY 1),
base AS (
  SELECT c.unified_customer_id, t.* EXCEPT (unified_customer_id), datediff((SELECT d FROM asof), t.last_order_date) AS dsl
  FROM ${catalog}.${silver}.customers c LEFT JOIN t_orders t USING (unified_customer_id)),
sc AS (
  SELECT *, CASE WHEN n_life IS NOT NULL THEN 6 - ntile(5) OVER (PARTITION BY n_life IS NOT NULL ORDER BY dsl) END AS r_s,
            CASE WHEN n_life IS NOT NULL THEN ntile(5) OVER (PARTITION BY n_life IS NOT NULL ORDER BY n_life) END AS f_s,
            CASE WHEN n_life IS NOT NULL THEN ntile(5) OVER (PARTITION BY n_life IS NOT NULL ORDER BY rev_life) END AS m_s
  FROM base)
SELECT sc.unified_customer_id, sc.first_order_date, sc.last_order_date, sc.dsl AS days_since_last_order,
       CAST(coalesce(sc.n_life, 0) AS INT) AS order_count_lifetime, CAST(coalesce(sc.n_12m, 0) AS INT) AS order_count_12m,
       coalesce(sc.rev_life, 0) AS revenue_lifetime, coalesce(sc.rev_12m, 0) AS revenue_12m,
       CAST(sc.rev_life / sc.n_life AS DECIMAL(12, 2)) AS aov_lifetime, CAST(coalesce(tl.margin_12m, 0) AS DECIMAL(14, 2)) AS margin_12m,
       sc.r_s AS recency_score, sc.f_s AS frequency_score, sc.m_s AS monetary_score,
       CASE WHEN sc.n_life IS NULL THEN 'no_purchase' WHEN sc.r_s >= 4 AND sc.f_s >= 4 THEN 'champions'
            WHEN sc.f_s >= 4 THEN 'loyal' WHEN sc.r_s >= 4 AND sc.f_s <= 2 THEN 'promising'
            WHEN sc.r_s <= 2 AND sc.f_s >= 3 THEN 'at_risk' WHEN sc.r_s <= 2 THEN 'hibernating' ELSE 'needs_attention' END AS rfm_segment,
       CASE WHEN sc.n_life IS NULL THEN 'prospect' WHEN sc.dsl > 365 THEN 'lapsed' WHEN sc.dsl > 180 THEN 'at_risk'
            WHEN datediff((SELECT d FROM asof), sc.first_order_date) <= 90 THEN 'new'
            WHEN sc.n_12m >= 5 THEN 'loyal' ELSE 'active' END AS lifecycle_stage,
       sc.preferred_channel, sc.preferred_store_id, tl.top_category, tl.top_sport, tl.category_affinity, tl.sport_affinity,
       round(sc.full_price_ratio, 3) AS full_price_ratio,
       CASE WHEN sc.full_price_ratio >= 0.8 THEN 'full_price' WHEN sc.full_price_ratio >= 0.45 THEN 'mixed'
            WHEN sc.full_price_ratio IS NOT NULL THEN 'deal_seeker' END AS discount_sensitivity,
       round(tl.return_rate, 3) AS return_rate, CAST(coalesce(tl.fit_return_count, 0) AS INT) AS fit_return_count,
       coalesce(tl.gifter, false) AS is_gifter, coalesce(sc.is_omnichannel, false) AS is_omnichannel,
       CAST(coalesce(tl.drops, 0) AS INT) AS limited_drop_purchases, CAST(coalesce(dig.sessions_30d, 0) AS INT) AS sessions_30d,
       CAST(coalesce(dig.pv_30d, 0) AS INT) AS product_views_30d,
       round(em.opens / nullif(em.sends, 0), 3) AS email_open_rate_90d, round(em.clicks / nullif(em.sends, 0), 3) AS email_click_rate_90d,
       round(coalesce(run.m30, 0), 1) AS run_miles_30d, round(coalesce(run.m90, 0), 1) AS run_miles_90d,
       race.race_date AS upcoming_race_date, race.race_km AS upcoming_race_distance_km,
       race.goal_date AS goal_race_date, race.goal_km AS goal_race_distance_km,
       CAST(coalesce(club.n, 0) AS INT) AS run_club_checkins_90d, nps.nps AS nps_latest,
       CAST(coalesce(tk.open_t, 0) AS INT) AS open_service_tickets, CAST(coalesce(rv.n, 0) AS INT) AS review_count,
       round(rv.r, 2) AS avg_review_rating, current_timestamp() AS updated_at
FROM sc LEFT JOIN t_lines tl USING (unified_customer_id) LEFT JOIN dig USING (unified_customer_id) LEFT JOIN em USING (unified_customer_id)
LEFT JOIN run USING (unified_customer_id) LEFT JOIN race USING (unified_customer_id) LEFT JOIN club USING (unified_customer_id)
LEFT JOIN nps USING (unified_customer_id) LEFT JOIN tk USING (unified_customer_id) LEFT JOIN rv USING (unified_customer_id)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10 - Identity resolution summary (appended each run)
# COMMAND ----------

run(r"""
INSERT INTO ${catalog}.${gold}.identity_resolution_stats BY NAME
WITH r AS (SELECT DISTINCT source_record_key, source_system, unified_customer_id FROM ${catalog}.${silver}.identity_graph),
cs AS (SELECT unified_customer_id, count(*) AS n FROM r GROUP BY 1),
src AS (SELECT r.source_system, avg(CASE WHEN cs.n > 1 THEN 1.0 ELSE 0 END) AS m FROM r JOIN cs USING (unified_customer_id) GROUP BY 1)
SELECT current_timestamp() AS run_ts,
       (SELECT count(*) FROM ${catalog}.${silver}.customer_source_records) AS source_records,
       (SELECT count(*) FROM ${catalog}.${silver}.customers) AS unified_customers,
       (SELECT count(*) FROM ${catalog}.${silver}.customers WHERE is_known) AS known_customers,
       (SELECT count(DISTINCT anonymous_id) FROM ${catalog}.${silver}.web_app_events WHERE unified_customer_id IS NULL) AS anonymous_profiles,
       (SELECT count(*) FROM ${catalog}.${silver}.customer_source_records) - (SELECT count(*) FROM ${catalog}.${silver}.customers) AS duplicates_collapsed,
       round(1 - (SELECT count(*) FROM ${catalog}.${silver}.customers) / (SELECT count(*) FROM ${catalog}.${silver}.customer_source_records), 4) AS dedup_rate,
       round((SELECT count(*) FROM ${catalog}.${silver}.identity_graph) / (SELECT count(*) FROM ${catalog}.${silver}.customers), 2) AS avg_ids_per_customer,
       (SELECT round(avg(CASE WHEN n > 1 THEN 1.0 ELSE 0 END), 4) FROM cs) AS multi_source_customer_pct,
       (SELECT count(*) FROM ${catalog}.${silver}.identity_match_pairs WHERE match_type = 'deterministic') AS deterministic_matches,
       (SELECT count(*) FROM ${catalog}.${silver}.identity_match_pairs WHERE match_type = 'probabilistic') AS probabilistic_matches,
       (SELECT map_from_entries(collect_list(struct(source_system, round(m, 4)))) FROM src) AS match_rate_by_source
""")
