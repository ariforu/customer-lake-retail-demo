# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 02 - Scores, recommendations and product feed
# MAGIC
# MAGIC **What it does:** Builds the product feed, simple rule-based scores (value, churn risk, shoe replacement date ...) and top-5 product picks per customer.
# MAGIC
# MAGIC - **Reads:** gold Customer 360 tables; silver products, prices, inventory, orders, reviews, shoe mileage
# MAGIC - **Writes:** `product_catalog_enriched`, `customer_predictions`, `customer_product_recommendations`
# MAGIC - **Runs in:** `strideforge_medallion` -> `gold_predictions_products`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Set the 'as of' date
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW asof AS
SELECT max(order_date) AS d, CAST(max(order_date) AS TIMESTAMP) + INTERVAL 1 DAY AS ts FROM ${catalog}.${silver}.orders
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Product feed (price, stock, rating), including discontinued products for past purchases
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.product_catalog_enriched BY NAME
WITH inv_dc AS (SELECT sku, sum(available_qty) AS q FROM ${catalog}.${silver}.inventory_daily
                WHERE location_type = 'dc' AND snapshot_date = (SELECT max(snapshot_date) FROM ${catalog}.${silver}.inventory_daily WHERE location_type = 'dc')
                GROUP BY 1),
inv_st AS (SELECT sku, sum(available_qty) AS q FROM ${catalog}.${silver}.inventory_daily WHERE location_type = 'store' GROUP BY 1),
px AS (SELECT sku, min_by(price, CASE price_type WHEN 'promo' THEN 1 WHEN 'markdown' THEN 2 ELSE 3 END) AS price,
              max(price_type <> 'list') AS md
       FROM ${catalog}.${silver}.price_history
       WHERE effective_from <= (SELECT d FROM asof) AND (effective_to IS NULL OR effective_to >= (SELECT d FROM asof)) GROUP BY 1),
rv AS (SELECT style_id, avg(rating) AS r, count(*) AS n, mode(ai_fit_feedback) AS fit FROM ${catalog}.${silver}.product_reviews GROUP BY 1)
SELECT k.sku, p.style_id, p.style_name, p.category, p.sport, p.gender, k.colorway, k.size, k.width,
       CAST(coalesce(px.price, p.msrp) AS DECIMAL(10, 2)) AS current_price, p.msrp, coalesce(px.md, false) AS is_on_markdown,
       CAST(coalesce(inv_dc.q, 0) AS INT) AS available_qty_online, CAST(coalesce(inv_st.q, 0) AS INT) AS available_qty_stores,
       CASE WHEN p.lifecycle_status = 'discontinued' THEN 'discontinued' WHEN coalesce(inv_dc.q, 0) = 0 THEN 'out_of_stock'
            WHEN inv_dc.q < 10 THEN 'low_stock' ELSE 'in_stock' END AS stock_status,
       round(rv.r, 2) AS avg_rating, CAST(coalesce(rv.n, 0) AS INT) AS review_count, rv.fit AS fit_feedback,
       p.launch_date > (SELECT d FROM asof) - INTERVAL 60 DAYS AS is_new_arrival, p.is_limited_drop, p.image_url,
       concat('https://www.strideforge.com/us/p/', lower(regexp_replace(p.style_name, '[^A-Za-z0-9]+', '-')), '/', p.style_id, '.html?sku=', k.sku) AS product_url,
       current_timestamp() AS updated_at
FROM ${catalog}.${silver}.skus k JOIN ${catalog}.${silver}.products p USING (style_id)
LEFT JOIN inv_dc USING (sku) LEFT JOIN inv_st USING (sku) LEFT JOIN px USING (sku) LEFT JOIN rv ON rv.style_id = p.style_id
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Scores: value, churn risk, shoe replacement date ...
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.customer_predictions BY NAME
WITH t AS (SELECT * FROM ${catalog}.${gold}.customer_traits),
shoe AS (SELECT unified_customer_id, max_by(total_miles, registered_at) AS miles, max_by(avg_weekly_miles, registered_at) AS wk
         FROM ${catalog}.${silver}.shoe_mileage WHERE is_active AND unified_customer_id IS NOT NULL GROUP BY 1),
runbuy AS (SELECT unified_customer_id, max(to_date(order_ts)) AS last_run_buy FROM ${catalog}.${silver}.order_lines
           WHERE category IN ('Running', 'Trail') AND unified_customer_id IS NOT NULL GROUP BY 1),
rs AS (SELECT l.unified_customer_id, max(n) AS max_sizes_one_style
       FROM (SELECT l.unified_customer_id, l.style_id, count(DISTINCT l.sku) AS n FROM ${catalog}.${silver}.order_lines l
             JOIN ${catalog}.${silver}.products p ON p.style_id = l.style_id AND p.is_limited_drop
             WHERE l.unified_customer_id IS NOT NULL GROUP BY 1, 2) l GROUP BY 1),
apps AS (SELECT unified_customer_id, count(*) AS n FROM ${catalog}.${silver}.sessions
         WHERE platform <> 'web' AND unified_customer_id IS NOT NULL GROUP BY 1),
opens AS (SELECT unified_customer_id, mode(hour(event_ts)) AS h, mode(date_format(event_ts, 'EEEE')) AS dname
          FROM ${catalog}.${silver}.message_events WHERE event_type = 'open' AND unified_customer_id IS NOT NULL GROUP BY 1),
s AS (
  SELECT t.*, shoe.miles, shoe.wk, runbuy.last_run_buy, coalesce(rs.max_sizes_one_style, 0) AS mss, coalesce(apps.n, 0) AS app_sessions,
         opens.h, opens.dname,
         1 / (1 + exp(-(-2.0 + 0.0065 * coalesce(t.days_since_last_order, 540) - 0.35 * ln(1 + t.order_count_12m)
                        + 0.45 * least(t.fit_return_count, 2) - 0.6 * CASE WHEN t.sessions_30d > 0 THEN 1 ELSE 0 END
                        + CASE WHEN t.order_count_lifetime = 0 THEN 1.5 ELSE 0 END))) AS churn
  FROM t LEFT JOIN shoe USING (unified_customer_id) LEFT JOIN runbuy USING (unified_customer_id) LEFT JOIN rs USING (unified_customer_id)
  LEFT JOIN apps USING (unified_customer_id) LEFT JOIN opens USING (unified_customer_id))
SELECT unified_customer_id,
       CAST(revenue_12m * (1 - churn) * 1.08 + CASE WHEN order_count_lifetime = 0 THEN 15 ELSE 0 END AS DECIMAL(14, 2)) AS predicted_clv_12m,
       CASE WHEN revenue_12m * (1 - churn) >= 900 THEN 'platinum' WHEN revenue_12m * (1 - churn) >= 350 THEN 'gold'
            WHEN revenue_12m * (1 - churn) >= 100 THEN 'silver' ELSE 'bronze' END AS clv_tier,
       round(churn, 4) AS churn_probability,
       CASE WHEN churn >= 0.7 THEN 'high' WHEN churn >= 0.4 THEN 'medium' ELSE 'low' END AS churn_risk_band,
       coalesce(try_element_at(map_keys(category_affinity), 2), top_category, 'Lifestyle') AS next_best_category,
       CAST(NULL AS STRING) AS next_best_style_id,
       round((1 - churn) * least(1.0, (order_count_12m + 0.5) / 12.0) * 1.4, 4) AS next_purchase_prob_30d,
       CASE WHEN miles IS NOT NULL AND wk > 0 THEN date_add((SELECT d FROM asof), CAST(greatest(0, (450 - miles) / wk * 7) AS INT))
            WHEN last_run_buy IS NOT NULL THEN date_add(last_run_buy, 180) END AS predicted_shoe_replacement_date,
       miles AS active_shoe_miles,
       round(least(1.0, 0.25 * limited_drop_purchases + 0.02 * least(app_sessions, 20)
                        + 0.3 * coalesce(sport_affinity['Lifestyle'], 0) + 0.2 * coalesce(sport_affinity['Basketball'], 0)), 3) AS drop_hype_score,
       round(least(1.0, CASE WHEN mss >= 4 THEN 0.9 WHEN mss >= 3 THEN 0.6 WHEN mss = 2 THEN 0.25 ELSE 0.02 END
                        + CASE WHEN limited_drop_purchases >= 8 THEN 0.1 ELSE 0 END), 3) AS reseller_risk_score,
       round(least(1.0, coalesce(return_rate, 0.15) * 1.6 + 0.08 * least(fit_return_count, 3)), 3) AS return_risk_score,
       CAST(coalesce(h, 19) AS INT) AS best_send_hour, coalesce(dname, 'Tuesday') AS best_send_day,
       round(least(1.0, 0.1 + coalesce(category_affinity['Apparel'], 0) * 1.5), 3) AS propensity_apparel,
       round(least(1.0, 0.03 + coalesce(category_affinity['Trail'], 0) * 2 + coalesce(category_affinity['Running'], 0) * 0.5), 3) AS propensity_trail,
       round(least(1.0, 0.03 + coalesce(category_affinity['Basketball'], 0) * 2), 3) AS propensity_basketball,
       round(least(1.0, coalesce(category_affinity['Kids Footwear'], 0) * 3 + coalesce(category_affinity['Kids Apparel'], 0) * 3), 3) AS propensity_kids,
       'heuristic_v1' AS model_version, current_timestamp() AS scored_at
FROM s
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Top-5 products per customer in their size
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${gold}.customer_product_recommendations BY NAME
WITH pop AS (
  SELECT l.style_id, p.category, p.gender, sum(l.quantity) AS units,
         row_number() OVER (PARTITION BY p.category, p.gender ORDER BY sum(l.quantity) DESC) AS rk
  FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.products p ON p.style_id = l.style_id
  WHERE l.order_ts > (SELECT ts FROM asof) - INTERVAL 90 DAYS AND NOT p.is_limited_drop AND p.lifecycle_status <> 'discontinued'
  GROUP BY 1, 2, 3),
cust AS (
  SELECT pr.unified_customer_id, pr.next_best_category AS cat, cp.gender AS g, cp.shoe_size_us, cp.apparel_top_size,
         pr.active_shoe_miles
  FROM ${catalog}.${gold}.customer_predictions pr JOIN ${catalog}.${gold}.customer_profile cp USING (unified_customer_id)
  WHERE cp.shoe_size_us IS NOT NULL OR cp.apparel_top_size IS NOT NULL),
cand AS (
  SELECT c.unified_customer_id, pop.style_id, pop.category, pop.units, pop.rk, c.shoe_size_us, c.apparel_top_size, c.active_shoe_miles
  FROM cust c JOIN pop ON pop.category = c.cat AND pop.rk <= 8
   AND pop.gender IN (CASE WHEN c.g = 'F' THEN 'Women' ELSE 'Men' END, 'Unisex')),
owned AS (SELECT DISTINCT unified_customer_id, style_id FROM ${catalog}.${silver}.order_lines WHERE unified_customer_id IS NOT NULL),
bis AS (SELECT DISTINCT unified_customer_id, sku FROM ${catalog}.${silver}.web_app_events
        WHERE event_type = 'back_in_stock_signup' AND unified_customer_id IS NOT NULL),
sk AS (
  SELECT cand.*, f.sku, f.stock_status, f.is_new_arrival, f.current_price
  FROM cand JOIN ${catalog}.${gold}.product_catalog_enriched f ON f.style_id = cand.style_id AND f.sku LIKE '%-01-%'
   AND (CASE WHEN f.category = 'Apparel' THEN f.size = cand.apparel_top_size ELSE try_cast(f.size AS DOUBLE) = cand.shoe_size_us END)
  LEFT ANTI JOIN owned o ON o.unified_customer_id = cand.unified_customer_id AND o.style_id = cand.style_id),
scored AS (
  SELECT sk.*, b.sku IS NOT NULL AS bis_signup,
         round((9 - rk) / 8.0 * CASE WHEN stock_status = 'out_of_stock' THEN 0.2 ELSE 1.0 END + CASE WHEN b.sku IS NOT NULL THEN 0.5 ELSE 0 END, 4) AS score
  FROM sk LEFT JOIN bis b ON b.unified_customer_id = sk.unified_customer_id AND b.sku = sk.sku)
SELECT unified_customer_id, CAST(row_number() OVER (PARTITION BY unified_customer_id ORDER BY score DESC, style_id) AS INT) AS rank,
       sku, style_id,
       CASE WHEN bis_signup THEN 'back_in_stock' WHEN category IN ('Running', 'Trail') AND active_shoe_miles >= 350 THEN 'replenish'
            WHEN is_new_arrival THEN 'new_arrival' ELSE 'cross_sell' END AS recommendation_type,
       score, stock_status <> 'out_of_stock' AS in_stock_in_size,
       concat('Top seller in ', category, ' in your size') AS reason, 'popularity_affinity_v1' AS model_version,
       current_timestamp() AS generated_at
FROM scored
QUALIFY row_number() OVER (PARTITION BY unified_customer_id ORDER BY score DESC, style_id) <= 5
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Add the #1 recommended style to the scores
# COMMAND ----------

run(r"""
MERGE INTO ${catalog}.${gold}.customer_predictions t
USING (SELECT unified_customer_id, style_id FROM ${catalog}.${gold}.customer_product_recommendations WHERE rank = 1) s
ON t.unified_customer_id = s.unified_customer_id
WHEN MATCHED THEN UPDATE SET t.next_best_style_id = s.style_id
""")
