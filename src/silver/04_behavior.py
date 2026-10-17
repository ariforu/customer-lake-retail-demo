# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 04 - Digital behaviour
# MAGIC
# MAGIC Silver digital behaviour: unified web + app events with identity stitching, sessionized
# MAGIC
# MAGIC _Generated from `04_behavior.sql`; parameters come from widgets / job base_parameters._
# COMMAND ----------

for _k, _d in [('catalog', 'customer_lake_si'), ('bronze', 'retail_bronze'), ('silver', 'retail_silver'), ('gold', 'retail_gold'), ('truth', 'retail_synthetic_truth')]:
    dbutils.widgets.text(_k, _d)
P = {k: dbutils.widgets.get(k) for k, _ in [('catalog', 'customer_lake_si'), ('bronze', 'retail_bronze'), ('silver', 'retail_silver'), ('gold', 'retail_gold'), ('truth', 'retail_synthetic_truth')]}


def run(sql):
    """Substitute ${catalog} / ${bronze} / ${silver} / ${gold} / ${truth} and execute one statement."""
    for k, v in P.items():
        sql = sql.replace("${" + k + "}", v)
    return spark.sql(sql)


print(P)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `idmap`
# COMMAND ----------

run(r"""
CREATE OR REPLACE TEMP VIEW idmap AS
SELECT id_type, id_value, min(unified_customer_id) AS unified_customer_id
FROM ${catalog}.${silver}.identity_graph GROUP BY id_type, id_value
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert `web_app_events`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.web_app_events BY NAME
WITH w AS (
  SELECT c.event_id, coalesce(m1.unified_customer_id, m2.unified_customer_id) AS unified_customer_id, c.cookie_id AS anonymous_id,
         c.session_id, 'web' AS platform, c.event_type, CAST(c.event_ts AS TIMESTAMP) AS event_ts, c.page_type, c.sku,
         c.search_term, c.selected_size, c.utm_source, c.utm_medium, c.utm_campaign, c.device_type,
         c.ip_geo_region AS geo_region, c.ip_geo_postal_code AS geo_postal_code
  FROM ${catalog}.${bronze}.web_clickstream_raw c
  LEFT JOIN idmap m1 ON m1.id_type = 'ecom_id' AND m1.id_value = c.ecom_customer_id
  LEFT JOIN idmap m2 ON m2.id_type = 'cookie_id' AND m2.id_value = c.cookie_id),
a AS (
  SELECT e.event_id, coalesce(m1.unified_customer_id, m2.unified_customer_id) AS unified_customer_id, e.device_id AS anonymous_id,
         e.session_id, e.platform, e.event_name AS event_type, CAST(e.event_ts AS TIMESTAMP) AS event_ts, e.screen_name AS page_type,
         e.sku, get_json_object(e.properties, '$.search_term') AS search_term, get_json_object(e.properties, '$.size') AS selected_size,
         get_json_object(e.properties, '$.source') AS utm_source,
         CASE WHEN get_json_object(e.properties, '$.source') = 'braze' THEN 'push' END AS utm_medium,
         get_json_object(e.properties, '$.campaign') AS utm_campaign, e.platform AS device_type,
         CAST(NULL AS STRING) AS geo_region, CAST(NULL AS STRING) AS geo_postal_code
  FROM ${catalog}.${bronze}.app_events_raw e
  LEFT JOIN idmap m1 ON m1.id_type = 'app_id' AND m1.id_value = e.app_user_id
  LEFT JOIN idmap m2 ON m2.id_type = 'device_id' AND m2.id_value = e.device_id)
SELECT x.*, to_date(x.event_ts) AS event_date, split_part(x.sku, '-', 1) AS style_id, p.category
FROM (SELECT * FROM w UNION ALL SELECT * FROM a) x
LEFT JOIN ${catalog}.${silver}.products p ON p.style_id = split_part(x.sku, '-', 1)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert `sessions`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.sessions BY NAME
SELECT session_id, max(unified_customer_id) AS unified_customer_id, first(anonymous_id) AS anonymous_id, first(platform) AS platform,
       min(event_ts) AS session_start_ts, max(event_ts) AS session_end_ts,
       CAST(unix_timestamp(max(event_ts)) - unix_timestamp(min(event_ts)) AS INT) AS duration_seconds,
       CAST(count_if(event_type IN ('page_view', 'product_view', 'screen_view', 'app_open')) AS INT) AS page_views,
       CAST(count_if(event_type = 'product_view') AS INT) AS product_views,
       CAST(count_if(event_type = 'add_to_cart') AS INT) AS add_to_carts, max(event_type = 'purchase') AS converted,
       coalesce(min_by(utm_source, event_ts), 'direct') AS traffic_source, min_by(utm_campaign, event_ts) AS utm_campaign,
       min_by(coalesce(page_type, event_type), event_ts) AS landing_page
FROM ${catalog}.${silver}.web_app_events GROUP BY session_id
""")
