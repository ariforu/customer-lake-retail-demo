# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 06 - Marketing
# MAGIC
# MAGIC Silver marketing: campaigns, owned-channel message events, paid media, affiliates, touchpoints
# MAGIC
# MAGIC _Generated from `06_marketing.sql`; parameters come from widgets / job base_parameters._
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
# MAGIC ### Insert `campaigns`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.campaigns BY NAME
WITH esp AS (
  SELECT campaign_id, first(campaign_name) AS campaign_name, first(channel) AS channel,
         to_date(min(CAST(send_ts AS TIMESTAMP))) AS start_date, to_date(max(CAST(send_ts AS TIMESTAMP))) AS end_date
  FROM ${catalog}.${bronze}.esp_sends_raw GROUP BY campaign_id),
paid AS (
  SELECT campaign_id, first(campaign_name) AS campaign_name, first(platform) AS channel, to_date(min(report_date)) AS start_date,
         to_date(max(report_date)) AS end_date, CAST(sum(spend) AS DECIMAL(14, 2)) AS budget
  FROM ${catalog}.${bronze}.paid_media_daily_raw GROUP BY campaign_id)
SELECT campaign_id, campaign_name,
       CASE WHEN campaign_id LIKE 'TRG-WELCOME%' THEN 'onboarding' WHEN campaign_id LIKE 'TRG-WINBACK%' THEN 'win_back'
            WHEN campaign_id LIKE 'TRG-REPLENISH%' THEN 'replenishment' WHEN campaign_id LIKE 'TRG-%' THEN 'retention'
            WHEN lower(campaign_name) RLIKE 'black friday|sale|flash' THEN 'promotion'
            WHEN lower(campaign_name) RLIKE 'drop|launch|is here|teaser|reviews are in' THEN 'launch'
            ELSE 'engagement' END AS objective,
       CASE WHEN campaign_id LIKE 'TRG-%' THEN 'triggered' ELSE 'batch' END AS campaign_type, channel,
       CAST(NULL AS STRING) AS segment_id,
       CASE WHEN lower(campaign_name) RLIKE 'black friday|sale' THEN 'percent_off' WHEN campaign_id = 'TRG-BIRTHDAY' THEN 'points'
            WHEN lower(campaign_name) LIKE '%double points%' THEN 'points_multiplier' ELSE 'none' END AS offer_type,
       CASE WHEN lower(campaign_name) RLIKE 'black friday' THEN '25%' WHEN campaign_id = 'TRG-BIRTHDAY' THEN '200 pts'
            WHEN lower(campaign_name) LIKE '%double points%' THEN '2x' END AS offer_value,
       start_date, end_date, CAST(NULL AS DECIMAL(14, 2)) AS budget, false AS has_holdout, CAST(NULL AS DOUBLE) AS holdout_pct,
       'crm_team' AS owner
FROM esp
UNION ALL
SELECT campaign_id, campaign_name,
       CASE WHEN lower(campaign_name) LIKE '%retargeting%' THEN 'retargeting' WHEN lower(campaign_name) LIKE '%prospecting%' THEN 'acquisition'
            ELSE 'conversion' END, 'paid', channel, NULL, 'none', NULL, start_date, end_date, budget, false, NULL, 'performance_marketing'
FROM paid
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert `message_events`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.message_events BY NAME
WITH s AS (
  SELECT s.send_id, s.campaign_id, s.channel, s.variant, CAST(s.send_ts AS TIMESTAMP) AS send_ts,
         coalesce(m1.unified_customer_id, m2.unified_customer_id, m3.unified_customer_id) AS unified_customer_id
  FROM ${catalog}.${bronze}.esp_sends_raw s
  LEFT JOIN idmap m1 ON s.channel = 'email' AND m1.id_type = 'email' AND m1.id_value = ${catalog}.${silver}.norm_email(s.recipient_email)
  LEFT JOIN idmap m2 ON s.channel = 'sms' AND m2.id_type = 'phone' AND m2.id_value = ${catalog}.${silver}.norm_phone(s.recipient_phone)
  LEFT JOIN idmap m3 ON s.channel = 'push' AND m3.id_type = 'push_token' AND m3.id_value = s.push_token)
SELECT send_id AS message_event_id, send_id, campaign_id, unified_customer_id, channel, 'send' AS event_type, send_ts AS event_ts,
       variant, CAST(NULL AS STRING) AS link_url, CAST(NULL AS STRING) AS device_type
FROM s
UNION ALL
SELECT e.engagement_id, e.send_id, e.campaign_id, s.unified_customer_id, e.channel, e.event_type, CAST(e.event_ts AS TIMESTAMP),
       s.variant, e.link_url, e.device_type
FROM ${catalog}.${bronze}.esp_engagement_raw e LEFT JOIN s USING (send_id)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert `paid_media_performance`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.paid_media_performance BY NAME
SELECT platform, campaign_id, ad_group_id, ad_id, audience_name, to_date(report_date) AS report_date, impressions, clicks, spend,
       video_views, platform_conversions, platform_conversion_value
FROM ${catalog}.${bronze}.paid_media_daily_raw
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert `affiliate_redemptions`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.affiliate_redemptions BY NAME
SELECT r.redemption_id, r.promo_code, c.partner_id, c.partner_name, c.partner_type, r.order_id, o.unified_customer_id,
       CAST(r.redemption_ts AS TIMESTAMP) AS redemption_ts, r.order_amount, r.commission_amount
FROM ${catalog}.${bronze}.affiliate_redemptions_raw r
LEFT JOIN ${catalog}.${bronze}.affiliate_codes_raw c USING (promo_code)
LEFT JOIN ${catalog}.${silver}.orders o USING (order_id)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert `marketing_touchpoints`
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.marketing_touchpoints BY NAME
SELECT md5(concat_ws('|', message_event_id, event_type)) AS touchpoint_id, unified_customer_id, event_ts AS touchpoint_ts, channel,
       campaign_id, event_type AS touchpoint_type, 'braze' AS source, channel AS medium
FROM ${catalog}.${silver}.message_events WHERE event_type IN ('open', 'click') AND unified_customer_id IS NOT NULL
UNION ALL
SELECT md5(concat('visit|', session_id)), unified_customer_id, session_start_ts,
       CASE WHEN traffic_source IN ('meta', 'tiktok', 'instagram') THEN 'paid_social' WHEN traffic_source = 'google' THEN 'search'
            ELSE traffic_source END, utm_campaign, 'visit', traffic_source, platform
FROM ${catalog}.${silver}.sessions WHERE unified_customer_id IS NOT NULL AND traffic_source <> 'direct'
UNION ALL
SELECT md5(concat('paid|', c.conversion_id)), m.unified_customer_id, CAST(c.conversion_ts AS TIMESTAMP) - INTERVAL 2 HOURS,
       c.platform, c.campaign_id, CASE WHEN c.click_id IS NULL THEN 'impression' ELSE 'click' END, c.platform, 'paid'
FROM ${catalog}.${bronze}.paid_media_conversions_raw c
JOIN (SELECT sha2(lower(trim(r.email)), 256) AS h, min(g.unified_customer_id) AS unified_customer_id
      FROM ${catalog}.${silver}.customer_source_records r
      JOIN (SELECT DISTINCT source_record_key, unified_customer_id FROM ${catalog}.${silver}.identity_graph) g USING (source_record_key)
      WHERE r.email IS NOT NULL GROUP BY 1) m ON m.h = c.hashed_email
""")
