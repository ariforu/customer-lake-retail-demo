# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 07 - Support, surveys and reviews
# MAGIC
# MAGIC **What it does:** Links support tickets, CSAT/NPS surveys and product reviews to customers and adds simple sentiment and theme tags.
# MAGIC
# MAGIC - **Reads:** bronze Zendesk, Qualtrics and Bazaarvoice tables; `identity_graph`, `orders`
# MAGIC - **Writes:** `service_tickets`, `surveys`, `product_reviews`
# MAGIC - **Runs in:** `strideforge_medallion` -> `silver_service`
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
# MAGIC ### Step 2 - Support tickets with sentiment
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.service_tickets BY NAME
SELECT t.ticket_id, coalesce(m1.unified_customer_id, m2.unified_customer_id, o.unified_customer_id) AS unified_customer_id,
       t.channel, t.subject, t.description, t.category, t.priority, t.status, t.order_reference AS order_id,
       CAST(t.created_ts AS TIMESTAMP) AS created_at, CAST(t.resolved_ts AS TIMESTAMP) AS resolved_at,
       round((unix_timestamp(CAST(t.resolved_ts AS TIMESTAMP)) - unix_timestamp(CAST(t.created_ts AS TIMESTAMP))) / 3600.0, 2) AS resolution_hours,
       CASE WHEN lower(t.description) RLIKE 'never|still no|separating|came apart|damaged|locked|stain' THEN 'negative'
            WHEN t.category IN ('sizing_fit', 'promo_pricing', 'loyalty_points') THEN 'neutral' ELSE 'negative' END AS sentiment
FROM ${catalog}.${bronze}.service_tickets_raw t
LEFT JOIN idmap m1 ON m1.id_type = 'email' AND m1.id_value = ${catalog}.${silver}.norm_email(t.requester_email)
LEFT JOIN idmap m2 ON m2.id_type = 'phone' AND m2.id_value = ${catalog}.${silver}.norm_phone(t.requester_phone)
LEFT JOIN ${catalog}.${silver}.orders o ON o.order_id = t.order_reference
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - CSAT and NPS answers
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.surveys BY NAME
SELECT c.survey_response_id, m.unified_customer_id, 'csat' AS survey_type, 'support_ticket' AS touchpoint, c.ticket_id,
       c.csat_score AS score, CAST(NULL AS STRING) AS nps_category, c.comment,
       CASE WHEN c.csat_score >= 4 THEN 'positive' WHEN c.csat_score = 3 THEN 'neutral' ELSE 'negative' END AS sentiment,
       CAST(c.response_ts AS TIMESTAMP) AS response_ts
FROM ${catalog}.${bronze}.csat_surveys_raw c
LEFT JOIN idmap m ON m.id_type = 'email' AND m.id_value = ${catalog}.${silver}.norm_email(c.respondent_email)
UNION ALL
SELECT n.survey_response_id, m.unified_customer_id, 'nps', n.touchpoint, NULL, n.nps_score,
       CASE WHEN n.nps_score >= 9 THEN 'promoter' WHEN n.nps_score >= 7 THEN 'passive' ELSE 'detractor' END, n.comment,
       CASE WHEN n.nps_score >= 9 THEN 'positive' WHEN n.nps_score >= 7 THEN 'neutral' ELSE 'negative' END,
       CAST(n.response_ts AS TIMESTAMP)
FROM ${catalog}.${bronze}.nps_surveys_raw n
LEFT JOIN idmap m ON m.id_type = 'email' AND m.id_value = ${catalog}.${silver}.norm_email(n.respondent_email)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Reviews with sentiment, themes and fit feedback
# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${silver}.product_reviews BY NAME
SELECT r.review_id, m.unified_customer_id, r.sku, r.style_id, r.rating, r.title, r.review_text, r.fit_rating,
       r.verified_purchase, CAST(r.submitted_ts AS TIMESTAMP) AS submitted_at,
       CASE WHEN r.rating >= 4 THEN 'positive' WHEN r.rating = 3 THEN 'mixed' ELSE 'negative' END AS ai_sentiment,
       filter(array(
         CASE WHEN lower(r.review_text) RLIKE 'size|tight|small|big|narrow|roomy|slips' THEN 'fit' END,
         CASE WHEN lower(r.review_text) RLIKE 'comfort|cushion|blister|firm|soft|support|bouncy' THEN 'comfort' END,
         CASE WHEN lower(r.review_text) RLIKE 'wore|apart|holding up|durable|miles' THEN 'durability' END,
         CASE WHEN lower(r.review_text) RLIKE 'look|compliment|jeans|color' THEN 'style' END,
         CASE WHEN lower(r.review_text) RLIKE 'price|cheap|value' THEN 'price' END), x -> x IS NOT NULL) AS ai_themes,
       CASE WHEN lower(r.review_text) RLIKE 'half a size small|size up|tight|narrower|tighter' THEN 'runs_small'
            WHEN lower(r.review_text) RLIKE 'runs a little big|sizing down|heel slips' THEN 'runs_large'
            ELSE 'true_to_size' END AS ai_fit_feedback
FROM ${catalog}.${bronze}.product_reviews_raw r
LEFT JOIN idmap m ON m.id_type = 'email' AND m.id_value = ${catalog}.${silver}.norm_email(r.reviewer_email)
""")
