# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 07b - AI Functions review enrichment
# MAGIC
# MAGIC AI Functions enrichment on the most recent reviews (bounded sample keeps cost and latency predictable).
# MAGIC Overwrites the rule-based ai_* columns for those rows.
# MAGIC materialize once so the AI calls are not re-evaluated by MERGE
# MAGIC
# MAGIC _Generated from `07b_ai_enrichment.sql`; parameters come from widgets / job base_parameters._
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
# MAGIC ### Create `_ai_reviews`
# COMMAND ----------

run(r"""
CREATE OR REPLACE TABLE ${catalog}.${silver}._ai_reviews AS
SELECT review_id,
       ai_analyze_sentiment(review_text) AS ai_sentiment,
       ai_classify(review_text, ARRAY('runs_small', 'true_to_size', 'runs_large')) AS ai_fit_feedback,
       ai_classify(review_text, ARRAY('fit', 'comfort', 'durability', 'style', 'price')) AS main_theme
FROM (SELECT review_id, review_text FROM ${catalog}.${silver}.product_reviews ORDER BY submitted_at DESC LIMIT 20000)
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge `product_reviews`
# COMMAND ----------

run(r"""
MERGE INTO ${catalog}.${silver}.product_reviews t
USING ${catalog}.${silver}._ai_reviews s ON t.review_id = s.review_id
WHEN MATCHED THEN UPDATE SET
  t.ai_sentiment = s.ai_sentiment,
  t.ai_fit_feedback = s.ai_fit_feedback,
  t.ai_themes = array_distinct(concat(array(s.main_theme), coalesce(t.ai_themes, array())))
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop `_ai_reviews`
# COMMAND ----------

run(r"""
DROP TABLE IF EXISTS ${catalog}.${silver}._ai_reviews
""")
