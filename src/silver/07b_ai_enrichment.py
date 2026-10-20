# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 07b - AI review tagging
# MAGIC
# MAGIC **What it does:** Uses Databricks AI Functions to tag sentiment, fit feedback and main theme for the 20,000 newest reviews (replaces the simple rule-based tags for those rows).
# MAGIC
# MAGIC - **Reads:** `product_reviews`
# MAGIC - **Writes:** `product_reviews` (updated)
# MAGIC - **Runs in:** `strideforge_medallion` -> `silver_ai_review_enrichment`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Run AI Functions on the 20K newest reviews (saved once)
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
# MAGIC ### Step 2 - Write the AI tags back to the reviews table
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
# MAGIC ### Step 3 - Remove the temporary table
# COMMAND ----------

run(r"""
DROP TABLE IF EXISTS ${catalog}.${silver}._ai_reviews
""")
