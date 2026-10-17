-- AI Functions enrichment on the most recent reviews (bounded sample keeps cost and latency predictable).
-- Overwrites the rule-based ai_* columns for those rows.

-- materialize once so the AI calls are not re-evaluated by MERGE
CREATE OR REPLACE TABLE ${catalog}.${silver}._ai_reviews AS
SELECT review_id,
       ai_analyze_sentiment(review_text) AS ai_sentiment,
       ai_classify(review_text, ARRAY('runs_small', 'true_to_size', 'runs_large')) AS ai_fit_feedback,
       ai_classify(review_text, ARRAY('fit', 'comfort', 'durability', 'style', 'price')) AS main_theme
FROM (SELECT review_id, review_text FROM ${catalog}.${silver}.product_reviews ORDER BY submitted_at DESC LIMIT 20000);

MERGE INTO ${catalog}.${silver}.product_reviews t
USING ${catalog}.${silver}._ai_reviews s ON t.review_id = s.review_id
WHEN MATCHED THEN UPDATE SET
  t.ai_sentiment = s.ai_sentiment,
  t.ai_fit_feedback = s.ai_fit_feedback,
  t.ai_themes = array_distinct(concat(array(s.main_theme), coalesce(t.ai_themes, array())));

DROP TABLE IF EXISTS ${catalog}.${silver}._ai_reviews;
