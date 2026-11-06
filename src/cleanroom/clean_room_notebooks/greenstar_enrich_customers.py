# Databricks notebook source
# MAGIC %md
# MAGIC # Greenstar Consumer Data - customer enrichment
# MAGIC
# MAGIC **Owner:** Greenstar Consumer Data (uploaded by the provider). **Runner:** StrideForge. Every collaborator must approve this notebook before it can run.
# MAGIC
# MAGIC **What it does:** Matches StrideForge customers to Greenstar people on the SHA-256 of the normalised email, then returns **summaries** of Greenstar's detailed attributes, plus the phone number where StrideForge has none.
# MAGIC
# MAGIC | Greenstar detail (never leaves the clean room) | Returned to StrideForge |
# MAGIC |---|---|
# MAGIC | `date_of_birth` | `generation`; `age_band` only when StrideForge has no birth date |
# MAGIC | `phone_number` | `phone_e164`, only when StrideForge has no phone |
# MAGIC | `household_income` | `income_band` |
# MAGIC | `home_ownership`, `home_value` | `homeowner` |
# MAGIC | `occupation_title` | `occupation_category` |
# MAGIC | `interest_scores` (20 scores) | `top_interests` (top 3 labels) |
# MAGIC | `marital_status`, `num_children`, age | `has_children`, `life_stage` |
# MAGIC | `person_id`, `email_sha256` | nothing (only StrideForge's own `unified_customer_id`) |
# MAGIC
# MAGIC Greenstar sees nothing: the output goes only to the runner's output catalog.
# COMMAND ----------

def param(name, default=None):
    """Clean room parameters are passed as widgets at run time; fall back to a default for local tests."""
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default


sf = param("cr_strideforge_input_catalog", "strideforge")
gs = param("cr_greenstar_input_catalog", "greenstar")
out = f"`{param('cr_output_catalog')}`.`{param('cr_output_schema')}`"
MIN_MATCHES = 100  # refuse tiny match lists, which could be used to probe individuals
print(sf, gs, out)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Match on hashed email

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW matched AS
SELECT k.unified_customer_id, k.has_phone, k.has_birth_date, g.*,
       floor(datediff(current_date(), g.date_of_birth) / 365.25) AS age
FROM `{sf}`.retail_cleanroom.cr_match_keys k
JOIN `{gs}`.consumer.person_attributes g ON k.email_sha256 = g.email_sha256
""")
n = spark.table("matched").count()
print(f"{n} matched customers")
assert n >= MIN_MATCHES, f"Only {n} matches; Greenstar requires at least {MIN_MATCHES}."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Summarise detailed attributes (Greenstar's logic)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {out}.customer_enrichment AS
WITH s AS (
  SELECT unified_customer_id, has_phone, has_birth_date,
    CASE WHEN date_of_birth IS NULL THEN NULL
         WHEN year(date_of_birth) >= 2013 THEN 'Gen Alpha' WHEN year(date_of_birth) >= 1997 THEN 'Gen Z'
         WHEN year(date_of_birth) >= 1981 THEN 'Millennial' WHEN year(date_of_birth) >= 1965 THEN 'Gen X'
         WHEN year(date_of_birth) >= 1946 THEN 'Baby Boomer' ELSE 'Silent' END AS generation,
    CASE WHEN has_birth_date OR date_of_birth IS NULL THEN NULL
         WHEN age < 25 THEN '16-24' WHEN age < 35 THEN '25-34' WHEN age < 45 THEN '35-44'
         WHEN age < 55 THEN '45-54' ELSE '55+' END AS age_band,
    CASE WHEN NOT has_phone THEN phone_number END AS phone_e164,
    CASE WHEN household_income < 50000 THEN '<50K' WHEN household_income < 100000 THEN '50-100K'
         WHEN household_income < 150000 THEN '100-150K' ELSE '150K+' END AS income_band,
    home_ownership = 'own' AS homeowner,
    CASE WHEN occupation_title IN ('Student', 'Retired') THEN occupation_title
         WHEN occupation_title IN ('Software Engineer', 'Accountant', 'Attorney', 'Marketing Manager', 'Financial Analyst',
                                   'Consultant', 'Graphic Designer', 'Operations Manager', 'Sales Representative') THEN 'Professional'
         WHEN occupation_title IN ('Physician', 'Registered Nurse', 'Teacher', 'Physical Therapist') THEN 'Healthcare & Education'
         WHEN occupation_title IN ('Electrician', 'Plumber', 'Carpenter') THEN 'Skilled Trades'
         WHEN occupation_title IN ('Retail Associate', 'Barista', 'Restaurant Server') THEN 'Service & Retail'
         WHEN occupation_title = 'Small Business Owner' THEN 'Business Owner' END AS occupation_category,
    slice(transform(
      array_sort(filter(map_entries(interest_scores), e -> e.value >= 0.6), (l, r) -> CASE WHEN l.value > r.value THEN -1 WHEN l.value < r.value THEN 1 ELSE 0 END),
      e -> coalesce(element_at(map('gym_fitness', 'Gym & Fitness', 'outdoor_camping', 'Outdoor & Camping', 'home_garden', 'Home & Garden',
                                    'health_wellness', 'Health & Wellness', 'finance_investing', 'Finance & Investing',
                                    'tech_gadgets', 'Tech & Gadgets'), e.key), initcap(replace(e.key, '_', ' ')))), 1, 3) AS top_interests,
    num_children > 0 AS has_children,
    CASE WHEN age >= 67 THEN 'Retiree'
         WHEN age >= 55 THEN 'Empty Nester'
         WHEN num_children > 0 AND age < 40 THEN 'Young Family'
         WHEN num_children > 0 THEN 'Established Family'
         WHEN marital_status = 'married' THEN 'Couple No Kids'
         WHEN age < 35 THEN 'Young Single'
         ELSE 'Single Adult' END AS life_stage
  FROM matched
)
SELECT unified_customer_id, generation, age_band, phone_e164, income_band, homeowner, occupation_category,
       top_interests, has_children, life_stage,
       filter(array(CASE WHEN phone_e164 IS NOT NULL THEN 'phone_e164' END,
                    CASE WHEN age_band IS NOT NULL THEN 'age_band' END), x -> x IS NOT NULL) AS fields_enriched,
       current_timestamp() AS enriched_at
FROM s
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Run summary (aggregates only)

# COMMAND ----------

summary = spark.sql(f"""
SELECT count(*) AS matched_customers,
       count_if(array_contains(fields_enriched, 'phone_e164')) AS phone_added,
       count_if(array_contains(fields_enriched, 'age_band')) AS age_band_added,
       count(generation) AS with_generation
FROM {out}.customer_enrichment
""")
summary.write.saveAsTable(f"{out}.enrichment_summary")
display(summary)
