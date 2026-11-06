# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh the clean room match keys
# MAGIC
# MAGIC **What it does:** Rebuilds `retail_cleanroom.cr_match_keys`, the only StrideForge table shared into the Greenstar clean room. The clean room reads it live through Delta Sharing, so the next clean room run sees the refreshed keys.
# MAGIC
# MAGIC - **Reads:** `retail_gold.customer_profile`, `retail_gold.customer_consent`
# MAGIC - **Writes:** `retail_cleanroom.cr_match_keys` (`INSERT OVERWRITE`, keeps the table identity the clean room depends on)
# MAGIC - **Runs in:** `strideforge_greenstar_enrichment` -> `refresh_match_keys`
# MAGIC
# MAGIC Only customers who allow profiling and are not globally suppressed are shared. Emails that belong to more than one customer are left out, so a match always maps to exactly one customer.
# COMMAND ----------

# MAGIC %run ./_cr_params

# COMMAND ----------

run(r"""
INSERT OVERWRITE ${catalog}.${cleanroom}.cr_match_keys
SELECT c.unified_customer_id,
       sha2(lower(trim(c.email)), 256) AS email_sha256,
       c.country,
       c.phone_e164 IS NOT NULL AS has_phone,
       c.birth_date IS NOT NULL AS has_birth_date,
       current_timestamp() AS updated_at
FROM ${catalog}.${gold}.customer_profile c
JOIN ${catalog}.${gold}.customer_consent k USING (unified_customer_id)
WHERE c.email IS NOT NULL AND k.personalization_profiling AND NOT k.global_suppression
QUALIFY count(*) OVER (PARTITION BY sha2(lower(trim(c.email)), 256)) = 1
""")
display(run(r"""
SELECT count(*) AS shared_customers, count_if(NOT has_phone) AS missing_phone, count_if(NOT has_birth_date) AS missing_birth_date
FROM ${catalog}.${cleanroom}.cr_match_keys
"""))
