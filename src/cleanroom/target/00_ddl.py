# Databricks notebook source
# MAGIC %md
# MAGIC # Clean room module: schema, tables and the Customer Lake view
# MAGIC
# MAGIC **What it does:** Creates the StrideForge side of the Greenstar clean room enrichment.
# MAGIC
# MAGIC - `retail_cleanroom.cr_match_keys`: the only StrideForge table shared into the clean room (hashed email, country, gap flags; profiling-consented customers only)
# MAGIC - `retail_cleanroom.customer_enrichment`: permanent copy of the clean room output (output tables expire after 30 days)
# MAGIC - `retail_cdp.customer_enrichment`: 1:1 view for Customer Lake, one row per customer, with first-party values preferred over Greenstar values
# MAGIC
# MAGIC - **Reads:** nothing (DDL only)
# MAGIC - **Runs in:** `strideforge_greenstar_enrichment` -> `ddl`
# MAGIC
# MAGIC Everything is created once (`IF NOT EXISTS`), so the clean room asset and the Customer Lake object keep their identity.
# COMMAND ----------

# MAGIC %run ./_cr_params

# COMMAND ----------

run(r"""
CREATE SCHEMA IF NOT EXISTS ${catalog}.${cleanroom}
COMMENT 'StrideForge CDP demo - clean room module: the match keys StrideForge shares into the Greenstar enrichment clean room and the saved enrichment output. Built by job strideforge_greenstar_enrichment.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `cr_match_keys` - what StrideForge shares into the clean room

# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${cleanroom}.cr_match_keys (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id (joins to retail_gold.customer_profile). Lets StrideForge map results back; meaningless to the provider.',
  email_sha256 STRING NOT NULL COMMENT 'SHA-256 hex of the lower-cased, trimmed best email. The only match key.',
  country STRING COMMENT 'Country code of the primary address (US, UK, DE, CA) or null.',
  has_phone BOOLEAN COMMENT 'StrideForge already has a phone number for the customer (the provider phone is only returned when false).',
  has_birth_date BOOLEAN COMMENT 'StrideForge already has a birth date for the customer.',
  updated_at TIMESTAMP COMMENT 'When the job last rebuilt this row.',
  CONSTRAINT pk_cr_match_keys PRIMARY KEY (unified_customer_id)
) COMMENT 'Clean room asset: the minimum StrideForge shares with the Greenstar enrichment clean room - hashed email, country and gap flags for customers who allow profiling (customer_consent.personalization_profiling) and are not globally suppressed. No names, raw emails, phones or birth dates. Built by job strideforge_greenstar_enrichment, task refresh_match_keys.'
CLUSTER BY (email_sha256)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `customer_enrichment` - saved clean room output

# COMMAND ----------

run(r"""
CREATE TABLE IF NOT EXISTS ${catalog}.${cleanroom}.customer_enrichment (
  unified_customer_id STRING NOT NULL COMMENT 'Unified customer id; joins to customer_profile.',
  generation STRING COMMENT 'Generation from the provider date of birth: Gen Alpha (2013+), Gen Z (1997-2012), Millennial (1981-1996), Gen X (1965-1980), Baby Boomer (1946-1964), Silent (before 1946).',
  age_band STRING COMMENT 'Age band from the provider date of birth, StrideForge bands (16-24, 25-34, 35-44, 45-54, 55+). Returned only when StrideForge has no birth date.',
  phone_e164 STRING COMMENT 'Provider mobile phone in E.164 format. Returned only when StrideForge has no phone.',
  income_band STRING COMMENT 'Household income band (USD-equivalent): <50K, 50-100K, 100-150K, 150K+.',
  homeowner BOOLEAN COMMENT 'True when the provider records the household as owning its home.',
  occupation_category STRING COMMENT 'Professional, Healthcare & Education, Skilled Trades, Service & Retail, Business Owner, Student or Retired.',
  top_interests ARRAY<STRING> COMMENT 'Up to 3 strongest interest labels (affinity >= 0.6), strongest first, e.g. Running, Trail Running, Health & Wellness.',
  has_children BOOLEAN COMMENT 'Children under 18 in the household.',
  life_stage STRING COMMENT 'Young Single, Young Family, Established Family, Couple No Kids, Single Adult, Empty Nester or Retiree.',
  fields_enriched ARRAY<STRING> COMMENT 'Which StrideForge gaps the provider filled: phone_e164, age_band.',
  clean_room_run_schema STRING COMMENT 'Clean room output schema of the run that produced the row (lineage back to the run).',
  enriched_at TIMESTAMP COMMENT 'When the clean room run produced the row.',
  CONSTRAINT pk_customer_enrichment PRIMARY KEY (unified_customer_id)
) COMMENT 'Greenstar Consumer Data enrichment for matched StrideForge customers, copied from the clean room output (which expires after 30 days). Summaries only, plus phone where StrideForge had none. Built by job strideforge_greenstar_enrichment, task save_enrichment.'
CLUSTER BY (unified_customer_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `retail_cdp.customer_enrichment` - Customer Lake view (1:1 with the customer profile)
# MAGIC
# MAGIC First-party values win: phone and age band come from StrideForge when it has them, otherwise from Greenstar, and `*_source` says which. Generation is computed from the StrideForge birth date when there is one. `generation_conflict` flags customers whose StrideForge and Greenstar generations differ.

# COMMAND ----------

run(r"""
CREATE VIEW IF NOT EXISTS ${catalog}.${cdp}.customer_enrichment (
  unified_customer_id COMMENT 'Unified customer id; joins to customer_profile.',
  greenstar_matched COMMENT 'True when the customer matched a Greenstar Consumer Data record in the clean room.',
  phone_e164 COMMENT 'Best phone: StrideForge phone, else the Greenstar phone.',
  phone_source COMMENT 'first_party, greenstar or null (no phone).',
  age_band COMMENT 'Best age band: StrideForge, else Greenstar (16-24, 25-34, 35-44, 45-54, 55+, unknown).',
  age_band_source COMMENT 'first_party, greenstar or null.',
  generation COMMENT 'Gen Alpha, Gen Z, Millennial, Gen X, Baby Boomer or Silent - from the StrideForge birth date, else Greenstar.',
  generation_source COMMENT 'first_party, greenstar or null.',
  generation_conflict COMMENT 'True when StrideForge and Greenstar both know the generation and disagree (first-party value is used).',
  income_band COMMENT 'Greenstar household income band (USD-equivalent): <50K, 50-100K, 100-150K, 150K+.',
  homeowner COMMENT 'Greenstar: household owns its home.',
  occupation_category COMMENT 'Greenstar occupation category.',
  top_interests COMMENT 'Greenstar top interest labels (up to 3).',
  has_children COMMENT 'Greenstar: children under 18 in the household.',
  life_stage COMMENT 'Greenstar life stage.',
  fields_enriched COMMENT 'StrideForge gaps Greenstar filled (phone_e164, age_band); empty when none.',
  enriched_at COMMENT 'When the clean room run produced the enrichment.'
)
COMMENT 'Customer Lake view: 1:1 related object for entity "Retail - StrideForge Customer" (one row per customer) with Greenstar Consumer Data enrichment from the Databricks clean room strideforge_greenstar_enrichment. First-party values win; *_source columns show where each value came from. Reads retail_gold.customer_profile and retail_cleanroom.customer_enrichment; created by notebook cleanroom/target/00_ddl.'
TBLPROPERTIES ('cdp_activation' = 'true')
AS
SELECT
  unified_customer_id,
  e.enriched_at IS NOT NULL AS greenstar_matched,
  coalesce(p.phone_e164, e.phone_e164) AS phone_e164,
  CASE WHEN p.phone_e164 IS NOT NULL THEN 'first_party' WHEN e.phone_e164 IS NOT NULL THEN 'greenstar' END AS phone_source,
  CASE WHEN p.birth_date IS NOT NULL THEN p.age_band ELSE coalesce(e.age_band, p.age_band) END AS age_band,
  CASE WHEN p.birth_date IS NOT NULL THEN 'first_party' WHEN e.age_band IS NOT NULL THEN 'greenstar' END AS age_band_source,
  coalesce(p.fp_generation, e.generation) AS generation,
  CASE WHEN p.fp_generation IS NOT NULL THEN 'first_party' WHEN e.generation IS NOT NULL THEN 'greenstar' END AS generation_source,
  coalesce(p.fp_generation <> e.generation, false) AS generation_conflict,
  e.income_band, e.homeowner, e.occupation_category, e.top_interests, e.has_children, e.life_stage,
  coalesce(e.fields_enriched, array()) AS fields_enriched,
  e.enriched_at
FROM (
  SELECT unified_customer_id, phone_e164, birth_date, age_band,
         CASE WHEN birth_date IS NULL THEN NULL
              WHEN year(birth_date) >= 2013 THEN 'Gen Alpha' WHEN year(birth_date) >= 1997 THEN 'Gen Z'
              WHEN year(birth_date) >= 1981 THEN 'Millennial' WHEN year(birth_date) >= 1965 THEN 'Gen X'
              WHEN year(birth_date) >= 1946 THEN 'Baby Boomer' ELSE 'Silent' END AS fp_generation
  FROM ${catalog}.${gold}.customer_profile
) p
LEFT JOIN ${catalog}.${cleanroom}.customer_enrichment e USING (unified_customer_id)
""")
