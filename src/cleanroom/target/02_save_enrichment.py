# Databricks notebook source
# MAGIC %md
# MAGIC # Save the clean room output
# MAGIC
# MAGIC **What it does:** Copies `customer_enrichment` from the latest clean room run's output schema into `retail_cleanroom.customer_enrichment`. Clean room output tables are read-only and expire after 30 days; Customer Lake needs a stable table.
# MAGIC
# MAGIC - **Reads:** `<clean room output catalog>.<run output schema>.customer_enrichment` (widgets `cr_output_catalog`, `cr_output_schema`; the job passes them from the `enrich` task)
# MAGIC - **Writes:** `retail_cleanroom.customer_enrichment` (`INSERT OVERWRITE ... BY NAME`: each run is a full snapshot)
# MAGIC - **Runs in:** `strideforge_greenstar_enrichment` -> `save_enrichment`
# COMMAND ----------

# MAGIC %run ./_cr_params

# COMMAND ----------

assert P["cr_output_catalog"] and P["cr_output_schema"], "Set cr_output_catalog and cr_output_schema (the job passes them from the enrich task)."
src = f"`{P['cr_output_catalog']}`.`{P['cr_output_schema']}`.customer_enrichment"

run(f"""
INSERT OVERWRITE ${{catalog}}.${{cleanroom}}.customer_enrichment BY NAME
SELECT unified_customer_id, generation, age_band, phone_e164, income_band, homeowner, occupation_category,
       top_interests, has_children, life_stage, fields_enriched, enriched_at,
       '{P['cr_output_schema']}' AS clean_room_run_schema
FROM {src}
""")

# COMMAND ----------

display(run(r"""
SELECT count(*) AS matched_customers,
       count_if(array_contains(fields_enriched, 'phone_e164')) AS phone_added,
       count_if(array_contains(fields_enriched, 'age_band')) AS age_band_added,
       count(generation) AS with_generation
FROM ${catalog}.${cleanroom}.customer_enrichment
"""))
