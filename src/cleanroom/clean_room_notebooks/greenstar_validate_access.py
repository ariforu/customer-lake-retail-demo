# Databricks notebook source
# MAGIC %md
# MAGIC # Greenstar clean room - validate access
# MAGIC
# MAGIC **Owner:** Greenstar Consumer Data (uploaded by the provider). **Runner:** StrideForge.
# MAGIC
# MAGIC Smoke test before the real analysis: proves the clean room's serverless compute can read both parties' shared tables (including StrideForge's own S3 storage) and returns only aggregate counts. No row-level data is written.
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
print(sf, gs, out)

# COMMAND ----------

counts = spark.sql(f"""
SELECT (SELECT count(*) FROM `{sf}`.retail_cleanroom.cr_match_keys) AS strideforge_keys,
       (SELECT count(*) FROM `{gs}`.consumer.person_attributes) AS greenstar_people
""")
display(counts)
counts.write.saveAsTable(f"{out}.access_check")
