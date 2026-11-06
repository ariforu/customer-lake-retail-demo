# Databricks notebook source
# MAGIC %md
# MAGIC # Clean room module parameters
# MAGIC
# MAGIC **What it does:** Adds the clean room module's widgets to the shared parameters (`../../_params`): the `cleanroom` schema (default `retail_cleanroom`) and, for the save step, the clean room output catalog and schema of the latest run.
# COMMAND ----------

# MAGIC %run ../../_params

# COMMAND ----------

for _k, _d in [("cleanroom", "retail_cleanroom"), ("cr_output_catalog", ""), ("cr_output_schema", "")]:
    dbutils.widgets.text(_k, _d)
    P[_k] = dbutils.widgets.get(_k)
a.cleanroom = P["cleanroom"]
print(P)
