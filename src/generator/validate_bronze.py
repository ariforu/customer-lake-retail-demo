# Databricks notebook source
# MAGIC %md
# MAGIC # Check the bronze data
# MAGIC
# MAGIC **What it does:** Prints row counts and checks that the demo stories are visible in the data (CloudStride 3 returns, Black Friday unsubscribes, reseller clusters, seasonality, worn-out shoes, rainy autumn).
# MAGIC
# MAGIC - **Reads:** `retail_bronze`
# MAGIC - **Writes:** - (report only)
# MAGIC - **Runs in:** `strideforge_bronze_setup` -> `validate_bronze`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

import os
import sys

os.environ.update(SF_CATALOG=P["catalog"], SF_BRONZE_SCHEMA=P["bronze"], SF_TRUTH_SCHEMA=P["truth"], SF_IN_JOB="1")
sys.path.insert(0, os.getcwd())
for m in [m for m in sys.modules if m in ("gen", "validate_stories") or m.startswith("gen.")]:
    del sys.modules[m]
import validate_stories

validate_stories.main()
