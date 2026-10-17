# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Row counts and story checks
# MAGIC
# MAGIC Verifies the business stories are present in bronze (CloudStride 3 returns, BF unsubscribe spike,
# MAGIC reseller clusters, seasonality, funnel, worn-out shoes, rainy autumn).
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
