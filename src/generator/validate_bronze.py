# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Row counts and story checks
# MAGIC
# MAGIC Verifies the business stories are present in bronze (CloudStride 3 returns, BF unsubscribe spike,
# MAGIC reseller clusters, seasonality, funnel, worn-out shoes, rainy autumn).
# COMMAND ----------

import os
import sys

for _k, _d in [("catalog", "customer_lake_si"), ("bronze", "retail_bronze"), ("truth", "retail_synthetic_truth")]:
    dbutils.widgets.text(_k, _d)
os.environ.update(SF_CATALOG=dbutils.widgets.get("catalog"), SF_BRONZE_SCHEMA=dbutils.widgets.get("bronze"),
                  SF_TRUTH_SCHEMA=dbutils.widgets.get("truth"), SF_IN_JOB="1")
sys.path.insert(0, os.getcwd())
import validate_stories  # noqa: F401  (prints the checks)
