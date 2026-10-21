# Databricks notebook source
# MAGIC %md
# MAGIC # Shared parameters
# MAGIC
# MAGIC **What it does:** Creates the catalog and schema widgets, and a `run()` helper that fills them into SQL. Every other notebook includes it with `%run ../_params`.
# MAGIC
# MAGIC Defaults: catalog `customer_lake_si`, schemas `retail_bronze`, `retail_silver`, `retail_gold`, `retail_synthetic_truth`, `retail_cdp`.
# COMMAND ----------

from types import SimpleNamespace

PARAMS = [("catalog", "customer_lake_si"), ("bronze", "retail_bronze"), ("silver", "retail_silver"),
          ("gold", "retail_gold"), ("truth", "retail_synthetic_truth"), ("cdp", "retail_cdp")]
for _k, _d in PARAMS:
    dbutils.widgets.text(_k, _d)
P = {k: dbutils.widgets.get(k) for k, _ in PARAMS}
a = SimpleNamespace(**P)


def run(sql):
    """Substitute schema placeholders and execute one SQL statement."""
    for k, v in P.items():
        sql = sql.replace("${" + k + "}", v)
    return spark.sql(sql)


print(P)
