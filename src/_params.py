# Databricks notebook source
# MAGIC %md
# MAGIC # Shared parameters
# MAGIC
# MAGIC Included by every StrideForge notebook via `%run ../_params`. Defines the schema widgets (filled from job
# MAGIC `base_parameters` or set interactively), the `P` dict, and `run(sql)`, which substitutes
# MAGIC `${catalog}` / `${bronze}` / `${silver}` / `${gold}` / `${truth}` and executes one statement.
# COMMAND ----------

from types import SimpleNamespace

PARAMS = [("catalog", "customer_lake_si"), ("bronze", "retail_bronze"), ("silver", "retail_silver"),
          ("gold", "retail_gold"), ("truth", "retail_synthetic_truth")]
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
