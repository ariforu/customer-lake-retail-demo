# Databricks notebook source
# MAGIC %md
# MAGIC # Generate synthetic bronze data
# MAGIC
# MAGIC **What it does:** Creates realistic fake data for StrideForge (1.5M customers at scale 1.0) and loads the 42 bronze tables. Also saves the 'answer key' used to check identity resolution. Use `scale` = 0.01 for a quick test. The last stage, `cleanup`, drops the generator's scratch tables (remove it from `stages` if you want to re-run single stages).
# MAGIC
# MAGIC - **Reads:** -
# MAGIC - **Writes:** `retail_bronze` (all tables), `retail_synthetic_truth`
# MAGIC - **Runs in:** `strideforge_bronze_setup` -> `generate_bronze`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %pip install faker -q
# COMMAND ----------

# MAGIC %run ../_params
# COMMAND ----------

import importlib
import os
import sys
import time

dbutils.widgets.text("scale", "1.0")
dbutils.widgets.text("stages", "dims,persons,profiles,commerce,loyalty,digital,marketing,sport,service,ops,cleanup")
W = dict(P, scale=dbutils.widgets.get("scale"), stages=dbutils.widgets.get("stages"))
os.environ.update(SF_CATALOG=W["catalog"], SF_BRONZE_SCHEMA=W["bronze"], SF_TRUTH_SCHEMA=W["truth"],
                  SF_SCALE=W["scale"], SF_IN_JOB="1")
sys.path.insert(0, os.getcwd())          # notebook directory -> gen/ package + sf_session.py
for m in [m for m in sys.modules if m == "gen" or m.startswith("gen.")]:
    del sys.modules[m]                   # re-read widget-driven config on every run
from gen.config import N_PERSONS, get_spark

get_spark()
print(W, f"persons={N_PERSONS:,}")
# COMMAND ----------

for st in [s.strip() for s in W["stages"].split(",") if s.strip()]:
    t = time.time()
    importlib.import_module(f"gen.{st}").run()
    print(f"== stage {st} done in {time.time() - t:,.0f}s", flush=True)
