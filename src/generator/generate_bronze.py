# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Synthetic StrideForge data generator
# MAGIC
# MAGIC Generates the 42 bronze tables (1.5M customers at `scale = 1.0`, ~274M rows) plus the
# MAGIC `retail_synthetic_truth` answer key. Library code lives in the `gen/` package next to this notebook.
# MAGIC Stages: dims, persons, profiles, commerce, loyalty, digital, marketing, sport, service, ops.
# MAGIC
# MAGIC _Interactive use: if you edit files under `gen/`, detach & re-attach so Python workers pick up the new code._
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
dbutils.widgets.text("stages", "dims,persons,profiles,commerce,loyalty,digital,marketing,sport,service,ops")
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
