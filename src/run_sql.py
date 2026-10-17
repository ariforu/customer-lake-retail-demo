"""Run one or more SQL files statement-by-statement with ${placeholder} substitution.

Usage (job task parameters):
  run_sql.py --catalog C --bronze B --silver S --gold G --truth T  ddl/01_bronze.sql silver/01_reference.sql ...
File paths are relative to this script's directory."""
import argparse
import os
import re
import sys
import time

from pyspark.sql import SparkSession


def split_statements(text):
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", "\n".join(lines)) if s.strip()]


def main(argv):
    ap = argparse.ArgumentParser()
    for k, d in [("catalog", "customer_lake_si"), ("bronze", "retail_bronze"), ("silver", "retail_silver"),
                 ("gold", "retail_gold"), ("truth", "retail_synthetic_truth")]:
        ap.add_argument(f"--{k}", default=d)
    ap.add_argument("files", nargs="+")
    a = ap.parse_args(argv)
    here = os.path.dirname(os.path.abspath(sys.argv[0] if "__file__" not in globals() else __file__))
    subs = {k: getattr(a, k) for k in ["catalog", "bronze", "silver", "gold", "truth"]}
    spark = SparkSession.builder.getOrCreate()
    for f in a.files:
        sql = open(os.path.join(here, f)).read()
        for k, v in subs.items():
            sql = sql.replace("${" + k + "}", v)
        for i, stmt in enumerate(split_statements(sql), 1):
            t = time.time()
            head = " ".join(stmt.split())[:110]
            try:
                spark.sql(stmt)
            except Exception:
                print(f"FAILED {f} #{i}: {head}", flush=True)
                raise
            print(f"[{time.time() - t:6.1f}s] {f} #{i}: {head}", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
