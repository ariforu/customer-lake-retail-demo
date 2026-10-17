"""Job entry point: generate the synthetic StrideForge bronze layer.

Parameters: --catalog --bronze --truth --scale [--stages dims,persons,...]"""
import argparse
import importlib
import os
import sys
import time

STAGES = ["dims", "persons", "profiles", "commerce", "loyalty", "digital", "marketing", "sport", "service", "ops"]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="customer_lake_si")
    ap.add_argument("--bronze", default="retail_bronze")
    ap.add_argument("--truth", default="retail_synthetic_truth")
    ap.add_argument("--scale", default="1.0")
    ap.add_argument("--stages", default=",".join(STAGES))
    a = ap.parse_args()
    os.environ.update(SF_CATALOG=a.catalog, SF_BRONZE_SCHEMA=a.bronze, SF_TRUTH_SCHEMA=a.truth, SF_SCALE=a.scale, SF_IN_JOB="1")
    here = os.path.dirname(os.path.abspath(sys.argv[0]))
    sys.path.insert(0, here)
    from gen.config import N_PERSONS, get_spark
    get_spark()
    print(f"catalog={a.catalog} scale={a.scale} persons={N_PERSONS:,}", flush=True)
    for st in [s.strip() for s in a.stages.split(",") if s.strip()]:
        t = time.time()
        importlib.import_module(f"gen.{st}").run()
        print(f"== stage {st} done in {time.time() - t:,.0f}s", flush=True)
