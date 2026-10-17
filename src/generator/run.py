"""StrideForge bronze data generator.

Usage:  SF_SCALE=1.0 .venv/bin/python run.py [stage ...]
Stages run in dependency order; pass a subset to re-run selected stages."""
import importlib
import sys
import time
import warnings

warnings.filterwarnings("ignore")

STAGES = ["dims", "persons", "profiles", "commerce", "loyalty", "digital", "marketing", "sport", "service", "ops"]

if __name__ == "__main__":
    from gen.config import N_PERSONS, SCALE, get_spark
    todo = sys.argv[1:] or STAGES
    get_spark()
    print(f"SCALE={SCALE} persons={N_PERSONS:,} stages={todo}", flush=True)
    for st in todo:
        t = time.time()
        importlib.import_module(f"gen.{st}").run()
        print(f"== stage {st} done in {time.time() - t:,.0f}s", flush=True)
