"""Job entry point: print bronze row counts and story checks (parameters: --catalog --bronze --truth)."""
import argparse
import os
import sys

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="customer_lake_si")
    ap.add_argument("--bronze", default="retail_bronze")
    ap.add_argument("--truth", default="retail_synthetic_truth")
    a = ap.parse_args()
    os.environ.update(SF_CATALOG=a.catalog, SF_BRONZE_SCHEMA=a.bronze, SF_TRUTH_SCHEMA=a.truth, SF_IN_JOB="1")
    sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0])))
    import validate_stories  # noqa: F401  (runs the checks on import)
