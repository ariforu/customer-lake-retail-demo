"""Final generator stage: drop scratch tables, keeping only the ground truth needed for validation."""
from gen.config import TRUTH, get_spark

KEEP = {"persons", "identity_answer_key_profiles", "identity_resolution_eval"}


def run():
    spark = get_spark()
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {TRUTH}").collect()]
    for t in sorted(set(tables) - KEEP):
        spark.sql(f"DROP TABLE IF EXISTS {TRUTH}.{t}")
        print(f"  dropped scratch table {TRUTH}.{t}", flush=True)
