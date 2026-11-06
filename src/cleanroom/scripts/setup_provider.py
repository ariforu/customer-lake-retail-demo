"""One-time setup of the mock data provider (Greenstar Consumer Data) on the PROVIDER workspace.

1. Samples StrideForge customers on the TARGET workspace who allow profiling: ~15K with a phone or birth-date gap and
   ~5K with neither gap. Only the SHA-256 email hash, country and birth date leave the target, so that some provider
   records match. This is demo staging only; in real life the provider already has its own consumer file.
2. Writes the sample to <provider_catalog>.staging.seed_matches on the provider workspace.
3. Runs sql/greenstar_person_attributes.sql: ~30K extra non-matching people are added, every attribute is
   generated, and the staging schema is dropped.

Usage (from the repo root):
  python3 src/cleanroom/scripts/setup_provider.py \
      --target-profile fevm-vijay --target-catalog vijay_catalog \
      --provider-profile partner-demo-catalog --provider-catalog greenstar_demo
"""
import argparse
import os
import re

from _dbsql import sql, sql_file, warehouse_id

HERE = os.path.dirname(os.path.abspath(__file__))

SEED_QUERY = """
WITH p AS (
  SELECT sha2(lower(trim(c.email)), 256) AS email_sha256, coalesce(c.country, 'US') AS country, c.birth_date,
         c.phone_e164 IS NOT NULL AND c.birth_date IS NOT NULL AS no_gap
  FROM {cat}.{gold}.customer_profile c
  JOIN {cat}.{gold}.customer_consent k USING (unified_customer_id)
  WHERE c.email IS NOT NULL AND k.personalization_profiling AND NOT k.global_suppression
  QUALIFY count(*) OVER (PARTITION BY sha2(lower(trim(c.email)), 256)) = 1
)
(SELECT email_sha256, country, CAST(birth_date AS STRING) AS birth_date, 'matched_gap' AS segment
 FROM p WHERE NOT no_gap ORDER BY email_sha256 LIMIT {n_gap})
UNION ALL
(SELECT email_sha256, country, CAST(birth_date AS STRING), 'matched_full'
 FROM p WHERE no_gap ORDER BY email_sha256 LIMIT {n_full})
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-profile", default="fevm-vijay")
    ap.add_argument("--target-catalog", default="vijay_catalog")
    ap.add_argument("--target-gold", default="retail_gold")
    ap.add_argument("--provider-profile", default="partner-demo-catalog")
    ap.add_argument("--provider-catalog", default="greenstar_demo")
    ap.add_argument("--n-gap", type=int, default=15000)
    ap.add_argument("--n-full", type=int, default=5000)
    ap.add_argument("--n-synthetic", type=int, default=30000)
    a = ap.parse_args()

    twh, pwh = warehouse_id(a.target_profile), warehouse_id(a.provider_profile)

    print(f"[target] sampling {a.n_gap} gap + {a.n_full} full customers from {a.target_catalog}.{a.target_gold}")
    seed = sql(a.target_profile, twh, SEED_QUERY.format(cat=a.target_catalog, gold=a.target_gold,
                                                        n_gap=a.n_gap, n_full=a.n_full))
    print(f"[target] {len(seed)} seed rows")

    pc = a.provider_catalog
    sql(a.provider_profile, pwh, f"CREATE CATALOG IF NOT EXISTS {pc} COMMENT "
        "'Demo: mock third-party consumer data provider (Greenstar Consumer Data). Synthetic data.'")
    sql(a.provider_profile, pwh, f"CREATE SCHEMA IF NOT EXISTS {pc}.staging COMMENT 'Temporary seed for the demo; dropped after generation.'")
    sql(a.provider_profile, pwh, f"CREATE OR REPLACE TABLE {pc}.staging.seed_matches "
        "(email_sha256 STRING, country STRING, birth_date DATE, segment STRING)")

    # Values are validated (hex hash, country code, ISO date, fixed segment) before being inlined.
    hexre, datere = re.compile(r"^[0-9a-f]{64}$"), re.compile(r"^\d{4}-\d{2}-\d{2}$")
    rows = []
    for r in seed:
        assert hexre.match(r["email_sha256"]) and r["country"] in ("US", "UK", "DE", "CA") \
            and (r["birth_date"] is None or datere.match(r["birth_date"])) and r["segment"] in ("matched_gap", "matched_full")
        dob = f"DATE'{r['birth_date']}'" if r["birth_date"] else "NULL"
        rows.append(f"('{r['email_sha256']}','{r['country']}',{dob},'{r['segment']}')")
    for i in range(0, len(rows), 4000):
        sql(a.provider_profile, pwh, f"INSERT INTO {pc}.staging.seed_matches VALUES " + ",".join(rows[i:i + 4000]))
    print(f"[provider] wrote {len(rows)} rows to {pc}.staging.seed_matches")

    for stmt in sql_file(os.path.join(HERE, "..", "sql", "greenstar_person_attributes.sql"),
                         provider_catalog=pc, n_synthetic=a.n_synthetic):
        sql(a.provider_profile, pwh, stmt)

    stats = sql(a.provider_profile, pwh, f"""
      SELECT count(*) AS people, count(date_of_birth) AS with_dob, count(phone_number) AS with_phone,
             count_if(home_ownership = 'own') AS owners, round(avg(household_income)) AS avg_income
      FROM {pc}.consumer.person_attributes""")
    print(f"[provider] {pc}.consumer.person_attributes: {stats[0]}")


if __name__ == "__main__":
    main()
