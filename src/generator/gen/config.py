"""Shared constants, Spark session and write helpers for the StrideForge bronze generator."""
import hashlib
import os
from datetime import date, timedelta

import numpy as np

CATALOG = os.environ.get("SF_CATALOG", "customer_lake_si")
BRONZE = f"{CATALOG}.{os.environ.get('SF_BRONZE_SCHEMA', 'retail_bronze')}"
TRUTH = f"{CATALOG}.{os.environ.get('SF_TRUTH_SCHEMA', 'retail_synthetic_truth')}"   # generator scratch + ground truth
PROFILE = os.environ.get("SF_PROFILE", "arijit_fevm")

SCALE = float(os.environ.get("SF_SCALE", "1.0"))
N_PERSONS = max(3000, int(1_500_000 * SCALE))
N_ANON_POS = max(1000, int(3_000_000 * SCALE))
N_ANON_SESSIONS = max(1000, int(6_500_000 * SCALE))
SEED = 20261016

START = date(2023, 10, 1)
END = date(2026, 10, 15)
N_DAYS = (END - START).days + 1
WEB_START = END - timedelta(days=182)        # web/app analytics retention window
ESP_START = END - timedelta(days=365)        # ESP export retention window
CS3_LAUNCH = date(2026, 3, 5)                # CloudStride 3 launch (runs ~half size small)
CS3_FIX = date(2026, 7, 15)                  # size guidance added to PDP
BF_MAIL_START, BF_MAIL_END = date(2025, 11, 20), date(2025, 12, 2)   # over-mailing window
WET_AUTUMN = (date(2025, 9, 15), date(2025, 12, 15))                 # rainy PNW / UK autumn

COUNTRIES = ["US", "UK", "DE", "CA"]
COUNTRY_W = [0.70, 0.12, 0.10, 0.08]
CURRENCY = {"US": "USD", "CA": "CAD", "UK": "GBP", "DE": "EUR"}
FX = {"US": 1.0, "CA": 1.35, "UK": 0.80, "DE": 0.93}
TAX = {"US": 0.0725, "CA": 0.13, "UK": 0.20, "DE": 0.19}
FREE_SHIP = {"US": 75, "CA": 99, "UK": 50, "DE": 60}
SHIP_FEE = {"US": 7.95, "CA": 9.95, "UK": 4.99, "DE": 4.95}


def di(d):
    """date -> day index from START"""
    return (d - START).days


def black_friday(year):
    d = date(year, 11, 1)
    first_thu = d + timedelta(days=(3 - d.weekday()) % 7)
    return first_thu + timedelta(days=22)


BF_DAYS = [black_friday(y) for y in (2023, 2024, 2025)]


def calendar_weights():
    """Daily demand multiplier: growth, weekday, seasonality, BF/CM, holidays."""
    w = np.ones(N_DAYS)
    month_f = {1: 1.25, 2: 0.85, 3: 0.95, 4: 1.0, 5: 1.05, 6: 0.95, 7: 1.0,
               8: 1.30, 9: 1.0, 10: 0.95, 11: 1.20, 12: 1.50}
    wd_f = [0.9, 0.88, 0.9, 0.95, 1.05, 1.35, 1.15]
    bfs = set(BF_DAYS)
    for i in range(N_DAYS):
        d = START + timedelta(days=i)
        f = (1 + 0.08 * i / 365) * month_f[d.month] * wd_f[d.weekday()]
        if d.month == 1 and d.day > 21:
            f *= 0.85
        if d.month == 12 and d.day > 24:
            f *= 0.75
        if d.month == 12 and d.day == 25:
            f = 0.08
        bf = next((b for b in bfs if 0 <= (d - b).days <= 3 or -6 <= (d - b).days < 0), None)
        if bf is not None:
            delta = (d - bf).days
            f *= {0: 4.0, 1: 2.4, 2: 2.0, 3: 3.4}.get(delta, 1.6)
        w[i] = f
    return w


def cum_weights(w):
    return np.concatenate([[0.0], np.cumsum(w)])


def sample_days(rng, cw, lo, hi, n):
    """Sample n day indexes in [lo, hi] proportional to calendar weights."""
    a, b = cw[lo], cw[hi + 1]
    t = a + rng.random(n) * (b - a)
    return np.clip(np.searchsorted(cw, t, side="right") - 1, lo, hi)


ONLINE_HOUR_W = np.array([1.2, .7, .4, .3, .3, .5, 1, 1.8, 2.4, 2.6, 2.8, 3, 3.2, 3.1, 3,
                          3, 3.1, 3.4, 3.9, 4.6, 5.0, 4.8, 3.6, 2.2])
ONLINE_HOUR_W = ONLINE_HOUR_W / ONLINE_HOUR_W.sum()
STORE_HOUR_W = np.array([0] * 10 + [2, 3, 4.5, 5, 5, 5.2, 5.6, 6, 5.5, 4, 2.5, 1] + [0] * 2, dtype=float)
STORE_HOUR_W = STORE_HOUR_W / STORE_HOUR_W.sum()

START64 = np.datetime64(START.isoformat(), "s")


def to_ts(day_idx, seconds):
    return START64 + (np.asarray(day_idx, dtype="int64") * 86400 + np.asarray(seconds, dtype="int64")).astype("timedelta64[s]")


def perm(x, a, b, m):
    """Bijective obfuscation of integer ids (gcd(a, m) == 1)."""
    return (int(x) * a + b) % m


def md5hex(s):
    return hashlib.md5(s.encode()).hexdigest()


def uuid_of(s):
    h = md5hex(s)
    return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-a{h[17:20]}-{h[20:32]}"


# ---- deterministic source-system ids (same formula used everywhere) ----
def ecom_id(idx, k=0):
    return f"EC{perm(idx + k * 4_000_000, 7_368_787, 1_234_567, 100_000_000):08d}"


def pos_id(idx, k=0):
    return f"P{perm(idx + k * 4_000_000, 9_999_991, 77_777, 1_000_000_000):09d}"


def loyalty_id(idx, k=0):
    return f"LY{perm(idx + k * 4_000_000, 3_141_593, 2_718_281, 1_000_000_000):09d}"


def loyalty_card(idx, k=0):
    return f"6034{perm(idx + k * 4_000_000, 982_451_653, 57_885_161, 1_000_000_000_000):012d}"


def app_user_id(idx):
    return uuid_of(f"app-{idx}")


def device_id(idx, k=0):
    return uuid_of(f"dev-{idx}-{k}").upper()


def fitness_id(idx):
    return f"RUN{perm(idx, 15_485_863, 32_452_843, 100_000_000):08d}"


def cookie_id(idx, k=0):
    return "ck_" + md5hex(f"ck-{idx}-{k}")[:20]


# ---------------- Spark session / IO ----------------
_TRUTH_READY = []


def get_spark():
    import sf_session
    import gen
    in_job = "DATABRICKS_RUNTIME_VERSION" in os.environ or os.environ.get("SF_IN_JOB") == "1"
    spark = sf_session.get_runtime(gen) if in_job else sf_session.get(PROFILE, gen)
    if not _TRUTH_READY:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TRUTH} COMMENT "
                  "'StrideForge synthetic generator scratch tables and ground truth (identity answer key)'")
        _TRUTH_READY.append(1)
    return spark


def save_truth(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TRUTH}.{name}")
    return get_spark().table(f"{TRUTH}.{name}")


def truth(name):
    return get_spark().table(f"{TRUTH}.{name}")


def write_bronze(df, table, source_system, landing_path, event_ts_col="_event_ts"):
    """Add ingestion metadata, align to the target table's schema and overwrite it.

    df must carry a TIMESTAMP column `_event_ts` (used to derive _ingested_at / _source_file)."""
    from pyspark.sql import functions as F
    spark = get_spark()
    full = f"{BRONZE}.{table}"
    target = spark.table(full).schema
    ev = F.coalesce(F.col(event_ts_col), F.lit(f"{END} 23:00:00").cast("timestamp"))
    lag_s = (F.pmod(F.xxhash64(*[F.col(c) for c in df.columns[:2]]), F.lit(5400)) + 60).cast("int")
    df = (df.withColumn("_ingested_at", F.least(
                F.from_unixtime(F.unix_timestamp(ev) + lag_s).cast("timestamp"),
                F.lit(f"{END} 23:59:00").cast("timestamp")))
            .withColumn("_source_system", F.lit(source_system))
            .withColumn("_source_file", F.concat(
                F.lit(f"s3://strideforge-landing/{landing_path}/"), F.date_format(ev, "yyyy/MM/dd"),
                F.lit(f"/{table.replace('_raw', '')}_"), F.date_format(ev, "yyyyMMdd"),
                F.lit("_"), F.lpad(F.pmod(F.hour(ev), F.lit(24)).cast("string"), 2, "0"), F.lit(".json"))))
    missing = [f.name for f in target if f.name not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing columns {missing}")
    df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target]).write.insertInto(full, overwrite=True)
    n = spark.table(full).count()
    print(f"  bronze.{table:34s} {n:>12,d} rows", flush=True)
    return n
