# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 02 - Identity resolution (who is who)
# MAGIC
# MAGIC **What it does:** Finds which records from different systems belong to the same person and gives each person one `unified_customer_id`. Then builds one best-version customer record.
# MAGIC
# MAGIC - **Reads:** bronze customer profiles from e-com, POS, loyalty, app, run app and race partners
# MAGIC - **Writes:** `customer_source_records`, `identity_match_pairs`, `identity_graph`, `customers`
# MAGIC - **Runs in:** `strideforge_medallion` -> `silver_identity_resolution`
# MAGIC
# MAGIC Catalog and schema names come from widgets (set by the job, or edit them at the top when running by hand).
# COMMAND ----------

# MAGIC %run ../_params

# COMMAND ----------

from pyspark.sql import functions as F

B, S, T = f"{a.catalog}.{a.bronze}", f"{a.catalog}.{a.silver}", f"{a.catalog}.{a.truth}"
fn = lambda name: f"{S}.{name}"


def sql(q):
    return spark.sql(q)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Put all customer records into one standard format
# COMMAND ----------

ADDR = """split({c}, ', ') AS _ap"""
sql(f"""
CREATE OR REPLACE TEMP VIEW src_ecom AS
SELECT concat('ecom:', ecom_customer_id) AS source_record_key, 'ecom' AS source_system, ecom_customer_id AS source_id,
       first_name, last_name, email, phone, to_date(birth_date) AS birth_date,
       CASE gender WHEN 'female' THEN 'F' WHEN 'male' THEN 'M' WHEN 'non-binary' THEN 'X' END AS gender,
       array_join(slice(_ap, 1, greatest(size(_ap) - 3, 1)), ', ') AS address_line1, element_at(_ap, -3) AS city,
       trim(regexp_replace(element_at(_ap, -2), coalesce({fn('postal_from_text')}(element_at(_ap, -2)), '$^'), '')) AS state,
       {fn('postal_from_text')}(element_at(_ap, -2)) AS postal_code, element_at(_ap, -1) AS country,
       CAST(NULL AS STRING) AS loyalty_id, CAST(NULL AS STRING) AS device_id, CAST(NULL AS STRING) AS cookie_id,
       CAST(created_at AS TIMESTAMP) AS created_at, CAST(updated_at AS TIMESTAMP) AS updated_at
FROM (SELECT *, {ADDR.format(c='default_address')} FROM {B}.ecom_customers_raw
      QUALIFY row_number() OVER (PARTITION BY ecom_customer_id ORDER BY _ingested_at DESC) = 1)""")
sql(f"""
CREATE OR REPLACE TEMP VIEW src_guest AS
SELECT concat('guest:', substr(sha2(em, 256), 1, 16)) AS source_record_key, 'ecom_guest' AS source_system,
       substr(sha2(em, 256), 1, 16) AS source_id, CAST(NULL AS STRING) AS first_name, CAST(NULL AS STRING) AS last_name,
       email, CAST(NULL AS STRING) AS phone, CAST(NULL AS DATE) AS birth_date, CAST(NULL AS STRING) AS gender,
       array_join(slice(_ap, 1, greatest(size(_ap) - 3, 1)), ', ') AS address_line1, element_at(_ap, -3) AS city,
       trim(regexp_replace(element_at(_ap, -2), coalesce({fn('postal_from_text')}(element_at(_ap, -2)), '$^'), '')) AS state,
       {fn('postal_from_text')}(element_at(_ap, -2)) AS postal_code, element_at(_ap, -1) AS country,
       CAST(NULL AS STRING) AS loyalty_id, CAST(NULL AS STRING) AS device_id, CAST(NULL AS STRING) AS cookie_id,
       created_at, updated_at
FROM (SELECT {fn('norm_email')}(guest_email) AS em, min_by(guest_email, order_ts) AS email,
             split(max_by(shipping_address, order_ts), ', ') AS _ap,
             min(CAST(order_ts AS TIMESTAMP)) AS created_at, max(CAST(order_ts AS TIMESTAMP)) AS updated_at
      FROM {B}.ecom_orders_raw WHERE guest_email IS NOT NULL GROUP BY 1)""")
sql(f"""
CREATE OR REPLACE TEMP VIEW src_pos AS
SELECT concat('pos:', p.pos_customer_id) AS source_record_key, 'pos' AS source_system, p.pos_customer_id AS source_id,
       p.first_name, p.last_name, p.email, p.phone,
       CASE p.country WHEN 'US' THEN try_to_date(p.birth_date, 'MM/dd/yyyy') WHEN 'UK' THEN try_to_date(p.birth_date, 'dd/MM/yyyy')
                      WHEN 'DE' THEN try_to_date(p.birth_date, 'dd.MM.yyyy') ELSE try_to_date(p.birth_date) END AS birth_date,
       CAST(NULL AS STRING) AS gender, p.address_line1, initcap(lower(p.city)) AS city, p.state, p.postal_code, p.country,
       l.loyalty_id, CAST(NULL AS STRING) AS device_id, CAST(NULL AS STRING) AS cookie_id,
       CASE WHEN p.country = 'US' THEN try_to_timestamp(p.created_at, 'MM/dd/yyyy HH:mm:ss') ELSE try_to_timestamp(p.created_at, 'dd/MM/yyyy HH:mm:ss') END AS created_at,
       CASE WHEN p.country = 'US' THEN try_to_timestamp(p.updated_at, 'MM/dd/yyyy HH:mm:ss') ELSE try_to_timestamp(p.updated_at, 'dd/MM/yyyy HH:mm:ss') END AS updated_at
FROM (SELECT * FROM {B}.pos_customers_raw QUALIFY row_number() OVER (PARTITION BY pos_customer_id ORDER BY _ingested_at DESC) = 1) p
LEFT JOIN (SELECT DISTINCT loyalty_card_number, loyalty_id FROM {B}.loyalty_members_raw) l USING (loyalty_card_number)""")
sql(f"""
CREATE OR REPLACE TEMP VIEW src_loyalty AS
SELECT concat('loyalty:', loyalty_id) AS source_record_key, 'loyalty' AS source_system, loyalty_id AS source_id,
       first_name, last_name, email, phone, to_date(birth_date) AS birth_date, CAST(NULL AS STRING) AS gender,
       CAST(NULL AS STRING) AS address_line1, CAST(NULL AS STRING) AS city, CAST(NULL AS STRING) AS state, postal_code,
       coalesce({fn('country_from_postal')}(postal_code),
                CASE WHEN {fn('norm_phone')}(phone) LIKE '+49%' THEN 'DE' WHEN {fn('norm_phone')}(phone) LIKE '+44%' THEN 'UK'
                     WHEN {fn('norm_phone')}(phone) LIKE '+1%' THEN 'US' END) AS country,
       loyalty_id, CAST(NULL AS STRING) AS device_id, CAST(NULL AS STRING) AS cookie_id,
       CAST(enrolled_ts AS TIMESTAMP) AS created_at, _ingested_at AS updated_at
FROM {B}.loyalty_members_raw QUALIFY row_number() OVER (PARTITION BY loyalty_id ORDER BY _ingested_at DESC) = 1""")
sql(f"""
CREATE OR REPLACE TEMP VIEW src_app AS
SELECT concat('app:', app_user_id) AS source_record_key, 'app' AS source_system, app_user_id AS source_id,
       first(first_name, true) AS first_name, first(last_name, true) AS last_name, first(email, true) AS email,
       first(phone, true) AS phone, CAST(NULL AS DATE) AS birth_date, CAST(NULL AS STRING) AS gender,
       CAST(NULL AS STRING) AS address_line1, CAST(NULL AS STRING) AS city, CAST(NULL AS STRING) AS state,
       CAST(NULL AS STRING) AS postal_code, CAST(NULL AS STRING) AS country, CAST(NULL AS STRING) AS loyalty_id,
       min(device_id) AS device_id, CAST(NULL AS STRING) AS cookie_id,
       min(CAST(install_ts AS TIMESTAMP)) AS created_at, max(CAST(last_open_ts AS TIMESTAMP)) AS updated_at
FROM {B}.app_users_raw GROUP BY app_user_id""")
sql(f"""
CREATE OR REPLACE TEMP VIEW src_fitness AS
SELECT concat('fitness:', fitness_user_id) AS source_record_key, 'fitness' AS source_system, fitness_user_id AS source_id,
       CAST(NULL AS STRING) AS first_name, CAST(NULL AS STRING) AS last_name, mode(email) AS email,
       CAST(NULL AS STRING) AS phone, CAST(NULL AS DATE) AS birth_date, CAST(NULL AS STRING) AS gender,
       CAST(NULL AS STRING) AS address_line1, CAST(NULL AS STRING) AS city, CAST(NULL AS STRING) AS state,
       CAST(NULL AS STRING) AS postal_code, CAST(NULL AS STRING) AS country, CAST(NULL AS STRING) AS loyalty_id,
       CAST(NULL AS STRING) AS device_id, CAST(NULL AS STRING) AS cookie_id,
       min(CAST(start_ts AS TIMESTAMP)) AS created_at, max(CAST(start_ts AS TIMESTAMP)) AS updated_at
FROM {B}.fitness_activities_raw GROUP BY fitness_user_id""")
sql(f"""
CREATE OR REPLACE TEMP VIEW src_race AS
SELECT concat('race:', substr(sha2(em, 256), 1, 16)) AS source_record_key, 'race_partner' AS source_system,
       substr(sha2(em, 256), 1, 16) AS source_id, first_name, last_name, email, CAST(NULL AS STRING) AS phone,
       CAST(NULL AS DATE) AS birth_date, CAST(NULL AS STRING) AS gender, CAST(NULL AS STRING) AS address_line1,
       CAST(NULL AS STRING) AS city, CAST(NULL AS STRING) AS state, CAST(NULL AS STRING) AS postal_code,
       CAST(NULL AS STRING) AS country, CAST(NULL AS STRING) AS loyalty_id, CAST(NULL AS STRING) AS device_id,
       CAST(NULL AS STRING) AS cookie_id, created_at, updated_at
FROM (SELECT {fn('norm_email')}(email) AS em, max_by(first_name, registered_ts) AS first_name,
             max_by(last_name, registered_ts) AS last_name, max_by(email, registered_ts) AS email,
             min(CAST(registered_ts AS TIMESTAMP)) AS created_at, max(CAST(registered_ts AS TIMESTAMP)) AS updated_at
      FROM {B}.event_registrations_raw WHERE email IS NOT NULL GROUP BY 1)""")
srcs = " UNION ALL ".join(f"SELECT * FROM src_{x}" for x in ["ecom", "guest", "pos", "loyalty", "app", "fitness", "race"])
sql(f"""
INSERT OVERWRITE {S}.customer_source_records BY NAME
SELECT source_record_key, source_system, source_id, first_name, last_name,
       concat_ws(' ', {fn('canon_first_name')}(first_name), {fn('norm_name')}(last_name)) AS full_name_normalized,
       email, {fn('norm_email')}(email) AS email_normalized, sha2({fn('norm_email')}(email), 256) AS email_sha256,
       {fn('norm_phone')}(phone) AS phone_e164, sha2({fn('norm_phone')}(phone), 256) AS phone_sha256, birth_date, gender,
       address_line1, city, state, upper(trim(postal_code)) AS postal_code, country,
       concat_ws('|', {fn('std_street')}(address_line1), upper(replace(postal_code, ' ', ''))) AS address_standardized,
       loyalty_id, device_id, cookie_id, created_at, updated_at
FROM ({srcs})""")
print("source records:", spark.table(f"{S}.customer_source_records").count(), flush=True)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Find matches (same email, phone, loyalty id, or name + postal code / birthday)
# COMMAND ----------

sql(f"""
CREATE OR REPLACE TEMP VIEW ir_keys AS
WITH r AS (SELECT *, {fn('canon_first_name')}(first_name) AS cf, {fn('norm_name')}(last_name) AS ln,
                  upper(replace(postal_code, ' ', '')) AS pc FROM {S}.customer_source_records)
SELECT source_record_key, 'email' AS rule, email_normalized AS k, 'deterministic' AS mtype, 1.0 AS score FROM r WHERE email_normalized IS NOT NULL
UNION ALL SELECT source_record_key, 'phone', phone_e164, 'deterministic', 0.95 FROM r WHERE phone_e164 IS NOT NULL
UNION ALL SELECT source_record_key, 'loyalty_id', loyalty_id, 'deterministic', 1.0 FROM r WHERE loyalty_id IS NOT NULL
UNION ALL SELECT source_record_key, 'name_postal', concat_ws('|', cf, ln, pc), 'probabilistic', 0.80 FROM r
          WHERE cf IS NOT NULL AND length(cf) > 1 AND ln IS NOT NULL AND pc IS NOT NULL
UNION ALL SELECT source_record_key, 'name_dob', concat_ws('|', cf, ln, CAST(birth_date AS STRING)), 'probabilistic', 0.85 FROM r
          WHERE cf IS NOT NULL AND length(cf) > 1 AND ln IS NOT NULL AND birth_date IS NOT NULL""")
sql(f"""
INSERT OVERWRITE {S}.identity_match_pairs BY NAME
WITH g AS (SELECT rule, k, mtype, max(score) AS score, collect_set(source_record_key) AS recs FROM ir_keys GROUP BY rule, k, mtype
           HAVING size(recs) BETWEEN 2 AND 25),
star AS (SELECT array_min(recs) AS record_key_a, explode(recs) AS record_key_b, rule, mtype, score FROM g),
card AS (SELECT DISTINCT concat('pos:', t.pos_customer_id) AS record_key_a, concat('loyalty:', l.loyalty_id) AS record_key_b,
                'pos_txn_card_scan' AS rule, 'deterministic' AS mtype, 1.0 AS score
         FROM {B}.pos_transactions_raw t JOIN (SELECT DISTINCT loyalty_card_number, loyalty_id FROM {B}.loyalty_members_raw) l
           USING (loyalty_card_number) WHERE t.pos_customer_id IS NOT NULL),
oref AS (SELECT DISTINCT concat('fitness:', s.fitness_user_id) AS record_key_a,
                coalesce(concat('ecom:', o.ecom_customer_id), concat('guest:', substr(sha2({fn('norm_email')}(o.guest_email), 256), 1, 16)),
                         concat('pos:', p.pos_customer_id)) AS record_key_b,
                'run_app_order_reference' AS rule, 'deterministic' AS mtype, 1.0 AS score
         FROM {B}.fitness_shoe_registrations_raw s
         LEFT JOIN {B}.ecom_orders_raw o ON o.order_id = s.order_reference
         LEFT JOIN {B}.pos_transactions_raw p ON p.transaction_id = s.order_reference
         WHERE s.order_reference IS NOT NULL AND coalesce(o.ecom_customer_id, o.guest_email, p.pos_customer_id) IS NOT NULL)
SELECT record_key_a, record_key_b, rule AS match_rule, mtype AS match_type, CAST(score AS DOUBLE) AS match_score,
       true AS is_accepted, current_timestamp() AS evaluated_at
FROM (SELECT * FROM star WHERE record_key_a <> record_key_b UNION ALL SELECT * FROM card UNION ALL SELECT * FROM oref)""")
print("match pairs:", spark.table(f"{S}.identity_match_pairs").count(), flush=True)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Group matched records into people (connected components)
# COMMAND ----------

E = spark.table(f"{S}.identity_match_pairs").where("is_accepted").select(F.col("record_key_a").alias("a"), F.col("record_key_b").alias("b"))
E = E.unionByName(E.select(F.col("b").alias("a"), F.col("a").alias("b"))).distinct()
E.write.mode("overwrite").saveAsTable(f"{S}._ir_edges")
spark.table(f"{S}.customer_source_records").select(F.col("source_record_key").alias("node"), F.col("source_record_key").alias("label")) \
    .write.mode("overwrite").saveAsTable(f"{S}._ir_labels_0")
cur = 0
for it in range(1, 30):
    L = spark.table(f"{S}._ir_labels_{cur}")
    Ed = spark.table(f"{S}._ir_edges")
    nb = Ed.join(L, Ed.b == L.node).groupBy(Ed.a).agg(F.min("label").alias("nl"))
    new = (L.join(nb, L.node == nb.a, "left")
             .select("node", F.least("label", F.coalesce("nl", "label")).alias("label"), (F.col("nl") < F.col("label")).alias("chg")))
    # pointer jumping: adopt the label of my label (halves the remaining diameter each round)
    nxt = 1 - cur
    new.write.mode("overwrite").saveAsTable(f"{S}._ir_labels_tmp")
    N = spark.table(f"{S}._ir_labels_tmp")
    J = N.alias("x").join(N.alias("y"), F.col("x.label") == F.col("y.node"), "left") \
        .select(F.col("x.node"), F.least(F.col("x.label"), F.coalesce(F.col("y.label"), F.col("x.label"))).alias("label"))
    J.write.mode("overwrite").saveAsTable(f"{S}._ir_labels_{nxt}")
    changed = N.where("chg").count()
    cur = nxt
    print(f"  CC iteration {it}: {changed:,} labels changed", flush=True)
    if changed == 0:
        break
spark.table(f"{S}._ir_labels_{cur}").select(
    F.col("node").alias("source_record_key"),
    F.concat(F.lit("UC"), F.upper(F.substring(F.sha2("label", 256), 1, 12))).alias("unified_customer_id")) \
    .write.mode("overwrite").saveAsTable(f"{S}._ir_resolved")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Build the identity graph (every id -> one customer)
# COMMAND ----------

sql(f"""
INSERT OVERWRITE {S}.identity_graph BY NAME
WITH r AS (SELECT c.*, x.unified_customer_id FROM {S}.customer_source_records c JOIN {S}._ir_resolved x USING (source_record_key)),
ids AS (
  SELECT unified_customer_id, source_record_key, source_system,
         CASE source_system WHEN 'ecom' THEN 'ecom_id' WHEN 'pos' THEN 'pos_id' WHEN 'loyalty' THEN 'loyalty_id'
              WHEN 'app' THEN 'app_id' WHEN 'fitness' THEN 'fitness_id' WHEN 'ecom_guest' THEN 'guest_id' ELSE 'race_id' END AS id_type,
         source_id AS id_value, created_at AS first_seen, updated_at AS last_seen FROM r
  UNION ALL SELECT unified_customer_id, source_record_key, source_system, 'email', email_normalized, created_at, updated_at FROM r WHERE email_normalized IS NOT NULL
  UNION ALL SELECT unified_customer_id, source_record_key, source_system, 'phone', phone_e164, created_at, updated_at FROM r WHERE phone_e164 IS NOT NULL
  UNION ALL SELECT r.unified_customer_id, r.source_record_key, r.source_system, 'loyalty_card', l.loyalty_card_number, r.created_at, r.updated_at
            FROM r JOIN {B}.loyalty_members_raw l ON r.source_system = 'loyalty' AND l.loyalty_id = r.source_id
  UNION ALL SELECT r.unified_customer_id, r.source_record_key, r.source_system, 'device_id', d.device_id,
                   min(CAST(d.install_ts AS TIMESTAMP)), max(CAST(d.last_open_ts AS TIMESTAMP))
            FROM r JOIN {B}.app_users_raw d ON r.source_system = 'app' AND d.app_user_id = r.source_id GROUP BY 1, 2, 3, 4, 5
  UNION ALL SELECT r.unified_customer_id, r.source_record_key, r.source_system, 'push_token', t.push_token,
                   min(CAST(t.updated_ts AS TIMESTAMP)), max(CAST(t.updated_ts AS TIMESTAMP))
            FROM r JOIN {B}.app_push_tokens_raw t ON r.source_system = 'app' AND t.app_user_id = r.source_id
            WHERE t.token_status = 'active' GROUP BY 1, 2, 3, 4, 5
  UNION ALL SELECT r.unified_customer_id, r.source_record_key, r.source_system, 'cookie_id', w.cookie_id,
                   min(CAST(w.session_start_ts AS TIMESTAMP)), max(CAST(w.session_start_ts AS TIMESTAMP))
            FROM r JOIN {B}.web_sessions_raw w ON r.source_system = 'ecom' AND w.ecom_customer_id = r.source_id
            GROUP BY 1, 2, 3, 4, 5)
SELECT unified_customer_id, source_record_key, source_system, id_type, id_value,
       row_number() OVER (PARTITION BY unified_customer_id, id_type ORDER BY last_seen DESC NULLS LAST) = 1 AS is_primary,
       first_seen, last_seen, current_timestamp() AS resolved_at
FROM ids""")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Build one best record per customer
# COMMAND ----------

sql(f"""
INSERT OVERWRITE {S}.customers BY NAME
WITH r AS (SELECT c.*, x.unified_customer_id,
                  CASE source_system WHEN 'ecom' THEN 1 WHEN 'loyalty' THEN 2 WHEN 'app' THEN 3 WHEN 'pos' THEN 4
                       WHEN 'fitness' THEN 5 WHEN 'race_partner' THEN 6 ELSE 7 END AS prio
           FROM {S}.customer_source_records c JOIN {S}._ir_resolved x USING (source_record_key)),
g AS (
  SELECT unified_customer_id,
         initcap(lower(min_by(first_name, CASE WHEN first_name IS NULL OR first_name LIKE '%.' THEN 99 ELSE prio END))) AS first_name,
         initcap(lower(min_by(last_name, CASE WHEN last_name IS NULL THEN 99 ELSE prio END))) AS last_name,
         lower(trim(min_by(email, CASE WHEN email IS NULL THEN 99 ELSE prio END))) AS email,
         max_by(phone_e164, CASE WHEN phone_e164 IS NULL THEN TIMESTAMP'1900-01-01' ELSE coalesce(updated_at, created_at) END) AS phone_e164,
         min_by(birth_date, CASE WHEN birth_date IS NULL THEN 99 ELSE prio END) AS birth_date,
         min_by(gender, CASE WHEN gender IS NULL THEN 99 ELSE prio END) AS gender,
         min(loyalty_id) AS loyalty_id, count(*) AS source_record_count,
         min(created_at) AS first_seen_at, max(coalesce(updated_at, created_at)) AS last_seen_at
  FROM r GROUP BY unified_customer_id)
SELECT unified_customer_id, first_name, last_name, email, phone_e164, birth_date,
       CASE WHEN birth_date IS NULL THEN 'unknown'
            WHEN floor(months_between(current_date(), birth_date) / 12) < 25 THEN '16-24'
            WHEN floor(months_between(current_date(), birth_date) / 12) < 35 THEN '25-34'
            WHEN floor(months_between(current_date(), birth_date) / 12) < 45 THEN '35-44'
            WHEN floor(months_between(current_date(), birth_date) / 12) < 55 THEN '45-54' ELSE '55+' END AS age_band,
       gender, CAST(NULL AS STRING) AS household_id, CAST(NULL AS STRING) AS primary_address_id,
       CAST(NULL AS STRING) AS home_store_id, loyalty_id, true AS is_known, CAST(source_record_count AS INT) AS source_record_count,
       first_seen_at, last_seen_at, current_timestamp() AS updated_at
FROM g""")
print("unified customers:", spark.table(f"{S}.customers").count(), flush=True)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Check accuracy against the answer key
# COMMAND ----------

if spark.catalog.tableExists(f"{T}.identity_answer_key_profiles"):
    ev = sql(f"""
    WITH k AS (SELECT DISTINCT source_system, source_id, person_idx FROM {T}.identity_answer_key_profiles),
    r AS (SELECT x.unified_customer_id, k.person_idx
          FROM {S}.customer_source_records c JOIN {S}._ir_resolved x USING (source_record_key)
          JOIN k ON k.source_id = c.source_id AND k.source_system = CASE c.source_system WHEN 'app' THEN 'app_user' ELSE c.source_system END),
    cl AS (SELECT unified_customer_id, count(DISTINCT person_idx) np FROM r GROUP BY 1),
    pe AS (SELECT person_idx, count(DISTINCT unified_customer_id) nu FROM r GROUP BY 1)
    SELECT current_timestamp() AS evaluated_at, (SELECT count(*) FROM r) AS records_evaluated,
           (SELECT count(*) FROM pe) AS true_people, (SELECT count(*) FROM cl) AS unified_clusters,
           (SELECT round(avg(CASE WHEN np = 1 THEN 1.0 ELSE 0 END), 4) FROM cl) AS cluster_purity,
           (SELECT round(avg(CASE WHEN nu = 1 THEN 1.0 ELSE 0 END), 4) FROM pe) AS person_completeness""")
    ev.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{T}.identity_resolution_eval")
    print(ev.toPandas().to_string(index=False), flush=True)
for t in ["_ir_edges", "_ir_labels_0", "_ir_labels_1", "_ir_labels_tmp", "_ir_resolved"]:
    sql(f"DROP TABLE IF EXISTS {S}.{t}")
