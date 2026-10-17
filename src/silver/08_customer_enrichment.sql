-- Silver customer enrichment (needs identity + commerce): addresses, households, fit profile, consent

CREATE OR REPLACE TEMP VIEW idmap AS
SELECT id_type, id_value, min(unified_customer_id) AS unified_customer_id
FROM ${catalog}.${silver}.identity_graph GROUP BY id_type, id_value;

CREATE OR REPLACE TEMP VIEW rec AS
SELECT c.*, g.unified_customer_id
FROM ${catalog}.${silver}.customer_source_records c
JOIN (SELECT DISTINCT source_record_key, unified_customer_id FROM ${catalog}.${silver}.identity_graph) g USING (source_record_key);

INSERT OVERWRITE ${catalog}.${silver}.customer_addresses BY NAME
WITH a AS (
  SELECT unified_customer_id, address_standardized, source_system, address_line1, city, state, postal_code, country,
         coalesce(updated_at, created_at) AS ts
  FROM rec WHERE postal_code IS NOT NULL AND address_line1 IS NOT NULL),
d AS (
  SELECT unified_customer_id, address_standardized, max_by(address_line1, ts) AS address_line1, max_by(city, ts) AS city,
         max_by(state, ts) AS state, max_by(postal_code, ts) AS postal_code, max_by(country, ts) AS country,
         max_by(source_system, ts) AS source_system, max(ts) AS updated_at,
         max(source_system IN ('ecom', 'ecom_guest')) AS is_shipping
  FROM a GROUP BY 1, 2)
SELECT concat('AD', upper(substr(sha2(concat(unified_customer_id, '|', address_standardized), 256), 1, 14))) AS address_id,
       unified_customer_id, CASE WHEN is_shipping THEN 'shipping' ELSE 'home' END AS address_type,
       address_line1, city, state, postal_code, country, CAST(NULL AS DOUBLE) AS latitude, CAST(NULL AS DOUBLE) AS longitude,
       row_number() OVER (PARTITION BY unified_customer_id ORDER BY updated_at DESC) = 1 AS is_primary,
       postal_code RLIKE '^([0-9]{5}|[A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}|[A-Z][0-9][A-Z] [0-9][A-Z][0-9])$' AS is_valid,
       source_system, updated_at
FROM d;

INSERT OVERWRITE ${catalog}.${silver}.households BY NAME
WITH p AS (
  SELECT a.unified_customer_id, concat_ws('|', ${catalog}.${silver}.std_street(a.address_line1), upper(replace(a.postal_code, ' ', ''))) AS hkey,
         a.address_id
  FROM ${catalog}.${silver}.customer_addresses a WHERE a.is_primary),
kids AS (
  SELECT l.unified_customer_id,
         collect_set(CASE WHEN k.size LIKE '%C' THEN '0-5' WHEN k.size LIKE '%Y' AND try_cast(replace(k.size, 'Y', '') AS DOUBLE) < 3.5 THEN '6-9'
                          WHEN k.size LIKE '%Y' THEN '10-14' WHEN k.size LIKE 'Y%' THEN '6-14' END) AS bands
  FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.skus k USING (sku)
  WHERE l.category IN ('Kids Footwear', 'Kids Apparel') AND l.unified_customer_id IS NOT NULL GROUP BY 1)
SELECT concat('HH', upper(substr(sha2(p.hkey, 256), 1, 12))) AS household_id, min(p.address_id) AS address_id,
       CAST(count(DISTINCT p.unified_customer_id) AS INT) AS member_count,
       CAST(count(DISTINCT p.unified_customer_id) AS INT) AS adult_count,
       CAST(size(array_distinct(flatten(collect_list(coalesce(kids.bands, array()))))) AS INT) AS kids_count,
       array_distinct(flatten(collect_list(coalesce(kids.bands, array())))) AS kids_age_bands, current_timestamp() AS updated_at
FROM p LEFT JOIN kids USING (unified_customer_id) GROUP BY p.hkey;

MERGE INTO ${catalog}.${silver}.customers c
USING (
  WITH pa AS (SELECT unified_customer_id, address_id,
                     concat('HH', upper(substr(sha2(concat_ws('|', ${catalog}.${silver}.std_street(address_line1),
                                                              upper(replace(postal_code, ' ', ''))), 256), 1, 12))) AS household_id
              FROM ${catalog}.${silver}.customer_addresses WHERE is_primary),
  hs AS (SELECT unified_customer_id, mode(store_id) AS home_store_id FROM ${catalog}.${silver}.orders
         WHERE channel = 'store' AND unified_customer_id IS NOT NULL GROUP BY 1),
  reg AS (SELECT m.unified_customer_id, min(p.store_id) AS reg_store
          FROM ${catalog}.${bronze}.pos_customers_raw p JOIN idmap m ON m.id_type = 'pos_id' AND m.id_value = p.pos_customer_id GROUP BY 1)
  SELECT c.unified_customer_id, pa.address_id, pa.household_id, coalesce(hs.home_store_id, reg.reg_store) AS home_store_id
  FROM ${catalog}.${silver}.customers c LEFT JOIN pa USING (unified_customer_id) LEFT JOIN hs USING (unified_customer_id)
  LEFT JOIN reg USING (unified_customer_id)) s
ON c.unified_customer_id = s.unified_customer_id
WHEN MATCHED THEN UPDATE SET c.primary_address_id = s.address_id, c.household_id = s.household_id,
                             c.home_store_id = s.home_store_id, c.updated_at = current_timestamp();

INSERT OVERWRITE ${catalog}.${silver}.customer_fit_profile BY NAME
WITH fw AS (
  SELECT l.unified_customer_id, k.size_us, k.width
  FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.skus k USING (sku)
  LEFT JOIN ${catalog}.${silver}.returns r ON r.order_id = l.order_id AND r.line_number = l.line_number
  WHERE l.unified_customer_id IS NOT NULL AND k.size_us IS NOT NULL AND r.return_id IS NULL
    AND l.category NOT IN ('Kids Footwear')),
fws AS (SELECT unified_customer_id, mode(size_us) AS shoe_size_us, mode(width) AS shoe_width FROM fw GROUP BY 1),
conf AS (SELECT f.unified_customer_id, round(avg(CASE WHEN fw.size_us = f.shoe_size_us THEN 1.0 ELSE 0 END), 2) AS c
         FROM fws f JOIN fw USING (unified_customer_id) GROUP BY 1),
ap AS (
  SELECT l.unified_customer_id,
         mode(CASE WHEN p.sub_category IN ('Tops', 'Outerwear', 'Base Layers') THEN k.size END) AS top_size,
         mode(CASE WHEN p.sub_category = 'Bottoms' THEN k.size END) AS bottom_size
  FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.skus k USING (sku)
  JOIN ${catalog}.${silver}.products p ON p.style_id = k.style_id
  WHERE l.unified_customer_id IS NOT NULL AND p.category = 'Apparel' GROUP BY 1),
gait AS (
  SELECT coalesce(m1.unified_customer_id, m2.unified_customer_id) AS unified_customer_id,
         max_by(g.recommended_size, g.analysis_ts) AS rec_size, max_by(g.recommended_width, g.analysis_ts) AS rec_width,
         max_by(g.arch_type, g.analysis_ts) AS arch_type, max_by(g.pronation_type, g.analysis_ts) AS pronation_type,
         max(CAST(g.analysis_ts AS TIMESTAMP)) AS last_gait
  FROM ${catalog}.${bronze}.gait_analysis_raw g
  LEFT JOIN idmap m1 ON m1.id_type = 'loyalty_id' AND m1.id_value = g.loyalty_id
  LEFT JOIN idmap m2 ON m2.id_type = 'email' AND m2.id_value = ${catalog}.${silver}.norm_email(g.email)
  WHERE coalesce(m1.unified_customer_id, m2.unified_customer_id) IS NOT NULL GROUP BY 1),
u AS (SELECT unified_customer_id FROM fws UNION SELECT unified_customer_id FROM ap UNION SELECT unified_customer_id FROM gait)
SELECT u.unified_customer_id, coalesce(try_cast(gait.rec_size AS DOUBLE), fws.shoe_size_us) AS shoe_size_us,
       coalesce(gait.rec_width, fws.shoe_width) AS shoe_width, gait.arch_type, gait.pronation_type,
       ap.top_size AS apparel_top_size, ap.bottom_size AS apparel_bottom_size,
       CASE WHEN gait.unified_customer_id IS NOT NULL THEN 'gait_lab' WHEN fws.unified_customer_id IS NOT NULL THEN 'purchase_history'
            ELSE 'declared' END AS size_source,
       CASE WHEN gait.unified_customer_id IS NOT NULL THEN 0.95 ELSE coalesce(conf.c, 0.5) END AS size_confidence,
       gait.last_gait AS last_gait_analysis_at, current_timestamp() AS updated_at
FROM u LEFT JOIN fws USING (unified_customer_id) LEFT JOIN conf USING (unified_customer_id) LEFT JOIN ap USING (unified_customer_id)
LEFT JOIN gait USING (unified_customer_id);

INSERT OVERWRITE ${catalog}.${silver}.consent BY NAME
WITH e AS (
  SELECT coalesce(m1.unified_customer_id, m2.unified_customer_id, m3.unified_customer_id) AS unified_customer_id,
         CASE WHEN c.purpose = 'profiling' THEN 'profiling' WHEN c.purpose IN ('ad_targeting', 'sale_of_data') THEN 'paid_media'
              ELSE c.channel END AS channel,
         c.purpose, CASE WHEN c.status = 'opted_in' THEN 'opted_in' ELSE 'opted_out' END AS status, c.jurisdiction,
         c.policy_version, c.capture_source, CAST(c.event_ts AS TIMESTAMP) AS effective_at
  FROM ${catalog}.${bronze}.consent_events_raw c
  LEFT JOIN idmap m1 ON c.identifier_type = 'email' AND m1.id_type = 'email' AND m1.id_value = ${catalog}.${silver}.norm_email(c.subject_identifier)
  LEFT JOIN idmap m2 ON c.identifier_type = 'phone' AND m2.id_type = 'phone' AND m2.id_value = ${catalog}.${silver}.norm_phone(c.subject_identifier)
  LEFT JOIN idmap m3 ON c.identifier_type = 'device_id' AND m3.id_type = 'device_id' AND m3.id_value = c.subject_identifier
  UNION ALL
  SELECT m.unified_customer_id, 'email', 'marketing', 'opted_out', NULL, NULL, 'esp_unsubscribe', CAST(u.unsubscribe_ts AS TIMESTAMP)
  FROM ${catalog}.${bronze}.esp_unsubscribes_raw u
  JOIN idmap m ON m.id_type = 'email' AND m.id_value = ${catalog}.${silver}.norm_email(u.recipient_email))
SELECT unified_customer_id, channel, purpose, max_by(status, effective_at) AS status, max_by(jurisdiction, effective_at) AS jurisdiction,
       max_by(policy_version, effective_at) AS policy_version, max_by(capture_source, effective_at) AS capture_source,
       max(effective_at) AS effective_at, current_timestamp() AS updated_at
FROM e WHERE unified_customer_id IS NOT NULL GROUP BY unified_customer_id, channel, purpose;
