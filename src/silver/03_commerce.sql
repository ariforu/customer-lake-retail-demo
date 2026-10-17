-- Silver commerce: unified orders / lines / returns / carts across e-commerce + POS, identity-stitched

CREATE OR REPLACE TEMP VIEW idmap AS
SELECT id_type, id_value, min(unified_customer_id) AS unified_customer_id
FROM ${catalog}.${silver}.identity_graph GROUP BY id_type, id_value;

CREATE OR REPLACE TEMP VIEW fx AS
SELECT * FROM VALUES ('USD', 1.0), ('CAD', 1.35), ('GBP', 0.80), ('EUR', 0.93) AS t(currency, rate);

INSERT OVERWRITE ${catalog}.${silver}.orders BY NAME
WITH el AS (SELECT order_id, sum(quantity) AS items, max(is_markdown) AS any_md FROM ${catalog}.${bronze}.ecom_order_lines_raw GROUP BY 1),
pl AS (SELECT transaction_id, sum(quantity) AS items, max(is_markdown) AS any_md FROM ${catalog}.${bronze}.pos_transaction_lines_raw GROUP BY 1),
e AS (
  SELECT o.order_id, coalesce(m1.unified_customer_id, m2.unified_customer_id) AS unified_customer_id,
         'salesforce_commerce_cloud' AS source_system,
         CASE WHEN o.device_type IN ('ios', 'android') THEN 'app' ELSE 'web' END AS channel, o.pickup_store_id AS store_id,
         CAST(o.order_ts AS TIMESTAMP) AS order_ts, o.order_status, o.subtotal_amount, o.discount_amount, o.shipping_amount,
         o.tax_amount, o.total_amount, o.currency, o.payment_type, o.promo_code, o.fulfillment_type,
         CAST(el.items AS INT) AS item_count, (NOT el.any_md AND o.discount_amount = 0) AS is_full_price,
         o.utm_source, o.utm_medium, o.utm_campaign, o.session_id
  FROM ${catalog}.${bronze}.ecom_orders_raw o JOIN el USING (order_id)
  LEFT JOIN idmap m1 ON m1.id_type = 'ecom_id' AND m1.id_value = o.ecom_customer_id
  LEFT JOIN idmap m2 ON m2.id_type = 'email' AND m2.id_value = ${catalog}.${silver}.norm_email(o.guest_email)),
p AS (
  SELECT t.transaction_id AS order_id, coalesce(m1.unified_customer_id, m2.unified_customer_id) AS unified_customer_id,
         'oracle_xstore_pos' AS source_system, 'store' AS channel, t.store_id,
         CASE WHEN t.currency = 'USD' THEN to_timestamp(t.transaction_ts, 'MM/dd/yyyy HH:mm:ss')
              ELSE to_timestamp(t.transaction_ts, 'dd/MM/yyyy HH:mm:ss') END AS order_ts,
         'completed' AS order_status, t.subtotal_amount, t.discount_amount, CAST(0 AS DECIMAL(12, 2)) AS shipping_amount,
         t.tax_amount, t.total_amount, t.currency, t.payment_type, t.promo_code, 'in_store' AS fulfillment_type,
         CAST(pl.items AS INT) AS item_count, (NOT pl.any_md AND t.discount_amount = 0) AS is_full_price,
         CAST(NULL AS STRING) AS utm_source, CAST(NULL AS STRING) AS utm_medium, CAST(NULL AS STRING) AS utm_campaign,
         CAST(NULL AS STRING) AS session_id
  FROM ${catalog}.${bronze}.pos_transactions_raw t JOIN pl USING (transaction_id)
  LEFT JOIN idmap m1 ON m1.id_type = 'pos_id' AND m1.id_value = t.pos_customer_id
  LEFT JOIN idmap m2 ON m2.id_type = 'loyalty_card' AND m2.id_value = t.loyalty_card_number)
SELECT *, to_date(order_ts) AS order_date FROM (SELECT * FROM e UNION ALL SELECT * FROM p);

INSERT OVERWRITE ${catalog}.${silver}.order_lines BY NAME
WITH l AS (
  SELECT order_id, line_number, sku, quantity, unit_price, discount_amount, line_total, is_markdown FROM ${catalog}.${bronze}.ecom_order_lines_raw
  UNION ALL
  SELECT transaction_id, line_number, sku, quantity, unit_price, discount_amount, line_total, is_markdown FROM ${catalog}.${bronze}.pos_transaction_lines_raw)
SELECT l.order_id, l.line_number, o.unified_customer_id, l.sku, k.style_id, p.category, l.quantity, l.unit_price,
       l.discount_amount, l.line_total,
       CAST(l.line_total - l.quantity * p.unit_cost * fx.rate AS DECIMAL(12, 2)) AS line_margin, l.is_markdown, o.order_ts
FROM l JOIN ${catalog}.${silver}.orders o USING (order_id)
LEFT JOIN ${catalog}.${silver}.skus k USING (sku)
LEFT JOIN ${catalog}.${silver}.products p ON p.style_id = k.style_id
LEFT JOIN fx ON fx.currency = o.currency;

INSERT OVERWRITE ${catalog}.${silver}.returns BY NAME
WITH r AS (
  SELECT return_id, order_id, line_number, sku, quantity, refund_amount, lower(return_reason_code) AS return_reason,
         return_reason_text, return_type, return_channel, CAST(requested_ts AS TIMESTAMP) AS return_ts
  FROM ${catalog}.${bronze}.ecom_returns_raw
  UNION ALL
  SELECT r.return_id, r.original_transaction_id, l.line_number, r.sku, r.quantity, r.refund_amount,
         CASE r.return_reason_code WHEN 'RC01' THEN 'too_small' WHEN 'RC02' THEN 'too_big' WHEN 'RC03' THEN 'comfort'
              WHEN 'RC04' THEN 'defect' WHEN 'RC05' THEN 'not_as_pictured' WHEN 'RC06' THEN 'changed_mind' ELSE 'other' END,
         r.return_reason_text, r.return_type, 'store', to_timestamp(r.return_ts, 'MM/dd/yyyy HH:mm:ss')
  FROM ${catalog}.${bronze}.pos_returns_raw r
  LEFT JOIN ${catalog}.${bronze}.pos_transaction_lines_raw l ON l.transaction_id = r.original_transaction_id AND l.sku = r.sku
  QUALIFY row_number() OVER (PARTITION BY r.return_id ORDER BY l.line_number) = 1)
SELECT r.*, o.unified_customer_id, r.return_reason IN ('too_small', 'too_big') AS is_fit_related
FROM r JOIN ${catalog}.${silver}.orders o USING (order_id);

INSERT OVERWRITE ${catalog}.${silver}.carts BY NAME
WITH c AS (
  SELECT cart_id, first(session_id) AS session_id, max(ecom_customer_id) AS ecom_customer_id, first(cookie_id) AS anon_id,
         min(CAST(event_ts AS TIMESTAMP)) AS cart_created_ts, max(CAST(event_ts AS TIMESTAMP)) AS last_activity_ts,
         CAST(sum(CASE WHEN action = 'add' THEN quantity ELSE -quantity END) AS INT) AS item_count,
         CAST(sum(CASE WHEN action = 'add' THEN quantity * unit_price ELSE -quantity * unit_price END) AS DECIMAL(12, 2)) AS cart_value,
         array_distinct(collect_list(CASE WHEN action = 'add' THEN sku END)) AS skus
  FROM ${catalog}.${bronze}.ecom_carts_raw GROUP BY cart_id),
asof AS (SELECT max(CAST(event_ts AS TIMESTAMP)) AS t FROM ${catalog}.${bronze}.ecom_carts_raw)
SELECT c.cart_id, coalesce(m1.unified_customer_id, m2.unified_customer_id, m3.unified_customer_id) AS unified_customer_id,
       c.session_id, c.cart_created_ts, c.last_activity_ts, c.item_count, c.cart_value, c.skus,
       CASE WHEN o.order_id IS NOT NULL THEN 'converted'
            WHEN c.last_activity_ts < (SELECT t FROM asof) - INTERVAL 1 DAY THEN 'abandoned' ELSE 'active' END AS cart_status,
       o.order_id AS converted_order_id
FROM c
LEFT JOIN (SELECT session_id, min(order_id) AS order_id FROM ${catalog}.${bronze}.ecom_orders_raw GROUP BY 1) o USING (session_id)
LEFT JOIN idmap m1 ON m1.id_type = 'ecom_id' AND m1.id_value = c.ecom_customer_id
LEFT JOIN idmap m2 ON m2.id_type = 'cookie_id' AND m2.id_value = c.anon_id
LEFT JOIN idmap m3 ON m3.id_type = 'device_id' AND m3.id_value = c.anon_id;
