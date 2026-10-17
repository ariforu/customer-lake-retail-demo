"""Validate that the bronze data carries the intended business stories (reads bronze only, plus truth for identity)."""
import warnings

warnings.filterwarnings("ignore")
from gen.config import BRONZE, TRUTH, get_spark

s = get_spark()
B, T = BRONZE, TRUTH


def show(title, sql):
    print(f"\n### {title}")
    print(s.sql(sql).toPandas().to_string(index=False))


print("### Row counts (all bronze tables)")
tables = [r.tableName for r in s.sql(f"SHOW TABLES IN {B}").collect()]
counts = {t: s.table(f"{B}.{t}").count() for t in tables}
for t in sorted(counts):
    print(f"  {t:36s} {counts[t]:>14,d}")
print(f"  {'TOTAL':36s} {sum(counts.values()):>14,d}")

show("1. Identity fragmentation: source profiles vs real people", f"""
SELECT (SELECT count(*) FROM {T}.persons) AS real_people,
       (SELECT count(*) FROM {B}.ecom_customers_raw) + (SELECT count(*) FROM {B}.pos_customers_raw)
     + (SELECT count(*) FROM {B}.loyalty_members_raw) + (SELECT count(DISTINCT app_user_id) FROM {B}.app_users_raw)
     + (SELECT count(DISTINCT fitness_user_id) FROM {B}.fitness_shoe_registrations_raw) AS source_profiles""")

show("2. CloudStride 3 online return rate + 'too small' share by month", f"""
WITH l AS (SELECT l.order_id, l.line_number, o.order_ts FROM {B}.ecom_order_lines_raw l
           JOIN {B}.ecom_orders_raw o USING (order_id) JOIN {B}.skus_raw k USING (sku)
           JOIN {B}.products_raw p USING (style_id) WHERE p.style_name LIKE 'CloudStride 3%' AND p.category = 'Running')
SELECT substr(order_ts, 1, 7) month, count(*) lines, round(avg(CASE WHEN r.return_id IS NOT NULL THEN 1 ELSE 0 END), 3) return_rate,
       round(avg(CASE WHEN r.return_reason_code = 'TOO_SMALL' THEN 1 ELSE 0 END), 3) too_small_rate
FROM l LEFT JOIN {B}.ecom_returns_raw r USING (order_id, line_number) GROUP BY 1 ORDER BY 1""")

show("3. CloudStride 3 review ratings before/after size guidance (2026-07-15)", f"""
SELECT CASE WHEN submitted_ts < '2026-07-25' THEN 'pre_fix' ELSE 'post_fix' END period, count(*) n, round(avg(rating), 2) avg_rating,
       round(avg(CASE WHEN fit_rating = 'runs_small' THEN 1 ELSE 0 END), 2) runs_small_share
FROM {B}.product_reviews_raw r JOIN {B}.products_raw p USING (style_id) WHERE p.style_name LIKE 'CloudStride 3%' GROUP BY 1""")

show("4. Black Friday 2025 over-mailing -> unsubscribes per week", f"""
SELECT date_trunc('week', to_timestamp(unsubscribe_ts)) wk, count(*) unsubs FROM {B}.esp_unsubscribes_raw
WHERE unsubscribe_ts BETWEEN '2025-10-20' AND '2026-01-20' GROUP BY 1 ORDER BY 1""")

show("5. Sizing tickets spike + resolution time", f"""
SELECT substr(created_ts, 1, 7) month, count(*) sizing_tickets,
       round(avg((unix_timestamp(to_timestamp(resolved_ts)) - unix_timestamp(to_timestamp(created_ts))) / 3600), 1) avg_res_h
FROM {B}.service_tickets_raw WHERE category = 'sizing_fit' AND created_ts >= '2025-11' GROUP BY 1 ORDER BY 1""")

show("6. Reseller clusters: shipping addresses with many accounts on drop days", f"""
SELECT shipping_address, count(DISTINCT ecom_customer_id) accounts, count(*) orders FROM {B}.ecom_orders_raw o
JOIN {B}.ecom_order_lines_raw l USING (order_id) JOIN {B}.skus_raw k USING (sku) JOIN {B}.product_drops_raw d USING (style_id)
GROUP BY 1 HAVING count(DISTINCT ecom_customer_id) >= 4 ORDER BY orders DESC LIMIT 8""")

show("7. Monthly revenue seasonality (all channels, USD)", f"""
WITH o AS (SELECT substr(order_ts, 1, 7) m, total_amount FROM {B}.ecom_orders_raw WHERE currency = 'USD'
           UNION ALL SELECT concat(substr(transaction_ts, 7, 4), '-', substr(transaction_ts, 1, 2)), total_amount
           FROM {B}.pos_transactions_raw WHERE currency = 'USD')
SELECT m, round(sum(total_amount) / 1e6, 2) revenue_musd, count(*) orders FROM o GROUP BY 1 ORDER BY 1""")

show("8. Web funnel (clickstream window)", f"""
SELECT count(DISTINCT session_id) sessions,
       count(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) cart_sessions,
       count(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) purchase_sessions,
       round(count(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) / count(DISTINCT session_id), 4) cvr
FROM {B}.web_clickstream_raw""")

show("9. Run-app mileage on shoes registered >= 1 year ago (worn-out shoe signal)", f"""
WITH m AS (SELECT a.shoe_registration_id, sum(a.distance_km) * 0.621 miles FROM {B}.fitness_activities_raw a
           WHERE a.shoe_registration_id IS NOT NULL GROUP BY 1)
SELECT CASE WHEN miles < 200 THEN '<200' WHEN miles < 400 THEN '200-400' WHEN miles < 600 THEN '400-600' ELSE '600+' END bucket,
       count(*) shoes FROM m GROUP BY 1 ORDER BY 1""")

show("10. Rainy autumn 2025 (Seattle / London) vs 2024 rain days Oct-Nov", f"""
SELECT z.city, year(to_date(w.weather_date)) yr, round(avg(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END), 2) rain_day_share
FROM {B}.weather_daily_by_zip_raw w JOIN {B}.zip_demographics_raw z USING (postal_code)
WHERE z.city IN ('Seattle', 'London') AND month(to_date(w.weather_date)) IN (10, 11) GROUP BY 1, 2 ORDER BY 1, 2""")
