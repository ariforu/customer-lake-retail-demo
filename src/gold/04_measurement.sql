-- Gold measurement: attribution, campaign performance, holdout lift, cohorts, channel/store KPIs, identity stats

CREATE OR REPLACE TEMP VIEW asof AS
SELECT max(order_date) AS d, CAST(max(order_date) AS TIMESTAMP) + INTERVAL 1 DAY AS ts FROM ${catalog}.${silver}.orders;

CREATE OR REPLACE TEMP VIEW fx AS
SELECT * FROM VALUES ('USD', 1.0), ('CAD', 1.35), ('GBP', 0.80), ('EUR', 0.93) AS t(currency, rate);

CREATE OR REPLACE TEMP VIEW o_usd AS
SELECT o.*, CAST((o.subtotal_amount - o.discount_amount) / fx.rate AS DECIMAL(14, 2)) AS net_usd
FROM ${catalog}.${silver}.orders o JOIN fx USING (currency);

-- ---------------------------------------------------------------- multi-touch attribution (7-day lookback, last 12 months)
INSERT OVERWRITE ${catalog}.${gold}.campaign_attribution BY NAME
WITH od AS (SELECT order_id, unified_customer_id, order_ts, net_usd FROM o_usd
            WHERE unified_customer_id IS NOT NULL AND order_ts > (SELECT ts FROM asof) - INTERVAL 365 DAYS),
tp AS (SELECT unified_customer_id, touchpoint_ts, channel, campaign_id FROM ${catalog}.${silver}.marketing_touchpoints
       WHERE touchpoint_type IN ('click', 'visit', 'impression') AND campaign_id IS NOT NULL),
j AS (SELECT od.*, tp.touchpoint_ts, tp.channel, tp.campaign_id,
             row_number() OVER (PARTITION BY od.order_id ORDER BY tp.touchpoint_ts DESC) AS rn_last,
             count(*) OVER (PARTITION BY od.order_id) AS n
      FROM od JOIN tp ON tp.unified_customer_id = od.unified_customer_id
       AND tp.touchpoint_ts BETWEEN od.order_ts - INTERVAL 7 DAYS AND od.order_ts)
SELECT order_id, unified_customer_id, order_ts, net_usd AS order_revenue, campaign_id, channel, touchpoint_ts,
       'last_touch' AS attribution_model, 1.0 AS attribution_weight, net_usd AS attributed_revenue
FROM j WHERE rn_last = 1
UNION ALL
SELECT order_id, unified_customer_id, order_ts, net_usd, campaign_id, channel, touchpoint_ts, 'linear', 1.0 / n,
       CAST(net_usd / n AS DECIMAL(12, 2))
FROM j;

-- ---------------------------------------------------------------- campaign performance (daily)
INSERT OVERWRITE ${catalog}.${gold}.campaign_performance BY NAME
WITH me AS (
  SELECT campaign_id, channel, to_date(event_ts) AS report_date,
         count(DISTINCT unified_customer_id) FILTER (WHERE event_type = 'send') AS audience_size,
         count_if(event_type = 'send') AS sends, count_if(event_type = 'bounce') AS bounces,
         count(DISTINCT send_id) FILTER (WHERE event_type = 'open') AS opens,
         count(DISTINCT send_id) FILTER (WHERE event_type = 'click') AS clicks,
         count_if(event_type = 'unsubscribe') AS unsubscribes
  FROM ${catalog}.${silver}.message_events GROUP BY 1, 2, 3),
pm AS (SELECT campaign_id, platform AS channel, report_date, sum(impressions) AS impressions, sum(clicks) AS clicks,
              sum(spend) AS spend FROM ${catalog}.${silver}.paid_media_performance GROUP BY 1, 2, 3),
att AS (SELECT campaign_id, to_date(touchpoint_ts) AS report_date, count(DISTINCT order_id) AS conversions,
               sum(attributed_revenue) AS rev FROM ${catalog}.${gold}.campaign_attribution WHERE attribution_model = 'last_touch'
        GROUP BY 1, 2),
mg AS (SELECT a.campaign_id, to_date(a.touchpoint_ts) AS report_date, sum(l.line_margin * a.attribution_weight) AS margin
       FROM ${catalog}.${gold}.campaign_attribution a JOIN ${catalog}.${silver}.order_lines l USING (order_id)
       WHERE a.attribution_model = 'last_touch' GROUP BY 1, 2),
u AS (
  SELECT campaign_id, channel, report_date, audience_size, sends, sends - bounces AS delivered, opens, clicks, unsubscribes,
         CAST(NULL AS BIGINT) AS impressions, CAST(NULL AS DECIMAL(14, 2)) AS spend FROM me
  UNION ALL
  SELECT campaign_id, channel, report_date, NULL, NULL, NULL, NULL, clicks, NULL, impressions, CAST(spend AS DECIMAL(14, 2)) FROM pm)
SELECT u.campaign_id, c.campaign_name, c.segment_id, u.channel, u.report_date, u.audience_size, u.sends, u.delivered, u.opens,
       u.clicks, u.unsubscribes, u.impressions, u.spend, coalesce(att.conversions, 0) AS conversions,
       CAST(coalesce(att.rev, 0) AS DECIMAL(14, 2)) AS attributed_revenue, CAST(coalesce(mg.margin, 0) AS DECIMAL(14, 2)) AS attributed_margin,
       round(u.opens / nullif(u.delivered, 0), 4) AS open_rate,
       round(u.clicks / nullif(coalesce(u.delivered, u.impressions), 0), 5) AS click_rate,
       round(coalesce(att.conversions, 0) / nullif(coalesce(u.delivered, u.clicks), 0), 5) AS conversion_rate,
       round(coalesce(att.rev, 0) / nullif(u.spend, 0), 3) AS roas
FROM u LEFT JOIN ${catalog}.${silver}.campaigns c USING (campaign_id)
LEFT JOIN att USING (campaign_id, report_date) LEFT JOIN mg USING (campaign_id, report_date);

-- ---------------------------------------------------------------- holdout lift: top batch email campaigns vs. contactable-but-not-sent
INSERT OVERWRITE ${catalog}.${gold}.campaign_holdout_results BY NAME
WITH top AS (SELECT campaign_id, start_date FROM ${catalog}.${silver}.campaigns
             WHERE channel = 'email' AND campaign_type = 'batch' AND start_date IS NOT NULL),
sz AS (SELECT campaign_id, count(*) AS n FROM ${catalog}.${silver}.message_events WHERE event_type = 'send' GROUP BY 1),
pick AS (SELECT t.* FROM top t JOIN sz USING (campaign_id) ORDER BY sz.n DESC LIMIT 20),
pool AS (SELECT unified_customer_id FROM ${catalog}.${gold}.customer_profile WHERE email_contactable),
test AS (SELECT DISTINCT p.campaign_id, p.start_date, m.unified_customer_id FROM pick p
         JOIN ${catalog}.${silver}.message_events m ON m.campaign_id = p.campaign_id AND m.event_type = 'send' AND m.unified_customer_id IS NOT NULL),
grp AS (SELECT p.campaign_id, p.start_date, pool.unified_customer_id, t.unified_customer_id IS NOT NULL AS is_test
        FROM pick p CROSS JOIN pool LEFT JOIN test t ON t.campaign_id = p.campaign_id AND t.unified_customer_id = pool.unified_customer_id),
conv AS (SELECT g.campaign_id, g.is_test, g.unified_customer_id, max(o.order_id IS NOT NULL) AS converted, sum(coalesce(o.net_usd, 0)) AS rev
         FROM grp g LEFT JOIN o_usd o ON o.unified_customer_id = g.unified_customer_id
          AND o.order_date > g.start_date AND o.order_date <= g.start_date + INTERVAL 7 DAYS
         GROUP BY 1, 2, 3),
s AS (SELECT campaign_id,
             count_if(is_test) AS nt, count_if(NOT is_test) AS nc,
             avg(CASE WHEN is_test THEN CAST(converted AS INT) END) AS ct, avg(CASE WHEN NOT is_test THEN CAST(converted AS INT) END) AS cc,
             avg(CASE WHEN is_test THEN rev END) AS rt, avg(CASE WHEN NOT is_test THEN rev END) AS rc
      FROM conv GROUP BY 1)
SELECT campaign_id, CAST(NULL AS STRING) AS segment_id, 7 AS measurement_window_days, nt AS test_customers, nc AS control_customers,
       round(ct, 5) AS test_conversion_rate, round(cc, 5) AS control_conversion_rate,
       CAST(rt AS DECIMAL(12, 2)) AS test_revenue_per_customer, CAST(rc AS DECIMAL(12, 2)) AS control_revenue_per_customer,
       round(try_divide(ct - cc, cc) * 100, 2) AS incremental_lift_pct, CAST((rt - rc) * nt AS DECIMAL(14, 2)) AS incremental_revenue,
       round(2 / (1 + exp(1.702 * abs(coalesce(try_divide(ct - cc, sqrt(((ct * nt + cc * nc) / (nt + nc)) * (1 - (ct * nt + cc * nc) / (nt + nc)) * (1.0 / nt + 1.0 / nc))), 0)))), 6) AS p_value,
       current_timestamp() AS computed_at
FROM s WHERE nt > 0 AND nc > 0;

-- ---------------------------------------------------------------- acquisition cohorts
INSERT OVERWRITE ${catalog}.${gold}.cohort_retention BY NAME
WITH f AS (SELECT unified_customer_id, CASE WHEN min_by(channel, order_ts) = 'store' THEN 'store' ELSE 'online' END AS ch,
                  trunc(min(order_date), 'MM') AS cm
           FROM o_usd WHERE unified_customer_id IS NOT NULL GROUP BY 1),
a AS (SELECT f.cm, f.ch, CAST(months_between(trunc(o.order_date, 'MM'), f.cm) AS INT) AS k,
             count(DISTINCT o.unified_customer_id) AS active, sum(o.net_usd) AS rev
      FROM o_usd o JOIN f USING (unified_customer_id) GROUP BY 1, 2, 3),
sz AS (SELECT cm, ch, count(*) AS n FROM f GROUP BY 1, 2)
SELECT a.cm AS cohort_month, a.ch AS acquisition_channel, a.k AS months_since_acquisition, sz.n AS cohort_size,
       a.active AS active_customers, round(a.active / sz.n, 4) AS retention_rate,
       CAST(sum(a.rev) OVER (PARTITION BY a.cm, a.ch ORDER BY a.k) AS DECIMAL(14, 2)) AS cumulative_revenue,
       CAST(sum(a.rev) OVER (PARTITION BY a.cm, a.ch ORDER BY a.k) / sz.n AS DECIMAL(12, 2)) AS revenue_per_customer
FROM a JOIN sz USING (cm, ch);

-- ---------------------------------------------------------------- channel KPIs (daily)
INSERT OVERWRITE ${catalog}.${gold}.channel_kpis_daily BY NAME
WITH first_o AS (SELECT unified_customer_id, min(order_date) AS d FROM o_usd WHERE unified_customer_id IS NOT NULL GROUP BY 1),
od AS (SELECT o.order_date AS report_date, o.channel, count(*) AS orders, count(DISTINCT o.unified_customer_id) AS customers,
              count(DISTINCT CASE WHEN f.d = o.order_date THEN o.unified_customer_id END) AS new_customers, sum(o.net_usd) AS revenue
       FROM o_usd o LEFT JOIN first_o f USING (unified_customer_id) GROUP BY 1, 2),
ss AS (SELECT to_date(session_start_ts) AS report_date, CASE WHEN platform = 'web' THEN 'web' ELSE 'app' END AS channel,
              count(*) AS sessions FROM ${catalog}.${silver}.sessions GROUP BY 1, 2),
rt AS (SELECT to_date(l.order_ts) AS report_date, o.channel, count(r.return_id) / count(*) AS rr
       FROM ${catalog}.${silver}.order_lines l JOIN ${catalog}.${silver}.orders o USING (order_id)
       LEFT JOIN ${catalog}.${silver}.returns r ON r.order_id = l.order_id AND r.line_number = l.line_number GROUP BY 1, 2),
ca AS (SELECT to_date(cart_created_ts) AS report_date, 'web' AS channel, avg(CASE WHEN cart_status = 'abandoned' THEN 1.0 ELSE 0 END) AS car
       FROM ${catalog}.${silver}.carts GROUP BY 1)
SELECT od.report_date, od.channel, ss.sessions, od.orders, od.customers, od.new_customers, CAST(od.revenue AS DECIMAL(14, 2)) AS revenue,
       CAST(od.revenue / od.orders AS DECIMAL(12, 2)) AS aov, round(od.orders / nullif(ss.sessions, 0), 4) AS conversion_rate,
       round(rt.rr, 4) AS return_rate, round(ca.car, 4) AS cart_abandonment_rate
FROM od LEFT JOIN ss USING (report_date, channel) LEFT JOIN rt USING (report_date, channel) LEFT JOIN ca USING (report_date, channel);

-- ---------------------------------------------------------------- store KPIs (daily)
INSERT OVERWRITE ${catalog}.${gold}.store_kpis_daily BY NAME
WITH st AS (SELECT o.order_date AS report_date, o.store_id, count(*) AS transactions, count(DISTINCT o.unified_customer_id) AS customers,
                   avg(CASE WHEN o.unified_customer_id IS NOT NULL THEN 1.0 ELSE 0 END) AS ident, sum(o.net_usd) AS revenue,
                   sum(o.item_count) AS units
            FROM o_usd o WHERE o.channel = 'store' GROUP BY 1, 2),
rt AS (SELECT to_date(r.return_ts) AS report_date, o.store_id, count(*) AS n FROM ${catalog}.${silver}.returns r
       JOIN ${catalog}.${silver}.orders o USING (order_id) WHERE o.channel = 'store' GROUP BY 1, 2),
ga AS (SELECT to_date(CAST(analysis_ts AS TIMESTAMP)) AS report_date, store_id, count(*) AS n FROM ${catalog}.${bronze}.gait_analysis_raw GROUP BY 1, 2),
rc AS (SELECT to_date(checkin_ts) AS report_date, store_id, count(*) AS n FROM ${catalog}.${silver}.run_club_attendance GROUP BY 1, 2),
bp AS (SELECT order_date AS report_date, store_id, count(*) AS n FROM ${catalog}.${silver}.orders WHERE fulfillment_type = 'bopis' GROUP BY 1, 2)
SELECT st.report_date, st.store_id, s.region, st.transactions, st.customers, round(st.ident, 4) AS identified_customer_pct,
       CAST(st.revenue AS DECIMAL(14, 2)) AS revenue, st.units, CAST(st.revenue / st.transactions AS DECIMAL(12, 2)) AS aov,
       coalesce(rt.n, 0) AS returns, CAST(coalesce(ga.n, 0) AS INT) AS gait_analyses, CAST(coalesce(rc.n, 0) AS INT) AS run_club_checkins,
       coalesce(bp.n, 0) AS bopis_orders
FROM st JOIN ${catalog}.${silver}.stores s USING (store_id) LEFT JOIN rt USING (report_date, store_id)
LEFT JOIN ga USING (report_date, store_id) LEFT JOIN rc USING (report_date, store_id) LEFT JOIN bp USING (report_date, store_id);

-- ---------------------------------------------------------------- identity resolution stats
INSERT INTO ${catalog}.${gold}.identity_resolution_stats BY NAME
WITH r AS (SELECT DISTINCT source_record_key, source_system, unified_customer_id FROM ${catalog}.${silver}.identity_graph),
cs AS (SELECT unified_customer_id, count(*) AS n FROM r GROUP BY 1),
src AS (SELECT r.source_system, avg(CASE WHEN cs.n > 1 THEN 1.0 ELSE 0 END) AS m FROM r JOIN cs USING (unified_customer_id) GROUP BY 1)
SELECT current_timestamp() AS run_ts,
       (SELECT count(*) FROM ${catalog}.${silver}.customer_source_records) AS source_records,
       (SELECT count(*) FROM ${catalog}.${silver}.customers) AS unified_customers,
       (SELECT count(*) FROM ${catalog}.${silver}.customers WHERE is_known) AS known_customers,
       (SELECT count(DISTINCT anonymous_id) FROM ${catalog}.${silver}.web_app_events WHERE unified_customer_id IS NULL) AS anonymous_profiles,
       (SELECT count(*) FROM ${catalog}.${silver}.customer_source_records) - (SELECT count(*) FROM ${catalog}.${silver}.customers) AS duplicates_collapsed,
       round(1 - (SELECT count(*) FROM ${catalog}.${silver}.customers) / (SELECT count(*) FROM ${catalog}.${silver}.customer_source_records), 4) AS dedup_rate,
       round((SELECT count(*) FROM ${catalog}.${silver}.identity_graph) / (SELECT count(*) FROM ${catalog}.${silver}.customers), 2) AS avg_ids_per_customer,
       (SELECT round(avg(CASE WHEN n > 1 THEN 1.0 ELSE 0 END), 4) FROM cs) AS multi_source_customer_pct,
       (SELECT count(*) FROM ${catalog}.${silver}.identity_match_pairs WHERE match_type = 'deterministic') AS deterministic_matches,
       (SELECT count(*) FROM ${catalog}.${silver}.identity_match_pairs WHERE match_type = 'probabilistic') AS probabilistic_matches,
       (SELECT map_from_entries(collect_list(struct(source_system, round(m, 4)))) FROM src) AS match_rate_by_source;
