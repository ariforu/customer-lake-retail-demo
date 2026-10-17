-- Silver reference / product / location / external tables

INSERT OVERWRITE ${catalog}.${silver}.products BY NAME
SELECT style_id, style_name, division, category, sub_category, sport, gender, split(technology, ',') AS technology,
       description, msrp, unit_cost, round((msrp - unit_cost) / msrp, 4) AS margin_pct, to_date(launch_date) AS launch_date,
       lifecycle_status, is_limited_drop, collab_partner, image_url
FROM ${catalog}.${bronze}.products_raw
QUALIFY row_number() OVER (PARTITION BY style_id ORDER BY _ingested_at DESC) = 1;

INSERT OVERWRITE ${catalog}.${silver}.skus BY NAME
SELECT sku, style_id, colorway, color_family, size,
       CASE WHEN size_system IN ('US_M', 'US_W') THEN try_cast(size AS DOUBLE) END AS size_us, width, upc
FROM ${catalog}.${bronze}.skus_raw
QUALIFY row_number() OVER (PARTITION BY sku ORDER BY _ingested_at DESC) = 1;

INSERT OVERWRITE ${catalog}.${silver}.product_drops BY NAME
SELECT drop_id, style_id, drop_name, CAST(release_ts AS TIMESTAMP) AS release_ts, units_available, release_channel,
       is_raffle, member_early_access
FROM ${catalog}.${bronze}.product_drops_raw;

INSERT OVERWRITE ${catalog}.${silver}.price_history BY NAME
SELECT sku, channel, price_type, CAST(price AS DECIMAL(10, 2)) AS price, to_date(effective_from) AS effective_from,
       to_date(effective_to) AS effective_to
FROM ${catalog}.${bronze}.price_history_raw;

INSERT OVERWRITE ${catalog}.${silver}.inventory_daily BY NAME
SELECT to_date(snapshot_date) AS snapshot_date, location_id, location_type, sku, on_hand_qty, on_order_qty, reserved_qty,
       greatest(on_hand_qty - reserved_qty, 0) AS available_qty
FROM ${catalog}.${bronze}.inventory_snapshots_raw;

INSERT OVERWRITE ${catalog}.${silver}.stores BY NAME
SELECT store_id, store_name, store_type, address_line1, city, state, postal_code, country, latitude, longitude, region,
       to_date(open_date) AS open_date, sq_ft, has_gait_lab, hosts_run_club
FROM ${catalog}.${bronze}.stores_raw;

INSERT OVERWRITE ${catalog}.${silver}.weather_daily BY NAME
SELECT postal_code, to_date(weather_date) AS weather_date, temp_high_c, temp_low_c, precipitation_mm, snowfall_mm, condition,
       is_forecast, precipitation_mm >= 2.0 AS is_rain_day, temp_high_c < 3.0 AS is_cold_snap
FROM ${catalog}.${bronze}.weather_daily_by_zip_raw;

INSERT OVERWRITE ${catalog}.${silver}.sports_calendar BY NAME
SELECT calendar_event_id, event_name, event_category, sport, region, to_date(start_date) AS start_date,
       to_date(end_date) AS end_date, description
FROM ${catalog}.${bronze}.sports_calendar_raw;

INSERT OVERWRITE ${catalog}.${silver}.zip_demographics BY NAME
SELECT postal_code, city, state, population, median_household_income,
       CASE WHEN median_household_income >= 110000 THEN 'high' WHEN median_household_income >= 65000 THEN 'upper_middle'
            WHEN median_household_income >= 40000 THEN 'middle' ELSE 'lower' END AS income_band,
       median_age, pct_households_with_kids, urbanicity
FROM ${catalog}.${bronze}.zip_demographics_raw;
