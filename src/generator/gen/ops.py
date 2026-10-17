"""Inventory snapshots and weather (Spark-native, hash-based randomness so values are stable per key)."""
from pyspark.sql import functions as F

from gen import refdata as R
from gen.config import N_DAYS, START, WET_AUTUMN, di, get_spark, truth, write_bronze

DCS = [("DC-USE", "US"), ("DC-USW", "US"), ("DC-USC", "US"), ("DC-UK", "UK"), ("DC-DE", "DE"), ("DC-CA", "CA")]


def u(*cols, k=0):
    return F.pmod(F.xxhash64(*cols, F.lit(k)), F.lit(1_000_000)) / 1e6


def run():
    spark = get_spark()
    day0 = F.lit(str(START)).cast("date")
    # ---------------- inventory: 90 days daily at DCs + current store snapshot
    p = truth("products").select("style_id", "weight", "launch_day", "disc_day", "division", "is_limited_drop")
    s = truth("skus").select("sku", "style_id", "size").join(p, "style_id") \
        .where(f"disc_day > {N_DAYS - 90} AND launch_day < {N_DAYS}")
    curve = F.when(F.col("size").isin("9", "9.5", "10", "10.5", "7.5", "8", "8.5", "M", "L"), 1.6) \
        .when(F.col("size").isin("8", "11", "11.5", "7", "S", "XL"), 1.1).otherwise(0.45)
    oos = truth("oos_skus").select("sku", "oos_start_day", "oos_end_day")
    dcs = spark.createDataFrame(DCS, "location_id string, country string")
    days = spark.range(N_DAYS - 90, N_DAYS).withColumnRenamed("id", "day")
    inv = (s.crossJoin(dcs).crossJoin(days).join(oos, "sku", "left")
           .where("day >= launch_day")
           .withColumn("base", F.col("weight") * 900 * curve * F.when(F.col("location_id").startswith("DC-US"), 1.0).otherwise(0.3))
           .withColumn("on_hand", F.when(F.col("day").between(F.col("oos_start_day"), F.col("oos_end_day")), F.lit(0))
                       .when(F.col("is_limited_drop"), F.lit(0))
                       .otherwise((F.col("base") * (0.4 + 1.2 * u("sku", "location_id", F.floor(F.col("day") / 7), k=1))
                                   + 3 * u("sku", "location_id", "day", k=2)).cast("int")))
           .withColumn("on_order", F.when(u("sku", "location_id", F.floor(F.col("day") / 14), k=3) < 0.3,
                                          (F.col("base") * 1.5).cast("int")).otherwise(0))
           .withColumn("reserved", (F.col("on_hand") * 0.08 * u("sku", "location_id", "day", k=4)).cast("int"))
           .withColumn("dt", F.date_add(day0, F.col("day").cast("int")))
           .select(F.date_format("dt", "yyyy-MM-dd").alias("snapshot_date"), "location_id",
                   F.lit("dc").alias("location_type"), "sku", F.col("on_hand").alias("on_hand_qty"),
                   F.col("on_order").alias("on_order_qty"), F.col("reserved").alias("reserved_qty"),
                   (F.col("dt").cast("timestamp") + F.expr("INTERVAL 5 HOURS")).alias("_event_ts")))
    st = truth("stores").select("store_id", "store_type")
    share = F.when(F.col("store_type") == "flagship", 0.6).when(F.col("store_type") == "standard", 0.22) \
        .when(F.col("store_type") == "outlet", 0.15).otherwise(0.06)
    sinv = (s.where(f"division <> 'Accessories' OR weight > 0.5").crossJoin(st)
            .where(u("store_id", "style_id", k=5) < share)
            .withColumn("on_hand", (F.col("weight") * 18 * curve * (0.2 + 1.3 * u("store_id", "sku", k=6))).cast("int"))
            .select(F.lit(str(START.fromordinal(START.toordinal() + N_DAYS - 1))).alias("snapshot_date"),
                    F.col("store_id").alias("location_id"), F.lit("store").alias("location_type"), "sku",
                    F.col("on_hand").alias("on_hand_qty"), F.lit(0).alias("on_order_qty"),
                    (F.col("on_hand") * 0.05).cast("int").alias("reserved_qty"),
                    F.lit(f"{START.fromordinal(START.toordinal() + N_DAYS - 1)} 05:00:00").cast("timestamp").alias("_event_ts")))
    write_bronze(inv.unionByName(sinv), "inventory_snapshots_raw", "manhattan_wms", "wms/inventory")

    # ---------------- weather: actuals for full window + 14-day forecast
    clim = spark.createDataFrame([(k, *v) for k, v in R.CLIMATE.items()],
                                 "climate string, mean_c double, amp_c double, rain_w double, rain_s double, mm double, snow boolean")
    geo = truth("geo").select("postal_code", "metro_id", "climate", "latitude").join(clim, "climate")
    wd = spark.range(0, N_DAYS + 14).withColumnRenamed("id", "day")
    w0, w1 = di(WET_AUTUMN[0]), di(WET_AUTUMN[1])
    doy = F.dayofyear(F.date_add(day0, F.col("day").cast("int")))
    season = F.cos(F.lit(2 * 3.14159265) * (doy - 200) / 365.0)          # +1 mid-July, -1 mid-January
    anomaly = (u("metro_id", F.floor(F.col("day") / 5), k=7) + u("metro_id", "day", k=8) + u("metro_id", "day", k=9) - 1.5) * 5.5
    wet = F.col("day").between(w0, w1) & F.col("climate").isin("pnw", "uk_maritime")
    rain_p = (F.col("rain_s") * (season + 1) / 2 + F.col("rain_w") * (1 - season) / 2) * F.when(wet, 1.35).otherwise(1.0)
    wx = (geo.crossJoin(wd)
          .withColumn("t_mean", F.col("mean_c") + F.col("amp_c") * season + anomaly + (u("postal_code", "day", k=10) - 0.5))
          .withColumn("rain", u("metro_id", "day", k=11) < rain_p)
          .withColumn("precip", F.when(F.col("rain"), F.round(-F.log(u("metro_id", "day", k=12) + 1e-6) * F.col("mm")
                                                              * F.when(wet, 1.4).otherwise(1.0), 1)).otherwise(0.0))
          .withColumn("snow", F.when(F.col("snow") & F.col("rain") & (F.col("t_mean") < 0.5), F.round(F.col("precip") * 8, 0)).otherwise(0.0))
          .withColumn("dt", F.date_add(day0, F.col("day").cast("int")))
          .select("postal_code", F.date_format("dt", "yyyy-MM-dd").alias("weather_date"),
                  F.round(F.col("t_mean") + 4 + 2 * u("metro_id", "day", k=13), 1).alias("temp_high_c"),
                  F.round(F.col("t_mean") - 4 - 2 * u("metro_id", "day", k=14), 1).alias("temp_low_c"),
                  F.col("precip").alias("precipitation_mm"), F.col("snow").alias("snowfall_mm"),
                  F.when(F.col("snow") > 0, "snow").when(F.col("precip") > 12, "heavy_rain").when(F.col("precip") > 0, "rain")
                  .when(u("metro_id", "day", k=15) < 0.45, "cloudy").otherwise("clear").alias("condition"),
                  (F.col("day") >= N_DAYS).alias("is_forecast"),
                  F.least(F.col("dt").cast("timestamp") + F.expr("INTERVAL 30 HOURS"),
                          F.lit(f"{START.fromordinal(START.toordinal() + N_DAYS - 1)} 06:00:00").cast("timestamp")).alias("_event_ts")))
    write_bronze(wx, "weather_daily_by_zip_raw", "tomorrow_io", "weather/daily")
