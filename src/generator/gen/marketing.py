"""ESP (email/SMS/push) sends + engagement + unsubscribes, paid media, platform conversions, affiliate redemptions."""
from datetime import date, timedelta

import numpy as np
import pandas as pd

from gen.config import (BF_MAIL_END, BF_MAIL_START, ESP_START, N_DAYS, SEED, START, di, get_spark, md5hex,
                        save_truth, to_ts, truth, write_bronze)
from gen.profiles import app_devices, n_push_tokens, push_token

ESP0 = di(ESP_START)
BF0, BF1 = di(BF_MAIL_START), di(BF_MAIL_END)
FAR = 99999
SCHEMA = """rec string, send_id string, campaign_id string, campaign_name string, channel string,
recipient_email string, recipient_phone string, push_token string, template_id string, variant string,
ts timestamp, event_type string, link_url string, device_type string, bounce_type string, reason string,
person_idx long"""
COLS = [c.split()[0] for c in SCHEMA.replace("\n", " ").split(", ")]
LINKS = ["https://www.strideforge.com/us/c/new-arrivals", "https://www.strideforge.com/us/c/running-shoes",
         "https://www.strideforge.com/us/p/cloudstride-3", "https://www.strideforge.com/us/c/sale",
         "https://www.strideforge.com/us/account/rewards", "https://www.strideforge.com/us/c/trail"]


def gen_esp(batches, ctx):
    camps = ctx["camps"]
    trig = ctx["trig"]
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 31 + int(b["person_idx"].iloc[0]))
        rows = []
        for p in b.itertuples():
            sports = {p.primary_sport, p.secondary_sport}
            e_from = max(p.acq_day, ESP0)
            e_to = min(p.email_optout_day, N_DAYS - 1) if p.email_optin else -1
            if p.email_bad:
                e_to = min(e_to, e_from + 21)
            low = p.email_engagement < 0.12
            sends = []   # (day, sec, campaign_id, name, channel, triggered)
            for c in camps:
                d = c[0]
                ch = c[3]
                if ch == "email":
                    if not (e_from <= d <= e_to):
                        continue
                elif ch == "sms":
                    if not (p.sms_optin and max(p.acq_day, ESP0) <= d):
                        continue
                else:
                    if not (p.is_app_user and p.push_enabled and max(p.app_day, ESP0) <= d):
                        continue
                theme, sp = c[4], c[5]
                if theme == "newsletter":
                    ok = rng.random() < (0.5 if low else 0.95)
                elif theme == "sport":
                    ok = sp in sports or rng.random() < 0.12
                elif theme == "black_friday":
                    ok = True
                elif theme == "drop":
                    ok = p.is_sneakerhead or p.primary_sport in ("Lifestyle", "Basketball") or rng.random() < 0.08
                elif theme in ("flash_sale", "new_arrivals"):
                    ok = rng.random() < 0.8
                else:
                    ok = (sp is None or sp in sports or rng.random() < 0.25) and rng.random() < 0.85
                if ok:
                    sends.append((d, c[6], c[1], c[2], ch, False))
            if p.email_optin:
                if p.acq_day >= ESP0:
                    for k, off in enumerate((0, 3, 7)):
                        sends.append((p.acq_day + off, 40000 + k, f"TRG-WELCOME-{k + 1}", trig[f"TRG-WELCOME-{k + 1}"], "email", True))
                od = p.order_days if isinstance(p.order_days, (list, np.ndarray)) else []
                for d in od:
                    sends.append((int(d) + 1, int(rng.integers(30000, 80000)), "TRG-POSTPURCHASE", trig["TRG-POSTPURCHASE"], "email", True))
                    if rng.random() < 0.5:
                        sends.append((int(d) + 14, 43200, "TRG-REVIEW", trig["TRG-REVIEW"], "email", True))
                rd = p.run_days if isinstance(p.run_days, (list, np.ndarray)) else []
                for d in rd:
                    sends.append((int(d) + 180, 36000, "TRG-REPLENISH-RUN", trig["TRG-REPLENISH-RUN"], "email", True))
                if len(od):
                    last = int(max(od))
                    if last + 90 < N_DAYS and not any(last < x <= last + 90 for x in od):
                        sends.append((last + 90, 39600, "TRG-WINBACK-90", trig["TRG-WINBACK-90"], "email", True))
                bd = p.birth_date
                for y in (2025, 2026):
                    try:
                        dd = di(date(y, bd.month, bd.day))
                    except ValueError:
                        dd = di(date(y, 3, 1))
                    sends.append((dd, 28800, "TRG-BIRTHDAY", trig["TRG-BIRTHDAY"], "email", True))
            sends.sort(key=lambda s: (s[0], s[1]))
            bf_seen = 0
            last_send = None
            plat = None
            tok = None
            if p.is_app_user and p.push_enabled:
                dv = app_devices(p)[0]
                plat = dv[3]
                tok = push_token(p, dv[0], n_push_tokens(p, dv[1], dv[2]) - 1, plat)
            unsub_done = False
            for d, sec, cid, cname, ch, is_trig in sends:
                if d < ESP0 or d >= N_DAYS:
                    continue
                if ch == "email" and not (e_from <= d <= e_to):
                    continue
                if ch == "email" and not unsub_done and p.email_optout_day < FAR and d >= p.email_optout_day:
                    continue
                if not is_trig:
                    sec = 36000 + int(rng.integers(0, 1800)) if ch != "push" else 64800 + int(rng.integers(0, 1800))
                t = to_ts(d, sec)
                sid = md5hex(f"{p.person_idx}-{cid}-{d}-{sec}")[:20]
                variant = "A" if rng.random() < 0.5 else "B"
                rows.append(("send", sid, cid, cname, ch, p.email if ch == "email" else None,
                             p.phone if ch == "sms" else None, tok if ch == "push" else None,
                             f"TPL-{cid.split('-')[0]}-{cid[-4:]}", variant, t, None, None, None, None, None, p.person_idx))
                if ch == "email":
                    last_send = (d, sec, sid, cid)
                    if p.email_bad:
                        rows.append(("eng", sid, cid, None, ch, p.email, None, None, None, None, to_ts(d, sec + 5), "bounce",
                                     None, None, "hard", None, p.person_idx))
                        continue
                    if rng.random() < 0.02:
                        rows.append(("eng", sid, cid, None, ch, p.email, None, None, None, None, to_ts(d, sec + 9), "bounce",
                                     None, None, "soft", None, p.person_idx))
                        continue
                    po = p.email_engagement * (1.6 if is_trig else 1.0)
                    if abs((sec // 3600) - p.send_hour) <= 2:
                        po *= 1.25
                    if BF0 <= d <= BF1:
                        bf_seen += 1
                        po *= 0.55 if bf_seen > 4 else 1.0
                    if p.end_day < d:
                        po *= 0.5
                    if rng.random() < min(po, 0.9):
                        delay = int(rng.exponential(5 * 3600 if abs((sec // 3600) - p.send_hour) > 2 else 2 * 3600))
                        dev = "mobile" if rng.random() < 0.68 else "desktop"
                        rows.append(("eng", sid, cid, None, ch, p.email, None, None, None, None, to_ts(d, sec + delay),
                                     "open", None, dev, None, None, p.person_idx))
                        if rng.random() < 0.25:
                            rows.append(("eng", sid, cid, None, ch, p.email, None, None, None, None,
                                         to_ts(d, sec + delay + int(rng.exponential(30000))), "open", None, dev, None, None, p.person_idx))
                        if rng.random() < (0.2 if is_trig else 0.12):
                            rows.append(("eng", sid, cid, None, ch, p.email, None, None, None, None,
                                         to_ts(d, sec + delay + int(rng.integers(5, 300))), "click",
                                         LINKS[rng.integers(len(LINKS))] + f"?utm_source=email&utm_campaign={cid}",
                                         dev, None, None, p.person_idx))
                    if rng.random() < 0.0003:
                        rows.append(("eng", sid, cid, None, ch, p.email, None, None, None, None, to_ts(d, sec + 7200),
                                     "complaint", None, None, None, None, p.person_idx))
                elif ch == "sms":
                    if rng.random() < 0.045:
                        rows.append(("eng", sid, cid, None, ch, None, p.phone, None, None, None, to_ts(d, sec + int(rng.exponential(1800))),
                                     "click", LINKS[3], "mobile", None, None, p.person_idx))
                else:
                    if rng.random() < 0.06:
                        rows.append(("eng", sid, cid, None, ch, None, None, tok, None, None, to_ts(d, sec + int(rng.exponential(3600))),
                                     "open", None, plat, None, None, p.person_idx))
            if p.email_optin and ESP0 <= p.email_optout_day < N_DAYS and last_send is not None:
                d, sec, sid, cid = last_send
                ut = to_ts(p.email_optout_day, max(sec, 3600) + int(rng.integers(600, 20000)))
                reason = str(rng.choice(["too_many_emails", "not_relevant", "no_longer_interested", "never_signed_up", None],
                                        p=[.45, .2, .2, .05, .1] if BF0 <= p.email_optout_day <= BF1 + 3 else [.2, .3, .3, .05, .15]))
                rows.append(("eng", sid, cid, None, "email", p.email, None, None, None, None, ut, "unsubscribe", None,
                             None, None, None, p.person_idx))
                rows.append(("unsub", sid, cid, None, "email", p.email, None, None, None, None, ut, None, None, None,
                             None, None if reason == "None" else reason, p.person_idx))
            if len(rows) > 300_000:
                yield _frame(rows)
                rows = []
        if rows:
            yield _frame(rows)


def _frame(rows):
    df = pd.DataFrame(rows, columns=COLS)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def run():
    from pyspark.sql import functions as F
    spark = get_spark()
    camps = truth("esp_campaigns").toPandas()
    batch = camps[camps.campaign_type == "batch"].sort_values("send_day")
    ctx = dict(camps=[(int(r.send_day), r.campaign_id, r.campaign_name, r.channel, r.theme,
                       r.sport if isinstance(r.sport, str) else None, 36000) for r in batch.itertuples()],
               trig={r.campaign_id: r.campaign_name for r in camps[camps.campaign_type == "triggered"].itertuples()})
    L = truth("order_lines").where(f"person_idx IS NOT NULL AND order_day >= {ESP0 - 200}")
    od = (L.groupBy("person_idx").agg(
        F.array_distinct(F.collect_list(F.when(F.col("order_day") >= ESP0, F.col("order_day")))).alias("order_days"),
        F.array_distinct(F.collect_list(F.when(F.col("cat_code").isin("RUN", "TRL") & ~F.col("returned"),
                                               F.col("order_day")))).alias("run_days")))
    P = (truth("persons").where("email_optin OR sms_optin OR (is_app_user AND push_enabled)")
         .join(od, "person_idx", "left").repartition(192, "person_idx").sortWithinPartitions("person_idx"))
    print("marketing: generating ESP sends / engagement", flush=True)
    E = save_truth(P.mapInPandas(lambda it: gen_esp(it, ctx), SCHEMA), "esp_events")
    iso = lambda c: F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    write_bronze(E.where("rec = 'send'").select("send_id", "campaign_id", "campaign_name", "channel", "recipient_email",
                                                "recipient_phone", "push_token", "template_id", "variant",
                                                iso(F.col("ts")).alias("send_ts"), F.col("ts").alias("_event_ts")),
                 "esp_sends_raw", "braze", "braze/currents/sends")
    write_bronze(E.where("rec = 'eng'").select(
        F.substring(F.sha2(F.concat_ws("|", "send_id", "event_type", F.col("ts").cast("string")), 256), 1, 24).alias("engagement_id"),
        "send_id", "campaign_id", "channel", "recipient_email", "recipient_phone", "event_type",
        iso(F.col("ts")).alias("event_ts"), "link_url", "device_type", "bounce_type", F.col("ts").alias("_event_ts")),
        "esp_engagement_raw", "braze", "braze/currents/engagement")
    write_bronze(E.where("rec = 'unsub'").select(
        F.concat(F.lit("UNS-"), F.substring("send_id", 1, 14)).alias("unsubscribe_id"), "channel", "recipient_email",
        "recipient_phone", "campaign_id", "reason", iso(F.col("ts")).alias("unsubscribe_ts"), F.col("ts").alias("_event_ts")),
        "esp_unsubscribes_raw", "braze", "braze/currents/unsubscribes")

    # ---------------- paid media daily (Spark-native)
    camp = truth("paid_campaigns")
    ads = truth("paid_ads")
    u = lambda k: (F.pmod(F.xxhash64("ad_id", "day", F.lit(k)), F.lit(1_000_000)) / 1e6)
    noise = F.exp(F.lit(0.45) * ((u(1) + u(2) + u(3) - 1.5) * 2))
    plat_imp = F.create_map(*[x for k, v in {"meta": 9000, "google": 4500, "tiktok": 7000, "youtube": 5000}.items() for x in (F.lit(k), F.lit(v))])
    ctr = F.create_map(*[x for k, v in {"meta": .011, "google": .045, "tiktok": .008, "youtube": .004}.items() for x in (F.lit(k), F.lit(v))])
    cpm = F.create_map(*[x for k, v in {"meta": 9.5, "google": 32.0, "tiktok": 6.5, "youtube": 8.0}.items() for x in (F.lit(k), F.lit(v))])
    csz = F.create_map(*[x for k, v in {"US": 1.0, "UK": .26, "DE": .21, "CA": .15}.items() for x in (F.lit(k), F.lit(v))])
    pdly = (ads.join(camp, "campaign_id")
            .withColumn("day", F.explode(F.sequence(F.col("start_day"), F.col("end_day"))))
            .withColumn("report_date", F.date_add(F.lit(str(START)).cast("date"), F.col("day").cast("int")))
            .withColumn("season", F.when(F.month("report_date").isin(11, 12), 1.35).when(F.month("report_date") == 8, 1.2)
                        .when(F.month("report_date") == 1, 1.15).otherwise(1.0))
            .withColumn("impressions", (plat_imp[F.col("platform")] * csz[F.col("country")] * F.col("budget_scale") * F.col("season")
                                        * F.when(F.col("objective") == "retargeting", 0.35).otherwise(1.0) * noise).cast("long"))
            .withColumn("clicks", (F.col("impressions") * ctr[F.col("platform")] * F.when(F.col("objective") == "retargeting", 1.8).otherwise(1.0)
                                   * (0.7 + 0.6 * u(4))).cast("long"))
            .withColumn("spend", F.round(F.col("impressions") / 1000 * cpm[F.col("platform")] * (0.85 + 0.3 * u(5)), 2))
            .withColumn("video_views", F.when(F.col("platform").isin("youtube", "tiktok", "meta"),
                                              (F.col("impressions") * (0.12 + 0.25 * u(6))).cast("long")))
            .withColumn("platform_conversions", (F.col("clicks") * F.when(F.col("objective") == "retargeting", 0.045)
                                                 .when(F.col("burst"), 0.022).otherwise(0.014) * (0.6 + 0.8 * u(7))).cast("long"))
            .withColumn("platform_conversion_value", F.round(F.col("platform_conversions") * (115 + 40 * u(8)), 2))
            .withColumn("currency", F.when(F.col("country") == "UK", "GBP").when(F.col("country") == "DE", "EUR")
                        .when(F.col("country") == "CA", "CAD").otherwise("USD"))
            .withColumn("_event_ts", (F.col("report_date").cast("timestamp") + F.expr("INTERVAL 30 HOURS")))
            .select("platform", "account_id", "campaign_id", "campaign_name", "ad_group_id", "ad_id", "audience_name",
                    F.date_format("report_date", "yyyy-MM-dd").alias("report_date"), "impressions", "clicks", "spend",
                    "video_views", "platform_conversions", "platform_conversion_value", "currency", "_event_ts"))
    write_bronze(pdly, "paid_media_daily_raw", "supermetrics", "ads/daily")

    # ---------------- platform-reported conversions (click-through + view-through over-attribution)
    persons = truth("persons").select("person_idx", "email", "paid_optin")
    orders = (truth("order_lines").where("line_number = 1 AND channel IN ('web','app') AND person_idx IS NOT NULL")
              .select("order_id", "person_idx", "order_ts", "order_day", "country", "total_amount", "utm_source",
                      "utm_medium", "utm_campaign"))
    ad_list = ads.groupBy("campaign_id").agg(F.collect_list("ad_id").alias("ads"))
    ct = (orders.where("utm_medium IN ('cpc','paid_social','paid_video') AND utm_campaign IS NOT NULL")
          .withColumn("platform", F.col("utm_source")).withColumn("campaign_id", F.col("utm_campaign"))
          .withColumn("kind", F.lit("click")))
    vt_c = camp.where("platform = 'meta' AND objective = 'prospecting'").select(
        F.col("campaign_id").alias("vt_campaign"), F.col("country").alias("vt_country"), "start_day", "end_day")
    vt = (orders.where("(utm_medium IS NULL OR utm_medium IN ('organic','email','referral')) AND pmod(xxhash64(order_id), 100) < 9")
          .join(vt_c, (F.col("country") == F.col("vt_country")) & F.col("order_day").between(F.col("start_day"), F.col("end_day")))
          .withColumn("platform", F.lit("meta")).withColumn("campaign_id", F.col("vt_campaign")).withColumn("kind", F.lit("view"))
          .drop("vt_campaign", "vt_country", "start_day", "end_day"))
    conv = (ct.unionByName(vt).join(persons, "person_idx").where("paid_optin").join(ad_list, "campaign_id", "left")
            .select(F.concat(F.lit("CV-"), F.substring(F.sha2(F.concat("order_id", "platform"), 256), 1, 18)).alias("conversion_id"),
                    "platform", "campaign_id",
                    F.element_at("ads", (F.pmod(F.xxhash64("order_id"), F.size("ads")) + 1).cast("int")).alias("ad_id"),
                    F.when(F.col("kind") == "view", F.lit(None)).otherwise(F.concat(
                        F.when(F.col("platform") == "google", "gclid_").when(F.col("platform") == "tiktok", "ttclid_")
                        .when(F.col("platform") == "youtube", "gclid_").otherwise("fbclid_"),
                        F.substring(F.sha2("order_id", 256), 1, 22))).alias("click_id"),
                    F.sha2(F.lower(F.trim("email")), 256).alias("hashed_email"), "order_id",
                    F.date_format("order_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("conversion_ts"),
                    F.col("total_amount").alias("conversion_value"), F.col("order_ts").alias("_event_ts")))
    write_bronze(conv, "paid_media_conversions_raw", "meta_capi_google_ec", "ads/conversions")

    # ---------------- affiliate redemptions
    aff = truth("affiliates").select("promo_code", "partner_type")
    comm = F.create_map(*[x for k, v in {"coupon_site": .03, "creator": .08, "athlete": .10, "run_club": .06, "blog": .05}.items()
                          for x in (F.lit(k), F.lit(v))])
    red = (truth("order_lines").where("line_number = 1 AND promo_code IS NOT NULL").join(aff, "promo_code")
           .select(F.concat(F.lit("AR-"), F.substring(F.sha2("order_id", 256), 1, 16)).alias("redemption_id"), "promo_code",
                   "order_id", F.date_format("order_ts", "yyyy-MM-dd HH:mm:ss").alias("redemption_ts"),
                   F.round(F.col("subtotal") - F.col("discount_amount"), 2).alias("order_amount"),
                   F.round((F.col("subtotal") - F.col("discount_amount")) * comm[F.col("partner_type")], 2).alias("commission_amount"),
                   (F.col("order_ts") + F.expr("INTERVAL 1 DAY")).alias("_event_ts")))
    write_bronze(red, "affiliate_redemptions_raw", "impact_affiliate", "affiliate/redemptions")
