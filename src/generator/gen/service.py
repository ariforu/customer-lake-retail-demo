"""Customer service tickets, CSAT, NPS and product reviews (free text reflects the CS3 sizing story)."""
from datetime import timedelta

import numpy as np
import pandas as pd

from gen import messy as M
from gen.config import CS3_FIX, CS3_LAUNCH, N_DAYS, SEED, START, di, get_spark, md5hex, to_ts, truth, write_bronze

CS3_L, CS3_F = di(CS3_LAUNCH), di(CS3_FIX)

TICKET_TPL = {
    "order_status": ("Where is my order {o}?", ["My order {o} still hasn't arrived and tracking hasn't updated in days.",
                                                "Tracking for {o} says delivered but I never received it.",
                                                "Can you tell me when order {o} will ship?"]),
    "returns_refunds": ("Refund for order {o}", ["I returned items from {o} two weeks ago and still no refund.",
                                                 "How do I start a return for {o}?", "My exchange for {o} hasn't shipped yet."]),
    "sizing_fit": ("Sizing question - {p}", ["The {p} feel much tighter than my previous pair in the same size. Should I size up?",
                                             "Ordered my usual size in {p} and my toes hit the front. Can I exchange for a half size up?",
                                             "Is the {p} true to size? Reviews say it runs small."]),
    "product_defect": ("Defective product - {p}", ["The sole on my {p} is separating after a few weeks.",
                                                   "Stitching came apart on my {p}.", "My {p} arrived with a glue stain."]),
    "promo_pricing": ("Promo code not working", ["The code {c} wasn't applied at checkout on order {o}.",
                                                 "Price dropped right after I bought order {o}, can I get an adjustment?"]),
    "loyalty_points": ("Missing loyalty points", ["I didn't receive points for order {o}.", "My points balance looks wrong after my last purchase."]),
    "account_login": ("Can't log in", ["I can't reset my password.", "I have two accounts with different emails, can you merge them?",
                                       "My account was locked after too many attempts."]),
}
CH = (["chat", "email", "phone", "social"], [.40, .35, .20, .05])
RES_H = {"chat": 2.0, "email": 20.0, "phone": 0.6, "social": 8.0}

T_SCHEMA = """ticket_id string, requester_email string, requester_phone string, channel string, subject string,
description string, category string, priority string, status string, order_reference string, created_ts string,
resolved_ts string, agent_id string, person_idx long, _event_ts timestamp, res_hours double, cs3 boolean"""
T_COLS = [c.split()[0] for c in T_SCHEMA.replace("\n", " ").split(", ")]


def gen_tickets(batches):
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 59 + int(b["person_idx"].iloc[0]))
        rows = []
        for o in b.itertuples():
            cats = []
            online = o.channel != "store"
            if online and o.fulfillment_type == "ship" and rng.random() < 0.06:
                cats.append(("order_status", o.order_day + int(rng.integers(3, 10))))
            if o.n_returned > 0 and rng.random() < 0.14:
                cats.append(("returns_refunds", o.last_return_day + int(rng.integers(5, 20))))
            if o.cs3 and o.order_day < CS3_F and rng.random() < 0.10:
                cats.append(("sizing_fit", o.order_day + int(rng.integers(0, 6))))
            elif o.has_footwear and online and rng.random() < 0.01:
                cats.append(("sizing_fit", o.order_day - int(rng.integers(0, 3))))
            if o.has_defect and rng.random() < 0.45:
                cats.append(("product_defect", o.last_return_day - int(rng.integers(0, 3))))
            if o.promo_code is not None and rng.random() < 0.02:
                cats.append(("promo_pricing", o.order_day + int(rng.integers(0, 4))))
            if o.loyalty_earn and rng.random() < 0.008:
                cats.append(("loyalty_points", o.order_day + int(rng.integers(3, 30))))
            if online and rng.random() < 0.006:
                cats.append(("account_login", o.order_day - int(rng.integers(0, 60))))
            for cat, d in cats:
                if d < 0 or d >= N_DAYS:
                    continue
                ch = str(rng.choice(CH[0], p=CH[1]))
                backlog = 2.6 if CS3_L <= d < CS3_F + 20 else 1.0
                if (START + timedelta(days=int(d))).month in (11, 12):
                    backlog *= 1.6
                res = float(rng.lognormal(np.log(RES_H[ch] * backlog), 0.8))
                c_ts = to_ts(d, int(rng.integers(25000, 80000)))
                c_pd = pd.Timestamp(c_ts)
                r_ts = c_pd + pd.Timedelta(hours=res)
                open_ = r_ts > pd.Timestamp(START + timedelta(days=N_DAYS - 1)) + pd.Timedelta(hours=20)
                subj, bodies = TICKET_TPL[cat]
                fmt = dict(o=o.order_id, p=o.top_style or "my shoes", c=o.promo_code or "")
                rows.append(("TK" + md5hex(f"{o.order_id}-{cat}")[:12].upper(),
                             M.email_variant(rng, o.email, p_case=0.05) if ch != "phone" or rng.random() < 0.5 else None,
                             o.phone if ch == "phone" else None, ch, subj.format(**fmt),
                             bodies[rng.integers(len(bodies))].format(**fmt), cat,
                             "high" if cat in ("product_defect", "order_status") and rng.random() < 0.4 else
                             str(rng.choice(["normal", "low"], p=[.8, .2])),
                             str(rng.choice(["open", "pending"])) if open_ else str(rng.choice(["solved", "closed"], p=[.3, .7])),
                             o.order_id if cat != "account_login" else None, c_pd.strftime("%Y-%m-%dT%H:%M:%SZ"),
                             None if open_ else r_ts.strftime("%Y-%m-%dT%H:%M:%SZ"), f"AG{int(rng.integers(1, 420)):04d}",
                             o.person_idx, c_pd, None if open_ else res, bool(o.cs3)))
        df = pd.DataFrame(rows, columns=T_COLS)
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


CSAT_COMMENTS = {5: ["Quick and helpful, thanks!", "Agent sorted it in minutes.", "Great service."],
                 4: ["Resolved, took a bit long.", "Helpful agent."],
                 3: ["Ok but had to explain twice.", "Took several days."],
                 2: ["Slow response.", "Still waiting on the refund."],
                 1: ["Terrible, nobody helps.", "Waited a week for a reply.", "Never buying again."]}
NPS_COMMENTS = {"promoter": ["Love the shoes and fast delivery.", "Best running shoes I've owned.", "Great store staff and fit advice.",
                             "Rewards program is great."],
                "passive": ["Good products, a bit pricey.", "Delivery took longer than expected.", "Decent experience overall."],
                "detractor": ["Sizing was way off and the return was a hassle.", "CloudStride 3 runs small, had to return.",
                              "Too many emails.", "Shoes wore out too fast.", "Customer service never answered."]}


def gen_surveys(batches):
    """Input: order-level rows (sampled). Emits NPS rows."""
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 61 + int(b["person_idx"].iloc[0]))
        out = []
        for o in b.itertuples():
            if rng.random() > 0.07:
                continue
            d = o.order_day + int(rng.integers(7, 21))
            if d >= N_DAYS:
                continue
            mu = 8.4 - (3.0 if o.cs3_fit_refund else 0) - (1.2 if o.n_returned else 0)
            score = int(np.clip(round(rng.normal(mu, 1.8)), 0, 10))
            cat = "promoter" if score >= 9 else "passive" if score >= 7 else "detractor"
            com = NPS_COMMENTS[cat]
            comment = ("CloudStride 3 runs small, had to return." if (o.cs3_fit_refund and rng.random() < 0.6)
                       else com[rng.integers(len(com))]) if rng.random() < 0.45 else None
            t = to_ts(d, int(rng.integers(30000, 80000)))
            out.append(dict(survey_response_id="NPS" + md5hex(o.order_id)[:12].upper(),
                            respondent_email=M.email_variant(rng, o.email, p_case=0.05),
                            touchpoint="post_purchase_" + ("store" if o.channel == "store" else "online"),
                            nps_score=score, comment=comment,
                            response_ts=pd.Timestamp(t).strftime("%Y-%m-%d %H:%M:%S"), _event_ts=t))
        df = pd.DataFrame(out, columns=["survey_response_id", "respondent_email", "touchpoint", "nps_score", "comment",
                                        "response_ts", "_event_ts"])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


# ---------------------------------------------------------------- reviews
OPEN = {5: ["Absolutely love these.", "Best {c} I've owned.", "Exceeded my expectations.", "Instant favourite."],
        4: ["Really solid {c}.", "Very happy with these overall.", "Great pair with a couple of small gripes."],
        3: ["Decent, but not perfect.", "Mixed feelings on these.", "They're okay for the price."],
        2: ["Disappointed with these.", "Not what I hoped for.", "Expected more from StrideForge."],
        1: ["Would not buy again.", "Really disappointed.", "Returned them after one wear."]}
COMFORT = {True: ["Cushioning feels plush on long runs.", "Super comfortable right out of the box.", "Light and bouncy.",
                  "Great support for my arches.", "Fabric is soft and breathable."],
           False: ["Felt stiff and uncomfortable.", "Gave me blisters on the first run.", "Too firm for my liking.",
                   "Material feels cheap."]}
FIT = {"true_to_size": ["Fits true to size.", "Ordered my usual size and it's perfect."],
       "runs_small": ["Runs about half a size small - size up!", "Toe box is really tight, had to exchange for a half size up.",
                      "Much narrower than the previous version.", "Same size as my CloudStride 2 but these are tighter."],
       "runs_large": ["Runs a little big.", "Heel slips a bit, consider sizing down."]}
USE = {"Running": ["Used them for marathon training.", "Great for tempo runs.", "My new daily trainer."],
       "Trail Running": ["Grippy on muddy trails.", "Kept my feet dry in the rain."],
       "Basketball": ["Great court feel.", "Ankle support is solid on cuts."],
       "Training": ["Stable for lifting.", "Good for HIIT classes."],
       "Lifestyle": ["Look great with jeans.", "Get compliments every time."],
       "Football": ["Great touch on the ball.", "Comfortable for 90 minutes."]}
DUR = {True: ["Holding up well after 200 miles.", "Durable so far."], False: ["Started falling apart after a month.",
                                                                          "Outsole wore down fast."]}
TITLE = {5: ["Love them!", "Perfect", "Best shoes ever", "10/10"], 4: ["Great", "Really good", "Solid choice"],
         3: ["Okay", "Average", "Not bad"], 2: ["Meh", "Disappointing"], 1: ["Avoid", "Terrible", "Returned"]}
R_SCHEMA = """review_id string, sku string, style_id string, reviewer_email string, reviewer_nickname string, rating int,
title string, review_text string, fit_rating string, verified_purchase boolean, submitted_ts string, _event_ts timestamp"""


def gen_reviews(batches):
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 67 + int(b["person_idx"].iloc[0]))
        out = []
        for l in b.itertuples():
            p_rev = (0.12 if l.channel != "store" else 0.05) + (0.06 if l.returned else 0)
            if l.division == "Accessories":
                p_rev *= 0.4
            if rng.random() > p_rev:
                continue
            d = l.order_day + int(rng.integers(7, 40))
            if d >= N_DAYS:
                continue
            cs3_bad = l.is_cs3 and l.order_day < CS3_F
            if cs3_bad:
                rating = int(rng.choice([1, 2, 3, 4, 5], p=[.14, .18, .24, .24, .20]))
                fit = "runs_small" if rng.random() < 0.7 else "true_to_size"
            else:
                rating = int(rng.choice([1, 2, 3, 4, 5], p=[.04, .05, .10, .28, .53]))
                if l.returned:
                    rating = max(1, rating - int(rng.integers(1, 3)))
                fit = ("runs_small" if l.return_reason == "too_small" else "runs_large" if l.return_reason == "too_big"
                       else str(rng.choice(["true_to_size", "runs_small", "runs_large"], p=[.8, .1, .1])))
            good = rating >= 4
            cat = "shoes" if l.division == "Footwear" else "piece" if l.division == "Apparel" else "accessory"
            parts = [OPEN[rating][rng.integers(len(OPEN[rating]))].format(c=cat)]
            parts.append(COMFORT[good if rating != 3 else rng.random() < 0.5][rng.integers(4)])
            if l.division == "Footwear" or rng.random() < 0.3:
                parts.append(FIT[fit][rng.integers(len(FIT[fit]))])
            u = USE.get(l.sport)
            if u and rng.random() < 0.6:
                parts.append(u[rng.integers(len(u))])
            if rng.random() < 0.35:
                parts.append(DUR[good][rng.integers(2)])
            t = to_ts(d, int(rng.integers(20000, 86000)))
            out.append(dict(review_id="RV" + md5hex(f"{l.order_id}-{l.line_number}")[:12].upper(), sku=l.sku, style_id=l.style_id,
                            reviewer_email=M.email_variant(rng, l.email, p_case=0.05), reviewer_nickname=
                            f"{l.first_name}{(' ' + l.last_name[0] + '.') if rng.random() < 0.6 else ''}" if rng.random() < 0.7
                            else f"{l.first_name[0]}{l.city.replace(' ', '')}Runner",
                            rating=rating, title=TITLE[rating][rng.integers(len(TITLE[rating]))], review_text=" ".join(parts),
                            fit_rating=fit if l.division == "Footwear" else None, verified_purchase=True,
                            submitted_ts=pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%SZ"), _event_ts=t))
        df = pd.DataFrame(out, columns=[c.split()[0] for c in R_SCHEMA.replace("\n", " ").split(", ")])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


def run():
    from pyspark.sql import functions as F
    spark = get_spark()
    L = truth("order_lines").where("person_idx IS NOT NULL")
    prods = truth("products").select("style_id", "style_name", "sport", F.col("style_name").startswith("CloudStride 3").alias("is_cs3"))
    Lp = L.join(prods, "style_id")
    persons = truth("persons").select("person_idx", "email", "phone", "first_name", "last_name", "city")
    o = (Lp.groupBy("person_idx", "order_id").agg(
        F.first("channel").alias("channel"), F.first("order_day").alias("order_day"),
        F.first("fulfillment_type").alias("fulfillment_type"), F.first("promo_code").alias("promo_code"),
        F.first("loyalty_earn").alias("loyalty_earn"), F.sum(F.col("returned").cast("int")).alias("n_returned"),
        F.max("return_day").alias("last_return_day"), F.max("is_cs3").alias("cs3"),
        F.max(F.col("division") == "Footwear").alias("has_footwear"),
        F.max(F.col("return_reason") == "defect").alias("has_defect"),
        F.max(F.col("is_cs3") & (F.col("return_reason") == "too_small") & (F.col("return_type") == "refund")).alias("cs3_fit_refund"),
        F.max_by("style_name", "unit_price").alias("top_style"))
        .join(persons, "person_idx").repartition(96, "person_idx").sortWithinPartitions("person_idx"))
    T = o.mapInPandas(gen_tickets, T_SCHEMA)
    from gen.config import save_truth
    T = save_truth(T, "service_tickets")
    write_bronze(T.drop("person_idx", "res_hours", "cs3"), "service_tickets_raw", "zendesk", "zendesk/tickets")
    cs = (T.where("res_hours IS NOT NULL AND pmod(xxhash64(ticket_id), 100) < 35")
          .withColumn("u", F.pmod(F.xxhash64("ticket_id", F.lit(1)), F.lit(1000)) / 1000.0)
          .withColumn("score", F.greatest(F.lit(1), F.least(F.lit(5), F.round(
              F.lit(5.2) - F.log(F.col("res_hours") + 1) * 0.45 - F.when(F.col("cs3"), 0.6).otherwise(0)
              + (F.col("u") - 0.5) * 2.2))).cast("int")))
    cmap = F.create_map(*[x for k, v in CSAT_COMMENTS.items() for x in (F.lit(k), F.array(*[F.lit(c) for c in v]))])
    cs = cs.select(F.concat(F.lit("CS"), F.substring("ticket_id", 3, 12)).alias("survey_response_id"), "ticket_id",
                   F.col("requester_email").alias("respondent_email"), F.col("score").alias("csat_score"),
                   F.when(F.col("u") < 0.4, F.element_at(cmap[F.col("score")], (F.pmod(F.xxhash64("ticket_id"), F.lit(2)) + 1).cast("int"))).alias("comment"),
                   F.date_format(F.col("_event_ts") + F.expr("INTERVAL 2 DAYS"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("response_ts"),
                   (F.col("_event_ts") + F.expr("INTERVAL 2 DAYS")).alias("_event_ts"))
    write_bronze(cs.where(F.col("_event_ts") < F.lit("2026-10-16").cast("timestamp")), "csat_surveys_raw", "zendesk", "zendesk/csat")
    write_bronze(o.mapInPandas(gen_surveys, "survey_response_id string, respondent_email string, touchpoint string, "
                                            "nps_score int, comment string, response_ts string, _event_ts timestamp"),
                 "nps_surveys_raw", "qualtrics", "qualtrics/nps")
    rl = (Lp.where("order_kind <> 'drop_resell'").join(persons, "person_idx")
          .select("person_idx", "order_id", "line_number", "channel", "order_day", "sku", "style_id", "division", "sport",
                  "is_cs3", "returned", "return_reason", "email", "first_name", "last_name", "city")
          .repartition(128, "person_idx"))
    write_bronze(rl.mapInPandas(gen_reviews, R_SCHEMA), "product_reviews_raw", "bazaarvoice", "bazaarvoice/reviews")
