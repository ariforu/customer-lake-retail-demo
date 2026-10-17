"""Loyalty points ledger (stateful per member) and member master derived from it."""
from datetime import date, timedelta

import numpy as np
import pandas as pd

from gen import messy as M
from gen.config import (TRUTH, BF_DAYS, END, N_DAYS, SEED, START, di, get_spark, loyalty_card, loyalty_id, md5hex,
                        save_truth, to_ts, truth, write_bronze)

LEDGER_SCHEMA = """ledger_id string, loyalty_id string, activity_type string, points int, reference_id string,
reference_type string, activity_ts timestamp, expiry_date string, person_idx long, tier_after string, acct int"""
COLS = [c.split()[0] for c in LEDGER_SCHEMA.replace("\n", " ").split(", ")]
BF_IDX = [di(b) for b in BF_DAYS]
DBL_WEEK = (di(date(2026, 5, 12)), di(date(2026, 5, 19)))
MULT = {"Rookie": 1.0, "Pro": 1.25, "Elite": 1.5}


def _tier(pts12):
    return "Elite" if pts12 >= 1000 else "Pro" if pts12 >= 300 else "Rookie"


def ledger_fn(pdf):
    pdf = pdf.sort_values("order_ts", na_position="first")
    m = pdf.iloc[0]
    pidx = int(m.person_idx)
    rng = np.random.default_rng(SEED + 17 + pidx)
    lid = loyalty_id(pidx)
    rows = []
    ev = []   # (day, sec, kind, payload)
    enroll = int(m.loyalty_day)
    if enroll < 0:
        ev.append((0, 3600, "migration", int(rng.integers(0, 900))))
    else:
        ev.append((enroll, int(rng.integers(36000, 70000)), "enroll", 250))
    bd = m.birth_date
    for y in (2023, 2024, 2025, 2026):
        try:
            d = date(y, bd.month, bd.day)
        except ValueError:
            d = date(y, 3, 1)
        dd = di(d)
        if max(enroll, 0) <= dd < min(N_DAYS, int(m.end_day) + 180):
            ev.append((dd, 32400, "birthday", 200))
    for o in pdf.itertuples():
        if pd.isna(o.order_id):
            continue
        t = pd.Timestamp(o.order_ts)
        day = (t.date() - START).days
        ev.append((day, t.hour * 3600 + t.minute * 60 + t.second, "order", o))
        if o.refund and o.refund > 0 and not pd.isna(o.return_ts):
            rt = pd.Timestamp(o.return_ts)
            ev.append(((rt.date() - START).days, rt.hour * 3600 + rt.minute * 60, "return", o))
    ev.sort(key=lambda e: (e[0], e[1]))
    bal, tier, hist, last_act = 0, "Rookie", [], None

    def add(day, sec, typ, pts, ref, rtype):
        nonlocal bal
        bal += pts
        ts = to_ts(day, sec)
        rows.append(("LG" + md5hex(f"{lid}-{len(rows)}-{day}-{typ}")[:14].upper(), lid, typ, int(pts), ref, rtype, ts,
                     (START + timedelta(days=day + 365)).isoformat() if pts > 0 else None, pidx, tier, 0))

    for day, sec, kind, x in ev:
        if day >= N_DAYS:
            continue
        if last_act is not None and day - last_act > 365 and bal > 0:
            add(last_act + 365, 7200, "expire", -bal, None, "inactivity")
        if kind in ("migration", "enroll", "birthday"):
            add(day, sec, "bonus" if kind != "migration" else "opening_balance", x,
                {"migration": "LEGACY", "enroll": "ENROLL", "birthday": "BIRTHDAY"}[kind], "promotion")
        elif kind == "order":
            o = x
            mult = MULT[tier] * (2 if (any(-6 <= day - b <= 3 for b in BF_IDX) or DBL_WEEK[0] <= day <= DBL_WEEK[1]) else 1)
            pts = int(np.floor(o.net * mult))
            if bal >= 1000 and rng.random() < 0.35:
                add(day, sec, "redeem", -1000, o.order_id, "order")
            add(day, sec, "earn", pts, o.order_id, "order")
            hist.append((day, pts))
        else:
            o = x
            add(day, sec, "adjust", -int(np.floor(o.refund * MULT[tier])), o.order_id, "return")
            hist.append((day, -int(np.floor(o.refund))))
        last_act = day
        pts12 = sum(p for d, p in hist if day - d < 365)
        nt = _tier(pts12)
        if nt != tier:
            tier = nt
            add(day, sec + 1, "tier_change", 0, nt, "tier")
    if last_act is not None and N_DAYS - last_act > 365 and bal > 0:
        add(last_act + 365, 7200, "expire", -bal, None, "inactivity")
    if m.loyalty_dup:
        dl = loyalty_id(pidx, 1)
        d2 = min(N_DAYS - 1, max(enroll, 0) + int(rng.integers(30, 700)))
        rows.append(("LG" + md5hex(f"{dl}-0")[:14].upper(), dl, "bonus", 250, "ENROLL", "promotion", to_ts(d2, 50000),
                     (START + timedelta(days=d2 + 365)).isoformat(), pidx, "Rookie", 1))
    df = pd.DataFrame(rows, columns=COLS)
    df["activity_ts"] = pd.to_datetime(df["activity_ts"])
    return df


MEMBER_SCHEMA = """loyalty_id string, loyalty_card_number string, email string, phone string, first_name string,
last_name string, birth_date string, postal_code string, tier string, points_balance int, enrolled_ts string,
enrollment_channel string, status string, person_idx long, _event_ts timestamp"""


def gen_members(batches):
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 19 + int(b["person_idx"].iloc[0]))
        out = []
        for p in b.itertuples():
            for k in ([0, 1] if p.loyalty_dup else [0]):
                lid = loyalty_id(p.person_idx, k)
                enr = max(int(p.loyalty_day), -1500) + (int(rng.integers(30, 700)) if k else 0)
                enr = min(enr, N_DAYS - 1)
                first, last = M.name_variant(rng, p.first_name, p.last_name, p_nick=0.05 + 0.3 * k, p_typo=0.03)
                bal = int(p.bal if k == 0 and not pd.isna(p.bal) else 250)
                last_act = p.last_act if k == 0 and not pd.isna(p.last_act) else None
                status = ("closed" if rng.random() < 0.01 else
                          "inactive" if last_act is not None and (pd.Timestamp(END) - pd.Timestamp(last_act)).days > 365 else "active")
                out.append(dict(
                    loyalty_id=lid, loyalty_card_number=loyalty_card(p.person_idx, k),
                    email=M.email_variant(rng, p.email if k == 0 or not p.email_alt else p.email_alt, p_case=0.08, p_typo=0.02),
                    phone=M.phone_format(rng, p.phone, "digits") if rng.random() < 0.75 else None,
                    first_name=first, last_name=last, birth_date=p.birth_date.isoformat() if rng.random() < 0.8 else None,
                    postal_code=p.new_postal_code if p.moved_day < enr else p.postal_code,
                    tier=(p.tier if k == 0 and isinstance(p.tier, str) else "Rookie"), points_balance=max(bal, 0),
                    enrolled_ts=M.fmt_ts(M.ts_of(enr, rng.integers(36000, 75000)), "space"),
                    enrollment_channel=str(rng.choice(["store", "web", "app"], p=[.5, .35, .15])
                                           if p.channel_pref != "online" else rng.choice(["web", "app"], p=[.7, .3])),
                    status=status, person_idx=p.person_idx,
                    _event_ts=pd.Timestamp(last_act) if last_act is not None else M.ts_of(max(enr, 0), 80000)))
        df = pd.DataFrame(out, columns=[c.split()[0] for c in MEMBER_SCHEMA.replace("\n", " ").split(", ")])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


def run():
    from pyspark.sql import Window, functions as F
    spark = get_spark()
    mem = truth("persons").where("is_loyalty").select("person_idx", "loyalty_day", "birth_date", "end_day", "loyalty_dup")
    orders = (truth("order_lines").where("loyalty_earn AND person_idx IS NOT NULL")
              .groupBy("person_idx", "order_id").agg(
                  F.first("order_ts").alias("order_ts"),
                  F.first(F.col("subtotal") - F.col("discount_amount")).alias("net"),
                  F.sum(F.when(F.col("returned") & (F.col("return_type") == "refund"), F.col("refund_amount"))).alias("refund"),
                  F.max(F.when(F.col("returned") & (F.col("return_type") == "refund"), F.col("return_ts"))).alias("return_ts")))
    j = mem.join(orders, "person_idx", "left").repartition(96, "person_idx")
    led = save_truth(j.groupBy("person_idx").applyInPandas(ledger_fn, LEDGER_SCHEMA), "loyalty_ledger")
    write_bronze(led.select("ledger_id", "loyalty_id", "activity_type", "points", "reference_id", "reference_type",
                            F.date_format("activity_ts", "yyyy-MM-dd HH:mm:ss").alias("activity_ts"), "expiry_date",
                            F.col("activity_ts").alias("_event_ts")),
                 "loyalty_points_ledger_raw", "loyalty_platform_capillary", "loyalty/ledger")
    summ = (led.where("acct = 0").groupBy("person_idx")
            .agg(F.sum("points").alias("bal"), F.max("activity_ts").alias("last_act"),
                 F.max_by("tier_after", "activity_ts").alias("tier")))
    P = (truth("persons").where("is_loyalty").join(summ, "person_idx", "left")
         .repartition(64, "person_idx").sortWithinPartitions("person_idx"))
    memdf = save_truth(P.mapInPandas(gen_members, MEMBER_SCHEMA), "src_loyalty_members")
    write_bronze(memdf.drop("person_idx"), "loyalty_members_raw", "loyalty_platform_capillary", "loyalty/members")
    idk = memdf.select(F.lit("loyalty").alias("source_system"), F.col("loyalty_id").alias("source_id"), "person_idx") \
        .unionByName(memdf.select(F.lit("loyalty_card").alias("source_system"), F.col("loyalty_card_number").alias("source_id"), "person_idx"))
    idk.write.mode("append").saveAsTable(f"{TRUTH}.identity_answer_key_profiles")
