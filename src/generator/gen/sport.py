"""Run-app shoe registrations + activities, race registrations, store run clubs, in-store gait analysis."""
from datetime import timedelta

import numpy as np
import pandas as pd

from gen import messy as M
from gen.config import (TRUTH, CS3_FIX, N_DAYS, SEED, START, di, fitness_id, get_spark, loyalty_id, md5hex, save_truth,
                        to_ts, truth, write_bronze)

FAR = 99999
NICK = ["Daily trainer", "Long run shoes", "Race day", "Tempo shoes", "Old faithful", "Trail beasts", "Rainy day pair",
        "Recovery shoes", "Speed work", None, None, None]

# ---------------------------------------------------------------- shoe registrations
REG_SCHEMA = """shoe_registration_id string, fitness_user_id string, sku string, style_name string, nickname string,
registered_ts string, retired_ts string, order_reference string, person_idx long, reg_day int, ret_day int,
_event_ts timestamp"""


def gen_regs(batches, names):
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 37 + int(b["person_idx"].iloc[0]))
        out = []
        for p in b.itertuples():
            buys = sorted([x for x in (p.buys if isinstance(p.buys, (list, np.ndarray)) else [])], key=lambda x: x["order_day"])
            start = max(int(p.fitness_day), 0)
            regs = []
            for x in buys:
                if rng.random() > 0.85:
                    continue
                d = max(start, int(x["order_day"]) + int(rng.integers(0, 11)))
                if d >= N_DAYS:
                    continue
                regs.append(dict(day=d, sku=x["sku"], name=names.get(x["style_id"], x["style_id"]),
                                 order=x["order_id"] if rng.random() < 0.4 else None))
            last_buy = max([int(x["order_day"]) for x in buys], default=-FAR)
            if N_DAYS - last_buy > 420 and rng.random() < 0.5:     # moved to a competitor brand
                d = min(N_DAYS - 5, max(start, last_buy + int(rng.integers(380, 480))))
                if d > start:
                    regs.append(dict(day=d, sku=None, name=str(rng.choice(["Other brand - road running shoe",
                                                                           "Other brand - carbon racer",
                                                                           "Other brand - trail shoe"])), order=None))
            regs.sort(key=lambda r: r["day"])
            for i, r in enumerate(regs):
                ret = regs[i + 1]["day"] + int(rng.integers(0, 20)) if i + 1 < len(regs) and rng.random() < 0.7 else FAR
                ret = ret if ret < N_DAYS else FAR
                rid = "SH" + md5hex(f"{p.person_idx}-{i}-{r['day']}")[:12].upper()
                out.append(dict(shoe_registration_id=rid, fitness_user_id=fitness_id(p.person_idx), sku=r["sku"],
                                style_name=r["name"], nickname=NICK[rng.integers(len(NICK))],
                                registered_ts=M.fmt_ts(M.ts_of(r["day"], rng.integers(25000, 80000)), "iso_z"),
                                retired_ts=M.fmt_ts(M.ts_of(ret, 70000), "iso_z") if ret < FAR else None,
                                order_reference=r["order"], person_idx=p.person_idx, reg_day=r["day"], ret_day=ret,
                                _event_ts=M.ts_of(r["day"], 80000)))
        df = pd.DataFrame(out, columns=[c.split()[0] for c in REG_SCHEMA.replace("\n", " ").split(", ")])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


# ---------------------------------------------------------------- activities
ACT_SCHEMA = """activity_id string, fitness_user_id string, email string, activity_type string, start_ts string,
duration_seconds int, distance_km double, elevation_gain_m double, avg_pace_sec_per_km double, avg_heart_rate int,
shoe_registration_id string, start_lat double, start_lon double, _event_ts timestamp"""
A_COLS = [c.split()[0] for c in ACT_SCHEMA.replace("\n", " ").split(", ")]


def gen_acts(batches):
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 41 + int(b["person_idx"].iloc[0]))
        rows = []
        for p in b.itertuples():
            d0 = max(int(p.fitness_day), 0)
            if d0 >= N_DAYS:
                continue
            stop = N_DAYS
            if rng.random() < 0.15:
                stop = d0 + int(rng.exponential(300))
            elif p.end_day < N_DAYS and rng.random() < 0.4:
                stop = int(p.end_day) + int(rng.integers(0, 120))
            stop = min(stop, N_DAYS)
            regs = sorted([r for r in (p.regs if isinstance(p.regs, (list, np.ndarray)) else [])], key=lambda r: r["reg_day"])
            email = p.email_alt if (p.fitness_alt_email and p.email_alt) else p.email
            fid = fitness_id(p.person_idx)
            trail = p.primary_sport == "Trail Running"
            base_pace = float(np.clip(372 - (p.weekly_km - 18) * 1.6 + rng.normal(0, 25), 235, 480))
            per_run = p.weekly_km / max(1, p.runs_per_week)
            run_dow = sorted(rng.choice(7, int(min(7, p.runs_per_week)), replace=False).tolist())
            for wk in range(d0 // 7, stop // 7 + 1):
                for dow in run_dow:
                    day = wk * 7 + dow
                    if day < d0 or day >= stop or rng.random() > 0.72:
                        continue
                    dt = START + timedelta(days=day)
                    long_run = dt.weekday() == 6
                    km = float(np.clip(per_run * rng.lognormal(0, 0.3) * (1.6 if long_run else 1.0), 1.0, 45))
                    u = rng.random()
                    atype = "walk" if u < 0.07 else "treadmill_run" if u < 0.14 else ("trail_run" if trail and u < 0.8 else "run")
                    if atype == "walk":
                        km, pace = km * 0.6, base_pace * 1.9
                    else:
                        pace = base_pace * rng.normal(1.0, 0.05) * (1.12 if atype == "trail_run" else 1.0) * (1.05 if long_run else 1.0)
                        if dt.month in (6, 7, 8):
                            pace *= 1.03
                    dur = int(km * pace)
                    shoe = None
                    if atype != "walk" and rng.random() < 0.8:
                        for r in regs:
                            if r["reg_day"] <= day < r["ret_day"]:
                                shoe = r["shoe_registration_id"]
                    hour = int(rng.choice([6, 7, 8, 12, 17, 18, 19], p=[.22, .2, .1, .1, .14, .16, .08]))
                    if long_run:
                        hour = 8
                    ts = to_ts(day, hour * 3600 + int(rng.integers(0, 3600)))
                    rows.append(("ACT" + md5hex(f"{fid}-{day}-{dow}")[:16], fid, email if rng.random() < 0.97 else None,
                                 atype, None, dur, round(km, 2),
                                 round(float(rng.gamma(2, 60 if atype == "trail_run" else 12)), 1) if atype != "treadmill_run" else 0.0,
                                 round(pace, 1), int(np.clip(rng.normal(150 if atype != "walk" else 108, 9), 90, 190)),
                                 shoe, round(p.latitude + rng.normal(0, 0.03), 5) if atype != "treadmill_run" else None,
                                 round(p.longitude + rng.normal(0, 0.03), 5) if atype != "treadmill_run" else None, ts))
            if len(rows) > 250_000:
                yield _afr(rows)
                rows = []
        if rows:
            yield _afr(rows)


def _afr(rows):
    df = pd.DataFrame(rows, columns=A_COLS)
    df["_event_ts"] = pd.to_datetime(df["_event_ts"])
    df["start_ts"] = df["_event_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


# ---------------------------------------------------------------- race registrations + run club
EVT_SCHEMA = """registration_id string, event_id string, event_name string, event_type string, event_date string,
event_city string, distance_km double, email string, first_name string, last_name string, registered_ts string,
registration_source string, _event_ts timestamp"""
CLUB_SCHEMA = """checkin_id string, store_id string, club_session_id string, loyalty_id string, email string,
checkin_ts string, run_distance_km double, _event_ts timestamp"""


def gen_events(batches, races, club_stores):
    ev_ids, ev_day, ev_metro, ev_pop = races["ids"], races["day"], races["metro"], races["pop"]
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 43 + int(b["person_idx"].iloc[0]))
        out = []
        for p in b.itertuples():
            rate = 0.9 if p.is_fitness else 0.15 if p.primary_sport in ("Running", "Trail Running") else 0.02
            n = rng.poisson(rate * 3.4)
            if n == 0:
                continue
            local = np.where(ev_metro == p.metro_id)[0]
            for _ in range(n):
                if len(local) and rng.random() < 0.7:
                    i = int(local[rng.integers(len(local))])
                else:
                    i = int(rng.choice(len(ev_ids), p=ev_pop))
                reg = int(ev_day[i]) - int(rng.integers(20, 160))
                if reg >= N_DAYS or reg < max(p.acq_day, 0) - 400 or reg < 0:
                    continue
                email = p.email_alt if (p.email_alt and rng.random() < 0.3) else p.email
                first, last = M.name_variant(rng, p.first_name, p.last_name, p_nick=0.1, p_typo=0.02)
                t = M.ts_of(reg, rng.integers(0, 86400))
                out.append(dict(registration_id="RG" + md5hex(f"{p.person_idx}-{ev_ids[i]}")[:14].upper(),
                                event_id=ev_ids[i], event_name=races["name"][i], event_type=races["type"][i],
                                event_date=races["date"][i], event_city=races["city"][i], distance_km=float(races["dist"][i]),
                                email=M.email_variant(rng, email, p_case=0.12, p_typo=0.02), first_name=first,
                                last_name=last, registered_ts=M.fmt_ts(t, "space"),
                                registration_source=str(rng.choice(["RaceRoster", "Active.com", "RunSignup", "StrideForge App"],
                                                                   p=[.35, .25, .25, .15])), _event_ts=t))
        df = pd.DataFrame(out, columns=[c.split()[0] for c in EVT_SCHEMA.replace("\n", " ").split(", ")])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


def gen_club(batches, club_stores):
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 47 + int(b["person_idx"].iloc[0]))
        out = []
        for p in b.itertuples():
            if p.home_store_id not in club_stores:
                continue
            if rng.random() > (0.14 if p.is_fitness else 0.03):
                continue
            j0 = max(0, int(p.acq_day)) + int(rng.integers(0, 600))
            j1 = min(N_DAYS, j0 + int(rng.exponential(260)) + 14, int(p.end_day) + 30)
            att = rng.uniform(0.2, 0.7)
            wd = 2  # Wednesday sessions
            first_wed = j0 + (wd - (START + timedelta(days=j0)).weekday()) % 7
            for day in range(first_wed, j1, 7):
                if rng.random() > att:
                    continue
                t = to_ts(day, 18 * 3600 + 1800 - int(rng.integers(0, 900)))
                out.append(dict(checkin_id="CK" + md5hex(f"{p.person_idx}-{day}")[:14].upper(), store_id=p.home_store_id,
                                club_session_id=f"{p.home_store_id}-RC-{(START + timedelta(days=day)):%Y%m%d}",
                                loyalty_id=loyalty_id(p.person_idx) if (p.is_loyalty and rng.random() < 0.75) else None,
                                email=p.email if rng.random() < 0.9 else None,
                                run_distance_km=float(rng.choice([5.0, 6.5, 8.0, 10.0], p=[.4, .25, .25, .1])), _event_ts=t))
        df = pd.DataFrame(out, columns=[c.split()[0] for c in CLUB_SCHEMA.replace("\n", " ").split(", ")])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        df["checkin_ts"] = df["_event_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
        yield df


# ---------------------------------------------------------------- gait analysis
GAIT_SCHEMA = """analysis_id string, store_id string, loyalty_id string, email string, analysis_ts string,
foot_length_left_mm double, foot_length_right_mm double, foot_width_mm double, arch_type string, pronation_type string,
recommended_size string, recommended_width string, recommended_styles string, _event_ts timestamp"""


def gen_gait(batches):
    fix = di(CS3_FIX)
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 53 + int(b["person_idx"].iloc[0]))
        out = []
        for r in b.itertuples():
            if rng.random() > 0.45:
                continue
            f = r.gender == "F"
            foot = (21.6 + 0.85 * (r.shoe_size - 5)) if f else (22.0 + 0.85 * (r.shoe_size - 4))
            L = foot * 10 + rng.normal(0, 2.5)
            width = L * (0.37 if r.arch_type != "flat" else 0.39) + rng.normal(0, 2)
            rec = r.shoe_size + (0.5 if (r.order_day >= fix and r.is_cs3) else 0)
            styles = ("StabilityMax 3, CloudStride 3" if r.pronation_type == "overpronation" else
                      "CloudStride 3, UltraGlide 3" if r.pronation_type == "neutral" else "UltraGlide 3, TempoFlex 3")
            ts = pd.Timestamp(r.order_ts) - pd.Timedelta(minutes=int(rng.integers(20, 55)))
            out.append(dict(analysis_id="GA" + md5hex(f"{r.order_id}")[:14].upper(), store_id=r.store_id,
                            loyalty_id=loyalty_id(r.person_idx) if r.is_loyalty else None,
                            email=r.email if rng.random() < 0.7 else None,
                            analysis_ts=ts.strftime("%Y-%m-%d %H:%M:%S"), foot_length_left_mm=round(L + rng.normal(0, 1.8), 1),
                            foot_length_right_mm=round(L + rng.normal(0, 1.8), 1), foot_width_mm=round(width, 1),
                            arch_type=r.arch_type, pronation_type=r.pronation_type, recommended_size=f"{rec:g}",
                            recommended_width="2E" if width / L > 0.395 and not f else "D" if not f else "B",
                            recommended_styles=styles, _event_ts=ts))
        df = pd.DataFrame(out, columns=[c.split()[0] for c in GAIT_SCHEMA.replace("\n", " ").split(", ")])
        df["_event_ts"] = pd.to_datetime(df["_event_ts"])
        yield df


def run():
    from pyspark.sql import functions as F
    spark = get_spark()
    persons = truth("persons")
    prods = truth("products").select("style_id", "style_name").toPandas()
    names = dict(zip(prods.style_id, prods.style_name))
    L = truth("order_lines").where("person_idx IS NOT NULL AND cat_code IN ('RUN','TRL') AND NOT (returned AND return_type = 'refund')")
    buys = L.groupBy("person_idx").agg(F.collect_list(F.struct("order_id", "order_day", "sku", "style_id")).alias("buys"))
    fit = persons.where("is_fitness").join(buys, "person_idx", "left").repartition(64, "person_idx").sortWithinPartitions("person_idx")
    regs = save_truth(fit.mapInPandas(lambda it: gen_regs(it, names), REG_SCHEMA), "shoe_registrations")
    write_bronze(regs.drop("person_idx", "reg_day", "ret_day"), "fitness_shoe_registrations_raw", "strideforge_run_app", "runapp/shoes")
    rl = regs.groupBy("person_idx").agg(F.collect_list(F.struct("shoe_registration_id", "reg_day", "ret_day")).alias("regs"))
    fa = persons.where("is_fitness").join(rl, "person_idx", "left").repartition(160, "person_idx").sortWithinPartitions("person_idx")
    write_bronze(fa.mapInPandas(gen_acts, ACT_SCHEMA), "fitness_activities_raw", "strideforge_run_app", "runapp/activities")
    idk = persons.where("is_fitness").select(F.lit("fitness").alias("source_system"), "person_idx")
    fid_udf = F.udf(lambda i: fitness_id(i), "string")
    idk.select("source_system", fid_udf("person_idx").alias("source_id"), "person_idx") \
        .write.mode("append").saveAsTable(f"{TRUTH}.identity_answer_key_profiles")

    rp = truth("races").toPandas()
    pop = rp.popularity.to_numpy()
    races = dict(ids=rp.event_id.to_numpy(), day=rp.event_date.map(lambda s: (pd.Timestamp(s).date() - START).days).to_numpy(),
                 metro=rp.metro_id.to_numpy(), pop=pop / pop.sum(), name=rp.event_name.to_numpy(),
                 type=rp.event_type.to_numpy(), date=rp.event_date.astype(str).to_numpy(), city=rp.event_city.to_numpy(),
                 dist=rp.distance_km.to_numpy())
    club_stores = set(truth("stores").where("hosts_run_club").toPandas().store_id)
    runners = persons.where("is_fitness OR primary_sport IN ('Running','Trail Running') OR rand() < 0.02") \
        .repartition(64, "person_idx").sortWithinPartitions("person_idx")
    write_bronze(runners.mapInPandas(lambda it: gen_events(it, races, club_stores), EVT_SCHEMA),
                 "event_registrations_raw", "race_registration_partners", "partners/race_registrations")
    write_bronze(runners.mapInPandas(lambda it: gen_club(it, club_stores), CLUB_SCHEMA),
                 "run_club_checkins_raw", "store_events_app", "stores/run_club")
    gait_stores = truth("stores").where("has_gait_lab").select("store_id")
    cs3 = truth("products").select("style_id", F.col("style_name").startswith("CloudStride 3").alias("is_cs3"))
    g = (truth("order_lines").where("channel = 'store' AND person_idx IS NOT NULL AND cat_code IN ('RUN','TRL') AND line_number = 1")
         .join(gait_stores, "store_id").join(cs3, "style_id")
         .join(persons.select("person_idx", "gender", "shoe_size", "arch_type", "pronation_type", "is_loyalty", "email"), "person_idx")
         .select("person_idx", "order_id", "order_ts", "order_day", "store_id", "is_cs3", "gender", "shoe_size", "arch_type",
                 "pronation_type", "is_loyalty", "email")
         .repartition(32, "person_idx"))
    write_bronze(g.mapInPandas(gen_gait, GAIT_SCHEMA), "gait_analysis_raw", "volumental_foot_scan", "stores/gait_lab")
