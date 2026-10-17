"""Latent 'true person' table (ground truth). Every source profile, order and event derives from it."""
import unicodedata
from datetime import timedelta

import numpy as np
import pandas as pd

from gen import refdata as R
from gen.config import (BF_MAIL_START, COUNTRIES, COUNTRY_W, END, ESP_START, N_DAYS, N_PERSONS, SEED, START,
                        calendar_weights, cum_weights, di, get_spark, md5hex, sample_days, save_truth, truth)

FAR = 99999

SCHEMA = """person_idx long, country string, metro_id int, postal_code string, city string, state string,
latitude double, longitude double, home_store_id string, first_name string, last_name string, gender string,
birth_date date, age int, street string, email string, email_alt string, phone string,
moved_day int, new_street string, new_postal_code string, household_id string, kids_ages array<int>,
channel_pref string, online_share double, has_ecom_account boolean, ecom_dup boolean, guest_only boolean,
has_pos_profile boolean, pos_dup boolean, is_loyalty boolean, loyalty_dup boolean, loyalty_day int,
is_app_user boolean, app_devices int, app_day int, push_enabled boolean,
is_fitness boolean, fitness_day int, fitness_alt_email boolean, weekly_km double, runs_per_week int,
primary_sport string, secondary_sport string, shoe_size double, apparel_size string, arch_type string,
pronation_type string, price_sensitivity string, is_sneakerhead boolean, is_reseller boolean, reseller_cluster int,
acq_day int, end_day int, n_orders int, is_prospect boolean, email_optin boolean, sms_optin boolean,
profiling_optin boolean, paid_optin boolean, do_not_sell boolean, email_optout_day int, email_engagement double,
send_hour int, email_bad boolean, n_cookies int, browse_rate double, app_rate double"""

SPORTS = ["Running", "Lifestyle", "Training", "Basketball", "Football", "Trail Running", "Multi-Sport"]
SPORT_W = [0.30, 0.25, 0.14, 0.10, 0.10, 0.06, 0.05]
FIT_P = {"Running": 0.35, "Trail Running": 0.30, "Training": 0.06}
LOCALES = {"US": "en_US", "UK": "en_GB", "DE": "de_DE", "CA": "en_CA"}
EMAIL_OPT = {"US": 0.68, "CA": 0.60, "UK": 0.52, "DE": 0.45}
SMS_OPT = {"US": 0.22, "CA": 0.20, "UK": 0.12, "DE": 0.08}
PROFILING = {"US": 0.97, "CA": 0.93, "UK": 0.56, "DE": 0.50}


def ascii_name(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return "".join(ch for ch in s if ch.isalpha())


def make_email(rng, first, last, yob, ctry, domain=None):
    f, l = ascii_name(first) or "user", ascii_name(last) or "mail"
    pat = rng.integers(9)
    n2, n3 = rng.integers(10, 100), rng.integers(100, 10000)
    local = [f"{f}.{l}", f"{f}{l}{yob % 100:02d}", f"{f[0]}{l}{n2}", f"{f}_{l}{n3}", f"{f}.{l}{n2}",
             f"{f}{l}", f"{l}.{f}{yob}", f"{f}{n3}", f"{f[0]}.{l}{yob % 100:02d}"][pat]
    if domain is None:
        doms = R.EMAIL_DOMAINS[ctry]
        domain = doms[rng.choice(len(doms), p=np.array([d[1] for d in doms]) / sum(d[1] for d in doms))][0]
    return f"{local}@{domain}"


def make_phone(rng, ctry, area_codes):
    if ctry in ("US", "CA"):
        return f"+1{area_codes[rng.integers(len(area_codes))]}{rng.integers(200, 1000)}{rng.integers(0, 10000):04d}"
    if ctry == "UK":
        return f"+447{rng.integers(100000000, 999999999)}"
    return f"+49{['151', '152', '160', '170', '171', '176', '157'][rng.integers(7)]}{rng.integers(1000000, 99999999)}"


def gen_persons(batches, ctx):
    from faker import Faker
    geo, cw = ctx["geo"], ctx["cw"]
    for b in batches:
        ids = b["id"].to_numpy()
        if len(ids) == 0:
            continue
        rng = np.random.default_rng(SEED + int(ids[0]))
        fk = {c: Faker(l) for c, l in LOCALES.items()}
        for i, f in enumerate(fk.values()):
            f.seed_instance(SEED + int(ids[0]) * 7 + i)
        n = len(ids)
        rows = []
        prev = None
        for k, idx in enumerate(ids):
            idx = int(idx)
            is_reseller = rng.random() < 0.00027
            ctry = "US" if is_reseller else COUNTRIES[rng.choice(4, p=COUNTRY_W)]
            g = geo[ctry]
            gi = rng.choice(len(g["postal"]), p=g["w"])
            mid = int(g["metro"][gi])
            gender = rng.choice(["F", "M", "X"], p=[0.48, 0.50, 0.02])
            first = fk[ctry].first_name_female() if gender == "F" else fk[ctry].first_name_male() if gender == "M" else fk[ctry].first_name()
            last = fk[ctry].last_name()
            age = int(rng.choice([rng.integers(16, 25), rng.integers(25, 35), rng.integers(35, 45),
                                  rng.integers(45, 55), rng.integers(55, 71)], p=[.22, .30, .23, .14, .11]))
            bdate = END - timedelta(days=int(age * 365.25 + rng.integers(0, 365)))
            street = fk[ctry].street_address().replace("\n", ", ")
            postal, city, state = g["postal"][gi], g["city"][gi], g["state"][gi]
            lat, lon = g["lat"][gi], g["lon"][gi]
            reseller_cluster = -1
            if is_reseller:
                reseller_cluster = int(rng.integers(len(ctx["reseller_addr"])))
                street, postal, city, state, mid, lat, lon = ctx["reseller_addr"][reseller_cluster]
            # household pairing with previous person (spouse / partner at same address)
            hh = None
            if prev is not None and prev["country"] == ctry and not is_reseller and rng.random() < 0.12 and idx % 2 == 1:
                street, postal, city, state, mid, lat, lon = (prev["street"], prev["postal_code"], prev["city"],
                                                              prev["state"], prev["metro_id"], prev["latitude"],
                                                              prev["longitude"])
                if rng.random() < 0.7:
                    last = prev["last_name"]
                hh = prev["household_id"]
            hh = hh or "HH" + md5hex(f"{street}|{postal}")[:12].upper()
            kids = []
            if 28 <= age <= 56 and rng.random() < 0.45:
                for _ in range(int(rng.choice([1, 2, 3], p=[.45, .4, .15]))):
                    ka = int(rng.integers(max(1, age - 45), max(2, min(18, age - 22))))
                    kids.append(min(ka, 17))
            yob = bdate.year
            email = make_email(rng, first, last, yob, ctry)
            email_alt = make_email(rng, first, last, yob, ctry) if rng.random() < 0.25 else None
            phone = make_phone(rng, ctry, ctx["area_codes"][mid])
            moved_day, new_street, new_postal = FAR, None, None
            if rng.random() < 0.08 and not is_reseller:
                moved_day = int(rng.integers(30, N_DAYS - 30))
                new_street = fk[ctry].street_address().replace("\n", ", ")
                mg = ctx["metro_postals"][mid]
                new_postal = mg[rng.integers(len(mg))]
            # behaviour
            u = rng.random()
            pref = ("online" if u < 0.35 + (0.08 if age < 30 else 0) else "store" if u < 0.66 else "omni")
            online_share = {"online": 0.95, "store": 0.05}.get(pref, float(rng.beta(2, 2)))
            sport = rng.choice(SPORTS, p=SPORT_W)
            sport2 = rng.choice([s for s in SPORTS if s != sport])
            is_sneaker = rng.random() < (0.08 if sport in ("Lifestyle", "Basketball") else 0.01)
            is_prospect = rng.random() < 0.10 and not is_reseller
            if is_prospect:
                acq = int(sample_days(rng, cw, 0, N_DAYS - 1, 1)[0])
            elif rng.random() < 0.45:
                acq = -int(rng.triangular(1, 1, 3200))
            else:
                acq = int(sample_days(rng, cw, 0, N_DAYS - 1, 1)[0])
            end_day = acq + int(rng.exponential(1500)) + 30
            if is_prospect:
                n_orders = 0
            elif rng.random() < 0.40:
                n_orders = 1 if (acq >= 0 or rng.random() < 0.5) else 0
            else:
                base = 2 + int(rng.lognormal(2.2, 0.9))
                lo, hi = max(acq, 0), min(end_day, N_DAYS - 1)
                frac = max(0.0, (hi - lo + 1) / N_DAYS)
                n_orders = int(min(150, max(1, round(base * frac))))
            if is_reseller:
                n_orders, pref, online_share, end_day = int(rng.integers(3, 10)), "online", 1.0, FAR
            if is_sneaker or is_reseller:
                sport = "Lifestyle" if rng.random() < 0.6 else "Basketball"
            has_acct = (pref != "store" and rng.random() < 0.9) or (pref == "store" and rng.random() < 0.2) or \
                       (is_prospect and rng.random() < 0.6) or is_reseller
            guest_only = pref != "store" and not has_acct
            has_pos = (pref != "online" and rng.random() < 0.72) or (pref == "online" and rng.random() < 0.05)
            is_loy = (not is_reseller) and rng.random() < (0.40 + (0.25 if n_orders > 2 else 0) + (0.05 if pref != "online" else 0))
            loy_day = max(acq, -1500) + int(rng.exponential(60))
            is_app = rng.random() < min(0.95, 0.27 + (0.15 if age < 35 else 0) + (0.35 if is_sneaker else 0) + (0.6 if is_reseller else 0))
            app_day = max(acq, -700) + int(rng.exponential(120))
            fit = (not is_reseller) and rng.random() < FIT_P.get(sport, 0.015)
            fit_day = max(acq, -400) + int(rng.exponential(150))
            wkm = float(np.clip(rng.lognormal(np.log(18), 0.55), 4, 110))
            if gender == "F":
                ss = float(np.clip(np.round(rng.normal(8.0, 1.05) * 2) / 2, 5, 11.5))
                asz = rng.choice(["XS", "S", "M", "L", "XL"], p=[.12, .30, .32, .18, .08])
            else:
                ss = float(np.clip(np.round(rng.normal(10.0, 1.25) * 2) / 2, 6.5, 14))
                asz = rng.choice(["XS", "S", "M", "L", "XL", "XXL"], p=[.02, .15, .32, .30, .16, .05])
            psens = rng.choice(["full_price", "mixed", "deal_seeker"], p=[.30, .45, .25])
            eng = float(rng.beta(2, 7))
            e_opt = rng.random() < EMAIL_OPT[ctry]
            optout = FAR
            if e_opt:
                sub0, esp0 = max(acq, -3000), di(ESP_START)
                pre = max(0, esp0 - sub0)
                if pre > 0 and rng.random() < 1 - np.exp(-pre / 2200):
                    optout = sub0 + int(rng.integers(0, pre))
                else:
                    bf0 = di(BF_MAIL_START)
                    bf_p = 0.06 if (psens == "deal_seeker" or eng < 0.12) else 0.018
                    if sub0 < bf0 and rng.random() < bf_p:
                        optout = bf0 + int(rng.choice(13, p=np.linspace(1, 3, 13) / np.linspace(1, 3, 13).sum()))
                    elif rng.random() < 0.035:
                        lo = max(esp0, sub0, 0)
                        optout = lo + int(rng.integers(0, max(1, N_DAYS - lo)))
            prof = rng.random() < PROFILING[ctry]
            dns = ctry in ("US", "CA") and rng.random() < 0.04
            rows.append(dict(
                person_idx=idx, country=ctry, metro_id=mid, postal_code=postal, city=city, state=state,
                latitude=float(lat), longitude=float(lon),
                home_store_id=ctx["home_stores"][mid][rng.integers(len(ctx["home_stores"][mid]))],
                first_name=first, last_name=last, gender=gender, birth_date=bdate, age=age, street=street,
                email=email, email_alt=email_alt, phone=phone, moved_day=moved_day, new_street=new_street,
                new_postal_code=new_postal, household_id=hh, kids_ages=kids, channel_pref=pref,
                online_share=online_share, has_ecom_account=bool(has_acct), ecom_dup=bool(has_acct and rng.random() < 0.07),
                guest_only=bool(guest_only), has_pos_profile=bool(has_pos), pos_dup=bool(has_pos and rng.random() < 0.12),
                is_loyalty=bool(is_loy), loyalty_dup=bool(is_loy and rng.random() < 0.03), loyalty_day=int(loy_day),
                is_app_user=bool(is_app), app_devices=int(1 if rng.random() < 0.88 else 2), app_day=int(app_day),
                push_enabled=bool(rng.random() < 0.55), is_fitness=bool(fit), fitness_day=int(fit_day),
                fitness_alt_email=bool(rng.random() < 0.2), weekly_km=round(wkm, 1),
                runs_per_week=int(np.clip(round(wkm / 7.5), 1, 7)), primary_sport=sport, secondary_sport=sport2,
                shoe_size=ss, apparel_size=asz, arch_type=rng.choice(["normal", "flat", "high"], p=[.6, .25, .15]),
                pronation_type=rng.choice(["neutral", "overpronation", "supination"], p=[.55, .35, .10]),
                price_sensitivity=psens, is_sneakerhead=bool(is_sneaker), is_reseller=bool(is_reseller),
                reseller_cluster=reseller_cluster, acq_day=int(acq), end_day=int(end_day), n_orders=int(n_orders),
                is_prospect=bool(is_prospect), email_optin=bool(e_opt), sms_optin=bool(rng.random() < SMS_OPT[ctry]),
                profiling_optin=bool(prof), paid_optin=bool(prof and not dns and rng.random() < 0.97), do_not_sell=bool(dns),
                email_optout_day=int(optout), email_engagement=round(eng, 4),
                send_hour=int(rng.choice([7, 8, 12, 17, 18, 19, 20, 21], p=[.1, .1, .12, .1, .14, .17, .16, .11])),
                email_bad=bool(rng.random() < 0.015), n_cookies=int(rng.choice([1, 2, 3, 4], p=[.35, .35, .2, .1])),
                browse_rate=round(float(rng.lognormal(0, 0.8)), 3), app_rate=round(float(rng.lognormal(np.log(2), 0.7)), 3)))
            prev = rows[-1]
        yield pd.DataFrame(rows)


def build_ctx():
    spark = get_spark()
    geo = truth("geo").toPandas()
    stores = truth("stores").toPandas()
    gctx = {}
    for c, g in geo.groupby("country"):
        w = g.country_weight.to_numpy()
        gctx[c] = dict(postal=g.postal_code.to_numpy(), metro=g.metro_id.to_numpy(), city=g.city.to_numpy(),
                       state=g.state.to_numpy(), lat=g.latitude.to_numpy(), lon=g.longitude.to_numpy(), w=w / w.sum())
    home = {}
    for mid in range(len(R.METROS)):
        s = stores[(stores.metro_id == mid) & stores.store_type.isin(["standard", "flagship"])]
        if s.empty:
            s = stores[stores.metro_id == mid]
        home[mid] = s.store_id.tolist()
    metro_postals = {int(m): g.postal_code.tolist() for m, g in geo.groupby("metro_id")}
    rng = np.random.default_rng(SEED + 99)
    us = geo[(geo.country == "US") & geo.city.isin(["New York", "Los Angeles", "Houston", "Atlanta", "Miami", "Chicago"])]
    from faker import Faker
    fk = Faker("en_US")
    fk.seed_instance(SEED)
    resell = []
    for _ in range(25):
        r = us.iloc[rng.integers(len(us))]
        resell.append((f"{rng.integers(100, 9999)} {fk.street_name()} Unit {rng.integers(1, 40)}", r.postal_code, r.city,
                       r.state, int(r.metro_id), float(r.latitude), float(r.longitude)))
    return dict(geo=gctx, cw=cum_weights(calendar_weights()), home_stores=home, metro_postals=metro_postals,
                area_codes={i: m[7] for i, m in enumerate(R.METROS)}, reseller_addr=resell)


def run():
    from pyspark.sql import Window, functions as F
    spark = get_spark()
    ctx = build_ctx()
    parts = max(8, min(128, N_PERSONS // 12000))
    print(f"persons: generating {N_PERSONS:,} people in {parts} partitions", flush=True)
    df = spark.range(0, N_PERSONS, numPartitions=parts).mapInPandas(lambda it: gen_persons(it, ctx), SCHEMA)
    w = Window.partitionBy(F.lower("email")).orderBy("person_idx")
    df = (df.withColumn("_rn", F.row_number().over(w))
            .withColumn("email", F.when(F.col("_rn") > 1, F.regexp_replace(
                "email", "@", F.concat((F.col("person_idx") % 9973).cast("string"), F.lit("@")))).otherwise(F.col("email")))
            .drop("_rn"))
    p = save_truth(df, "persons")
    print(f"  truth.persons {p.count():,}", flush=True)
