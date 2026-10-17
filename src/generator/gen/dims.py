"""Dimension / reference tables built on the driver (small) and written to truth + bronze."""
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from gen import refdata as R
from gen.config import (BF_DAYS, BF_MAIL_END, BF_MAIL_START, CS3_LAUNCH, END, ESP_START, N_DAYS, SCALE,
                        SEED, START, di, get_spark, md5hex, save_truth, write_bronze)

FAR = 99999  # "no end" day index
LOCALES = {"US": "en_US", "UK": "en_GB", "DE": "de_DE", "CA": "en_CA"}


def _d(i):
    return START + timedelta(days=int(i))


# ---------------------------------------------------------------- geo / stores
def build_geo(rng):
    rows = []
    letters = "ABDEFGHJLNPQRSTUWXYZ"
    for mid, (city, state, ctry, lat, lon, w, prefixes, acs, clim) in enumerate(R.METROS):
        tot_w = sum(m[5] for m in R.METROS if m[2] == ctry)
        n = int(max(8, round(R.POSTAL_TARGET[ctry] * w / tot_w)))
        codes = set()
        while len(codes) < n:
            p = prefixes[rng.integers(len(prefixes))]
            if ctry == "US":
                codes.add(f"{p}{rng.integers(0, 100):02d}")
            elif ctry == "UK":
                codes.add(f"{p}{rng.integers(1, 21)} {rng.integers(1, 10)}{letters[rng.integers(20)]}{letters[rng.integers(20)]}")
            elif ctry == "DE":
                codes.add(f"{p}{rng.integers(0, 1000):03d}")
            else:
                codes.add(f"{p}{'ABCEGHJKLMNPRSTVXY'[rng.integers(18)]} {rng.integers(0, 10)}{letters[rng.integers(20)]}{rng.integers(0, 10)}")
        pop_w = rng.lognormal(0, 0.6, n)
        for code, pw in zip(sorted(codes), pop_w):
            rows.append(dict(postal_code=code, metro_id=mid, city=city, state=state, country=ctry,
                             latitude=round(lat + rng.normal(0, 0.12), 5), longitude=round(lon + rng.normal(0, 0.15), 5),
                             climate=clim, pop_weight=float(w * pw / n)))
    geo = pd.DataFrame(rows)
    geo["country_weight"] = geo["pop_weight"] / geo.groupby("country")["pop_weight"].transform("sum")
    return geo


def _us_region(state):
    if state in {"CA", "WA", "OR", "NV", "AZ", "CO", "UT", "ID", "NM", "HI"}:
        return "US-West"
    if state in {"TX", "FL", "GA", "NC", "TN", "LA", "KY", "OK"}:
        return "US-South"
    if state in {"IL", "MN", "MO", "MI", "OH", "IN", "WI"}:
        return "US-Central"
    return "US-East"


def build_stores(rng, geo):
    fakers = {c: Faker(l) for c, l in LOCALES.items()}
    for i, f in enumerate(fakers.values()):
        f.seed_instance(SEED + i)
    tot_w = sum(m[5] for m in R.METROS)
    rows, sid = [], 1
    order = sorted(range(len(R.METROS)), key=lambda m: -R.METROS[m][5])
    outlet_left, sis_left = 50, 28

    def add(mid, stype, name):
        nonlocal sid
        city, state, ctry, *_ = R.METROS[mid]
        g = geo[geo.metro_id == mid].sample(1, random_state=int(rng.integers(1e9))).iloc[0]
        sq = {"flagship": rng.integers(22000, 42000), "standard": rng.integers(4000, 9500),
              "outlet": rng.integers(6000, 13000), "shop_in_shop": rng.integers(800, 2200)}[stype]
        gait = stype == "flagship" or (stype == "standard" and rng.random() < 0.42)
        club = stype == "flagship" or (stype == "standard" and rng.random() < 0.45)
        region = _us_region(state) if ctry == "US" else {"UK": "UK & Ireland", "DE": "DACH", "CA": "Canada"}[ctry]
        rows.append(dict(store_id=f"ST{sid:04d}", store_name=name, store_type=stype,
                         address_line1=fakers[ctry].street_address().replace("\n", ", "), city=city, state=state,
                         postal_code=g.postal_code, country=ctry, latitude=g.latitude, longitude=g.longitude,
                         region=region, open_date=_d(-int(rng.integers(30, 6500))).isoformat(), sq_ft=int(sq),
                         has_gait_lab=bool(gait), hosts_run_club=bool(club), metro_id=mid))
        sid += 1

    malls = ["Town Center", "Galleria", "Plaza", "Commons", "Crossing", "Square", "Marketplace", "Promenade"]
    for mid in order:
        city, state, ctry, lat, lon, w, *_ = R.METROS[mid]
        n_std = max(1, int(round(w / tot_w * 182)))
        if city in R.FLAGSHIP_CITIES:
            add(mid, "flagship", f"StrideForge Flagship {city}")
            n_std = max(0, n_std - 1)
        for k in range(n_std):
            add(mid, "standard", f"StrideForge {city} {malls[(mid + k) % len(malls)]}")
        if outlet_left > 0 and (w > 2.0 or rng.random() < 0.3):
            add(mid, "outlet", f"StrideForge Outlet {city}")
            outlet_left -= 1
        if sis_left > 0 and rng.random() < 0.45:
            host = {"US": "Macy's", "UK": "JD Sports", "DE": "Karstadt Sports", "CA": "Sport Chek"}[ctry]
            add(mid, "shop_in_shop", f"StrideForge at {host} {city}")
            sis_left -= 1
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- products
def build_products(rng):
    rows = []
    counters = {}

    def sid(code):
        counters[code] = counters.get(code, 0) + 1
        return f"{code}{counters[code]:04d}"

    for code, (division, category, sport, lines, n_ver, genders, variants) in R.FOOTWEAR.items():
        for rank, (line, base, tech) in enumerate(lines):
            # version launch schedule; latest version in-window for ~45% of lines
            if line == "CloudStride":
                launches = [date(2021, 9, 1), date(2023, 3, 1), CS3_LAUNCH]
            elif n_ver == 1:
                launches = [date(2015, 1, 1) + timedelta(days=int(rng.integers(0, 3000)))]
            else:
                latest = (_d(rng.integers(60, N_DAYS - 20)) if rng.random() < 0.45
                          else START - timedelta(days=int(rng.integers(30, 600))))
                launches = sorted(latest - timedelta(days=int(540 * k + rng.integers(-60, 60)))
                                  for k in range(n_ver))
            for vi, launch in enumerate(launches):
                ver = vi + 1
                nxt = launches[vi + 1] if vi + 1 < len(launches) else None
                disc = nxt + timedelta(days=150) if nxt else None
                ver_f = 1.0 if nxt is None else (0.45 if vi == len(launches) - 2 else 0.2)
                for g in genders:
                    for vsuf, vdelta, vw in variants:
                        vname = f" {ver}" if n_ver > 1 else ""
                        gname = {"Men": " Men's", "Women": " Women's", "Unisex": " Unisex", "Kids": ""}[g]
                        tech_l = [tech] + (["GORE-TEX"] if "GTX" in vsuf else [])
                        rows.append(dict(
                            style_id=sid(code), style_name=f"{line}{vname}{vsuf}{gname}", cat_code=code,
                            division=division, category=category, sub_category=f"{category} Shoes",
                            sport=sport, gender=g, technology=",".join(tech_l),
                            msrp=float(base + vdelta + 5 * vi), launch_date=launch, discontinue_date=disc,
                            next_launch=nxt, is_limited_drop=False, collab_partner=None, line=line,
                            weight=float((1 / (rank + 1) ** 0.9) * ver_f * vw * rng.lognormal(0, 0.25)
                                         * (0.55 if g == "Unisex" else 1.0))))
    # collab / limited drops (lifestyle + basketball)
    editions = ["Tokyo Night", "Desert Bloom", "Archive 01", "Midnight Court", "Harbor Fog", "Sunset Strip",
                "Concrete Jungle", "Neon Rain", "Velvet Hour", "Glacier", "Ember", "Old Gold", "Paper Lantern",
                "Static", "Low Tide", "Night Market", "Ultraviolet", "Monochrome", "Terracotta", "Forest Floor"]
    drop_days = np.linspace(40, N_DAYS - 3, 40).astype(int) + rng.integers(-10, 10, 40)
    for k, dd in enumerate(np.clip(drop_days, 30, N_DAYS - 2)):
        code = "LIF" if k % 8 < 5 else "BBL"
        line = R.FOOTWEAR[code][3][k % len(R.FOOTWEAR[code][3])][0]
        partner = R.COLLAB_PARTNERS[k % len(R.COLLAB_PARTNERS)]
        rows.append(dict(style_id=sid(code), style_name=f"{line} x {partner} '{editions[k % 20]}'", cat_code=code,
                         division="Footwear", category=R.FOOTWEAR[code][1], sub_category=f"{R.FOOTWEAR[code][1]} Shoes",
                         sport=R.FOOTWEAR[code][2], gender="Unisex", technology="Classic",
                         msrp=float(rng.choice([180, 200, 220, 250, 300])), launch_date=_d(dd), discontinue_date=None,
                         next_launch=None, is_limited_drop=True, collab_partner=partner, line=line, weight=0.004))
    # apparel
    for name, sub, sport, base in R.APPAREL:
        for g in ["Men", "Women"]:
            for v in R.APPAREL_VARIANTS:
                launch = (_d(rng.integers(0, N_DAYS - 30)) if rng.random() < 0.25
                          else START - timedelta(days=int(rng.integers(30, 1800))))
                disc = launch + timedelta(days=int(rng.integers(500, 1100))) if rng.random() < 0.3 else None
                rows.append(dict(style_id=sid("APP"), style_name=f"{name}{v} {g}'s", cat_code="APP",
                                 division="Apparel", category="Apparel", sub_category=sub, sport=sport, gender=g,
                                 technology="ForgeDry" if sub != "Outerwear" else "StormShield",
                                 msrp=float(base + (15 if v == " Pro" else 0)), launch_date=launch,
                                 discontinue_date=disc, next_launch=None, is_limited_drop=False, collab_partner=None,
                                 line=name, weight=float(rng.lognormal(0, 0.6) * (1.6 if v == "" else 0.7))))
    for name, sub, sport, base in R.APPAREL[:10]:
        for v in ["", " Essentials"]:
            rows.append(dict(style_id=sid("KDA"), style_name=f"{name}{v} Kids", cat_code="KDA", division="Apparel",
                             category="Kids Apparel", sub_category=sub, sport=sport, gender="Kids",
                             technology="ForgeDry", msrp=float(round(base * 0.65)),
                             launch_date=START - timedelta(days=int(rng.integers(30, 900))), discontinue_date=None,
                             next_launch=None, is_limited_drop=False, collab_partner=None, line=name,
                             weight=float(rng.lognormal(0, 0.5))))
    for name, sub, base, sized in R.ACCESSORIES:
        for v in R.ACCESSORY_VARIANTS:
            rows.append(dict(style_id=sid("ACC"), style_name=f"{name}{v}", cat_code="ACC", division="Accessories",
                             category="Accessories", sub_category=sub, sport="Multi-Sport", gender="Unisex",
                             technology="ForgeDry", msrp=float(base + (8 if v == " Pro" else 0)),
                             launch_date=START - timedelta(days=int(rng.integers(30, 1500))), discontinue_date=None,
                             next_launch=None, is_limited_drop=False, collab_partner=None, line=name,
                             weight=float(rng.lognormal(0, 0.5) * (3.0 if sub == "Socks" else 1.0)
                                          * (1.5 if v == "" else 0.6))))
    p = pd.DataFrame(rows)
    p["unit_cost"] = (p.msrp * rng.uniform(0.33, 0.46, len(p))).round(2)
    p["n_colorways"] = np.where(p.division == "Footwear", rng.choice([2, 3, 4, 5, 6], len(p), p=[.15, .3, .3, .15, .1]),
                                np.where(p.division == "Apparel", rng.choice([2, 3, 4, 5], len(p)),
                                         rng.choice([1, 2, 3], len(p))))
    p.loc[p.is_limited_drop, "n_colorways"] = 1
    # lifecycle + markdown windows
    p["launch_day"] = p.launch_date.map(di)
    p["disc_day"] = p.discontinue_date.map(lambda d: di(d) if d else FAR)
    p["next_day"] = p.next_launch.map(lambda d: di(d) if d else FAR)
    md_s, md_e, md_p = np.full(len(p), FAR), np.full(len(p), FAR), np.zeros(len(p))
    for i, r in p.iterrows():
        if r.is_limited_drop:
            continue
        if r.next_day < FAR:                         # superseded version -> clearance until discontinued
            md_s[i], md_e[i], md_p[i] = r.next_day, r.disc_day, 0.30
        elif rng.random() < 0.35:
            lo = max(r.launch_day + 120, 0)
            if lo < N_DAYS - 40:
                s = int(rng.integers(lo, N_DAYS - 30))
                md_s[i], md_e[i], md_p[i] = s, s + int(rng.integers(42, 85)), round(rng.uniform(0.2, 0.4), 2)
    p["md_start"], p["md_end"], p["md_pct"] = md_s, md_e, md_p
    p["lifecycle_status"] = np.where(p.disc_day <= N_DAYS, "discontinued",
                                     np.where(p.launch_day > N_DAYS - 60, "new", "active"))
    p["size_system"] = np.select(
        [p.cat_code.isin(["APP"]), p.cat_code == "KDA", (p.cat_code == "ACC"), p.gender == "Women", p.gender == "Kids"],
        ["ALPHA", "ALPHA_K", "ACC", "US_W", "US_K"], "US_M")
    p["image_url"] = "https://cdn.strideforge.com/img/" + p.style_id + "/main.jpg"
    return p


def size_labels(row):
    ss = row.size_system
    if ss == "US_M":
        return [f"{s:g}" for s in R.MEN_SIZES]
    if ss == "US_W":
        return [f"{s:g}" for s in R.WOMEN_SIZES]
    if ss == "US_K":
        return R.KID_SIZES
    if ss == "ALPHA":
        return R.APPAREL_SIZES
    if ss == "ALPHA_K":
        return R.KID_APPAREL_SIZES
    sized = any(row.style_name.startswith(a[0]) and a[3] for a in R.ACCESSORIES)
    return R.SOCK_SIZES if sized else ["OS"]


def sku_code(style_id, cw, label):
    return f"{style_id}-{cw + 1:02d}-{label.replace('.', '')}"


def build_skus(rng, p):
    rows = []
    for r in p.itertuples():
        palette = R.COLORWAYS if r.division == "Footwear" else R.APPAREL_COLORS
        idx = rng.choice(len(palette), r.n_colorways, replace=False)
        if r.division == "Footwear" and 0 not in idx:
            idx[0] = 0
        for cw, ci in enumerate(sorted(idx)):
            cname, fam = palette[ci]
            for lab in size_labels(r):
                width = ("2E" if "Wide" in r.style_name and r.gender != "Women" else
                         "D" if "Wide" in r.style_name else
                         "B" if r.size_system == "US_W" else "D" if r.size_system == "US_M" else None)
                sku = sku_code(r.style_id, cw, lab)
                rows.append(dict(sku=sku, style_id=r.style_id, colorway=cname, color_family=fam, size=lab,
                                 size_system=r.size_system, width=width,
                                 upc=f"8{int(md5hex(sku)[:10], 16) % 10**11:011d}"))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- campaigns / partners / events
def build_esp_campaigns(rng, drops):
    rows = []

    def add(cid, name, channel, ctype, theme, sport, day):
        rows.append(dict(campaign_id=cid, campaign_name=name, channel=channel, campaign_type=ctype,
                         theme=theme, sport=sport, send_day=int(day)))
    sports = ["Running", "Lifestyle", "Training", "Basketball", "Football", "Trail Running"]
    d0 = di(ESP_START)
    for w in range(53):
        day = d0 + 7 * w + 1          # Tuesday-ish newsletter
        if day >= N_DAYS:
            break
        dt = _d(day)
        iy, iw, _ = dt.isocalendar()
        add(f"EM-{iy}-W{iw:02d}-NL", f"Weekly Newsletter {iy} W{iw:02d}", "email", "batch", "newsletter", None, day)
        sp = sports[w % len(sports)]
        add(f"EM-{iy}-W{iw:02d}-{sp[:3].upper()}", f"{sp} Spotlight {iy} W{iw:02d}", "email", "batch", "sport", sp, day + 3)
        if w % 2 == 0:
            add(f"SMS-{iy}-W{iw:02d}", f"SMS Flash Offer {iy} W{iw:02d}", "sms", "batch", "flash_sale", None, day + 2)
        add(f"PUSH-{iy}-W{iw:02d}", f"App New Arrivals {iy} W{iw:02d}", "push", "batch", "new_arrivals", None, day + 4)
    bf = di(BF_MAIL_START)
    for k in range((BF_MAIL_END - BF_MAIL_START).days + 1):
        add(f"EM-2025-BF{k + 1:02d}", f"Black Friday 2025 - Day {k + 1}", "email", "batch", "black_friday", None, bf + k)
        if k % 3 == 0:
            add(f"SMS-2025-BF{k + 1:02d}", f"Black Friday 2025 SMS {k + 1}", "sms", "batch", "black_friday", None, bf + k)
            add(f"PUSH-2025-BF{k + 1:02d}", f"Black Friday 2025 Push {k + 1}", "push", "batch", "black_friday", None, bf + k)
    for name, d, theme, sp in [("Holiday Gift Guide", date(2025, 12, 5), "holiday", None),
                               ("Last Shipping Day", date(2025, 12, 18), "holiday", None),
                               ("Boxing Day Sale", date(2025, 12, 26), "sale", None),
                               ("New Year New Miles", date(2026, 1, 2), "new_year", "Running"),
                               ("Resolution Training Kit", date(2026, 1, 9), "new_year", "Training"),
                               ("CloudStride 3 Teaser", date(2026, 3, 1), "launch", "Running"),
                               ("CloudStride 3 Is Here", CS3_LAUNCH, "launch", "Running"),
                               ("CloudStride 3 Reviews Are In", date(2026, 3, 12), "launch", "Running"),
                               ("Spring Marathon Prep", date(2026, 3, 24), "seasonal", "Running"),
                               ("Members Double Points Week", date(2026, 5, 12), "loyalty", None),
                               ("Summer Sale", date(2026, 7, 7), "sale", None),
                               ("Back to School", date(2026, 8, 4), "back_to_school", None),
                               ("Back to School - Team Sports", date(2026, 8, 11), "back_to_school", "Football"),
                               ("Fall Marathon Season", date(2026, 9, 8), "seasonal", "Running"),
                               ("Trail Season Waterproof", date(2026, 10, 6), "seasonal", "Trail Running")]:
        if d >= ESP_START:
            add(f"EM-{d:%Y%m%d}-{theme.upper()[:6]}", name, "email", "batch", theme, sp, di(d))
    for r in drops.itertuples():
        if r.release_day - 1 >= d0:
            add(f"EM-{r.drop_id}", f"Drop Alert: {r.drop_name}", "email", "batch", "drop", "Lifestyle", r.release_day - 1)
            add(f"SMS-{r.drop_id}", f"Drop Live: {r.drop_name}", "sms", "batch", "drop", "Lifestyle", r.release_day)
            add(f"PUSH-{r.drop_id}", f"Drop Live: {r.drop_name}", "push", "batch", "drop", "Lifestyle", r.release_day)
    for cid, name in [("TRG-WELCOME-1", "Welcome Series 1"), ("TRG-WELCOME-2", "Welcome Series 2"),
                      ("TRG-WELCOME-3", "Welcome Series 3"), ("TRG-POSTPURCHASE", "Post-Purchase Thank You"),
                      ("TRG-REVIEW", "Review Request"), ("TRG-REPLENISH-RUN", "Time For New Running Shoes?"),
                      ("TRG-BIRTHDAY", "Happy Birthday Reward"), ("TRG-WINBACK-90", "We Miss You - 90 Days")]:
        add(cid, name, "email", "triggered", cid.split("-")[1].lower(), None, -1)
    return pd.DataFrame(rows)


def build_paid_campaigns(rng):
    plats = {"meta": ("paid_social", ["Advantage+ Broad", "Lookalike 1% Purchasers", "Interest: Running",
                                       "Retargeting 7d Viewers", "Interest: Sneakers"]),
             "google": ("cpc", ["Brand Search", "Generic Running Shoes", "Shopping - Footwear", "PMax",
                                "Competitor Terms"]),
             "tiktok": ("paid_social", ["Spark Ads Creators", "Broad 18-34", "Sneaker Culture"]),
             "youtube": ("paid_video", ["In-Market Sports", "Custom Intent Runners", "Retargeting Viewers"])}
    camps, ads = [], []
    q_starts = [date(2023, 10, 1)] + [date(y, m, 1) for y in (2024, 2025, 2026) for m in (1, 4, 7, 10)]
    q_starts = [q for q in q_starts if q <= END]
    for plat, (medium, audiences) in plats.items():
        for ctry in ["US", "UK", "DE", "CA"]:
            for qi, qs in enumerate(q_starts):
                qe = min((q_starts[qi + 1] - timedelta(days=1)) if qi + 1 < len(q_starts) else END, END)
                for obj in ["prospecting", "retargeting"]:
                    camps.append(dict(campaign_id=f"{plat[:2].upper()}-{ctry}-{qs:%Y}Q{(qs.month - 1) // 3 + 1}-{obj[:4].upper()}",
                                      campaign_name=f"{plat.title()} {ctry} {obj.title()} {qs:%Y} Q{(qs.month - 1) // 3 + 1}",
                                      platform=plat, medium=medium, country=ctry, objective=obj, burst=False,
                                      start_day=max(di(qs), 0), end_day=di(qe), budget_scale=1.0))
            bursts = [(f"BF{b.year}", f"Black Friday {b.year}", b - timedelta(days=10), b + timedelta(days=4), 3.0) for b in BF_DAYS]
            bursts += [(f"BTS{y}", f"Back To School {y}", date(y, 7, 25), date(y, 8, 31), 1.8) for y in (2024, 2025, 2026)]
            bursts += [(f"HOL{y}", f"Holiday {y}", date(y, 12, 1), date(y, 12, 23), 2.2) for y in (2023, 2024, 2025)]
            bursts += [("CS3LAUNCH", "CloudStride 3 Launch", CS3_LAUNCH - timedelta(days=3), CS3_LAUNCH + timedelta(days=45), 2.5),
                       ("NY2025", "New Year New Miles 2025", date(2025, 1, 1), date(2025, 1, 25), 1.5),
                       ("NY2026", "New Year New Miles 2026", date(2026, 1, 1), date(2026, 1, 25), 1.5)]
            for code, name, s, e, sc in bursts:
                camps.append(dict(campaign_id=f"{plat[:2].upper()}-{ctry}-{code}", campaign_name=f"{plat.title()} {ctry} {name}",
                                  platform=plat, medium=medium, country=ctry, objective="conversion", burst=True,
                                  start_day=max(di(s), 0), end_day=min(di(e), N_DAYS - 1), budget_scale=sc))
    for c in camps:
        plat_aud = plats[c["platform"]][1]
        for gi in range(3):
            aud = plat_aud[(gi + int(md5hex(c["campaign_id"])[:6], 16)) % len(plat_aud)]
            for ai in range(3):
                ads.append(dict(campaign_id=c["campaign_id"], ad_group_id=f"{c['campaign_id']}-AG{gi + 1}",
                                ad_id=f"{c['campaign_id']}-AG{gi + 1}-AD{ai + 1}", audience_name=aud,
                                account_id=f"act_{c['platform']}_{c['country'].lower()}_{int(md5hex(c['platform'] + c['country'])[:8], 16) % 10**8:08d}"))
    return pd.DataFrame(camps), pd.DataFrame(ads)


def build_affiliates(rng):
    fk = Faker("en_US")
    fk.seed_instance(SEED)
    rows, used = [], set()
    types = ([("coupon_site", n) for n in ["RetailMeNot", "Honey", "Slickdeals", "Rakuten", "VoucherCodes UK",
                                            "mydealz", "CouponCabin", "Groupon", "TopCashback", "Capital One Shopping"]]
             + [("athlete", None)] * 40 + [("creator", None)] * 1200 + [("run_club", None)] * 350 + [("blog", None)] * 400)
    for i, (ptype, pname) in enumerate(types):
        if pname is None:
            if ptype == "run_club":
                pname = f"{fk.city()} Run Club"
            elif ptype == "blog":
                pname = f"{fk.last_name()} {rng.choice(['Runs', 'Kicks', 'Fit', 'Miles', 'Sole', 'Court'])}"
            else:
                pname = fk.name()
        disc = int(rng.choice([10, 15, 20], p=[.45, .4, .15]))
        base = "".join(ch for ch in pname.upper() if ch.isalpha())[:10] or "SF"
        code = f"{base}{disc}"
        while code in used:
            code = f"{base}{rng.integers(10, 99)}"
        used.add(code)
        sport = rng.choice(["Running", "Lifestyle", "Basketball", "Training", "Football"], p=[.4, .3, .1, .12, .08])
        w = {"coupon_site": 60.0, "athlete": 6.0, "creator": 1.0, "run_club": 0.8, "blog": 0.6}[ptype] * rng.lognormal(0, 1.0)
        rows.append(dict(promo_code=code, partner_id=f"AFF{i + 1:05d}", partner_name=pname, partner_type=ptype,
                         sport="Running" if ptype == "run_club" else sport, discount_pct=float(disc),
                         valid_from=(START - timedelta(days=int(rng.integers(0, 400)))).isoformat(),
                         valid_to=(END + timedelta(days=int(rng.integers(30, 400)))).isoformat(), weight=float(w)))
    return pd.DataFrame(rows)


def build_races(rng):
    rows, eid = [], 1
    last = date(2027, 3, 31)
    for mid, (city, state, ctry, lat, lon, w, *_rest) in enumerate(R.METROS):
        clim = R.METROS[mid][8]
        for y in range(2023, 2028):
            plan = [("Half Marathon", 21.1, 4), ("10K", 10.0, 6), ("5K", 5.0, 9)]
            if w > 2.0:
                plan.insert(0, ("Marathon", 42.2, 10 if clim not in ("south_hot", "desert", "tropical") else 2))
            if ctry == "US":
                plan.append(("Turkey Trot 5K", 5.0, 11))
            for name, dist, month in plan:
                if name == "Turkey Trot 5K":
                    d = black_friday(y) - timedelta(days=1)
                else:
                    d = date(y, month, 1) + timedelta(days=int(rng.integers(0, 27)))
                if not (START <= d <= last):
                    continue
                d = d + timedelta(days=(6 - d.weekday()) % 7)          # Sundays
                rows.append(dict(event_id=f"EV{eid:05d}", event_name=f"{city} {name} {y}", event_type=
                                 "marathon" if name == "Marathon" else "half_marathon" if "Half" in name else "10k" if name == "10K" else "5k",
                                 event_date=d, event_city=city, country=ctry, metro_id=mid, distance_km=dist,
                                 popularity=float(w * (3 if name == "Marathon" else 1.5 if "Half" in name else 1))))
                eid += 1
    return pd.DataFrame(rows)


def black_friday(year):
    from gen.config import black_friday as bf
    return bf(year)


def build_sports_calendar(races):
    rows, cid = [], 1

    def add(name, cat, sport, region, s, e, desc):
        nonlocal cid
        rows.append(dict(calendar_event_id=f"CAL{cid:04d}", event_name=name, event_category=cat, sport=sport,
                         region=region, start_date=s.isoformat(), end_date=e.isoformat(), description=desc))
        cid += 1
    for y in range(2023, 2028):
        for region in ["US", "CA"]:
            add(f"Fall School Sports {y}", "school_season", "Football/Soccer", region, date(y, 8, 10), date(y, 11, 20), "High school fall season (football, soccer, cross country)")
            add(f"Winter Basketball {y}-{y + 1}", "school_season", "Basketball", region, date(y, 11, 15), date(y + 1, 3, 15), "School / youth basketball season")
            add(f"Spring Track {y}", "school_season", "Running", region, date(y, 3, 1), date(y, 6, 1), "Track & field and spring sports")
            add(f"Back to School {y}", "retail_moment", "All", region, date(y, 7, 20), date(y, 9, 5), "Back-to-school shopping peak")
        for region in ["UK", "DE"]:
            add(f"Football Season {y}/{str(y + 1)[2:]}", "league_season", "Football", region, date(y, 8, 9), date(y + 1, 5, 25), "Domestic football league season")
            add(f"Back to School {y}", "retail_moment", "All", region, date(y, 8, 15) if region == "UK" else date(y, 8, 1), date(y, 9, 10), "Back-to-school shopping peak")
        for region in ["US", "CA", "UK", "DE"]:
            add(f"New Year Resolutions {y}", "retail_moment", "Training", region, date(y, 1, 1), date(y, 1, 31), "Fitness resolution peak")
            bf = black_friday(y)
            add(f"Black Friday {y}", "retail_moment", "All", region, bf - timedelta(days=6), bf, "Black Friday week")
            add(f"Cyber Monday {y}", "retail_moment", "All", region, bf + timedelta(days=3), bf + timedelta(days=3), "Cyber Monday")
            add(f"Holiday Gifting {y}", "retail_moment", "All", region, date(y, 12, 1), date(y, 12, 24), "Holiday gifting season")
            add(f"Spring Marathon Season {y}", "race_season", "Running", region, date(y, 3, 15), date(y, 5, 15), "Peak spring marathon season")
            add(f"Fall Marathon Season {y}", "race_season", "Running", region, date(y, 9, 15), date(y, 11, 15), "Peak fall marathon season")
            add(f"Trail Season {y}", "race_season", "Trail Running", region, date(y, 5, 1), date(y, 10, 15), "Trail running season")
    for r in races[races.event_type == "marathon"].itertuples():
        add(r.event_name, "major_race", "Running", r.country, r.event_date, r.event_date, f"{r.event_city} marathon")
    return pd.DataFrame(rows)


def build_drops(rng, p):
    d = p[p.is_limited_drop].copy().reset_index(drop=True)
    d["drop_id"] = [f"DRP{i + 1:03d}" for i in range(len(d))]
    d["release_day"] = d.launch_day
    d["release_ts"] = [datetime.combine(x, datetime.min.time()) + timedelta(hours=10) for x in d.launch_date]
    d["units_available"] = (rng.integers(5000, 14000, len(d)) * max(SCALE, 0.02)).astype(int)
    d["release_channel"] = rng.choice(["app_only", "app_and_web", "app_web_store"], len(d), p=[.35, .45, .2])
    d["is_raffle"] = rng.random(len(d)) < 0.4
    d["member_early_access"] = rng.random(len(d)) < 0.5
    # three drops in 2025-26 were hit hard by reseller bots
    recent = d.index[d.release_day > di(date(2025, 3, 1))].tolist()
    d["bot_hit"] = d.index.isin(recent[1:len(recent):max(1, len(recent) // 3)][:3])
    d["drop_name"] = d.style_name
    return d


def build_oos(rng, p, skus):
    act = p[(p.disc_day > N_DAYS) & (p.launch_day < N_DAYS - 20) & (p.division == "Footwear") & ~p.is_limited_drop]
    top = act.sort_values("weight", ascending=False).head(60).style_id
    s = skus[skus.style_id.isin(top)]
    common = s["size"].isin(["9", "9.5", "10", "10.5", "11", "7", "7.5", "8", "8.5", "9"])
    pick = s[common & (rng.random(len(s)) < 0.06)]
    cs3 = p[(p.line == "CloudStride") & (p.launch_day == di(CS3_LAUNCH)) & (p.style_name.str.match(r"CloudStride 3 (Men|Women)"))].style_id
    cs3_oos = skus[skus.style_id.isin(cs3) & skus["size"].isin(["9.5", "10", "10.5", "11", "8", "8.5"])
                   & skus.sku.str.contains("-01-")]
    o = pd.concat([pick, cs3_oos]).drop_duplicates("sku")[["sku", "style_id", "size"]].copy()
    o["oos_start_day"] = N_DAYS - rng.integers(5, 45, len(o))
    o["oos_end_day"] = o.oos_start_day + rng.integers(7, 35, len(o))
    return o


def build_zip_demo(rng, geo):
    base = {"San Francisco": 128000, "Seattle": 116000, "Boston": 112000, "Washington": 117000, "New York": 96000,
            "Los Angeles": 89000, "San Diego": 98000, "Denver": 95000, "Austin": 92000, "Minneapolis": 90000,
            "London": 52000, "München": 58000, "Frankfurt am Main": 54000, "Toronto": 98000, "Vancouver": 95000,
            "Calgary": 100000, "Honolulu": 95000}
    ctry_def = {"US": 74000, "UK": 36000, "DE": 46000, "CA": 85000}
    rows = []
    for g in geo.itertuples():
        inc = base.get(g.city, ctry_def[g.country]) * rng.lognormal(0, 0.28)
        rows.append(dict(postal_code=g.postal_code, city=g.city, state=g.state,
                         population=int(g.pop_weight * 2.4e6 * rng.uniform(0.7, 1.3)) + 800,
                         median_household_income=int(round(inc, -2)), median_age=round(rng.normal(38.5, 4.5), 1),
                         pct_households_with_kids=round(float(np.clip(rng.normal(0.29, 0.07), 0.08, 0.55)), 3),
                         urbanicity=rng.choice(["urban", "suburban", "rural"], p=[.38, .5, .12])))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- driver
def run():
    spark = get_spark()
    from pyspark.sql import functions as F
    rng = np.random.default_rng(SEED)
    print("dims: building reference data", flush=True)
    geo = build_geo(rng)
    stores = build_stores(rng, geo)
    products = build_products(rng)
    skus = build_skus(rng, products)
    drops = build_drops(rng, products)
    oos = build_oos(rng, products, skus)
    esp = build_esp_campaigns(rng, drops)
    paid, ads = build_paid_campaigns(rng)
    aff = build_affiliates(rng)
    races = build_races(rng)
    cal = build_sports_calendar(races)
    zdemo = build_zip_demo(rng, geo)

    def sdf(pdf):
        out = pdf.copy()
        for c in out.columns:
            if out[c].dtype == object:
                nn = out[c].dropna()
                if len(nn) and isinstance(nn.iloc[0], date):
                    out[c] = out[c].map(lambda x: x.isoformat() if isinstance(x, date) else None)
        return spark.createDataFrame(out)

    for name, pdf in [("geo", geo), ("stores", stores), ("products", products), ("skus", skus), ("drops", drops),
                      ("oos_skus", oos), ("esp_campaigns", esp), ("paid_campaigns", paid), ("paid_ads", ads),
                      ("affiliates", aff), ("races", races)]:
        save_truth(sdf(pdf), name)
        print(f"  truth.{name:30s} {len(pdf):>10,d}", flush=True)

    ts0 = F.lit(f"{START} 02:00:00").cast("timestamp")
    s = sdf(stores).withColumn("_event_ts", ts0)
    write_bronze(s, "stores_raw", "store_master_mdm", "mdm/stores")
    pr = (sdf(products).withColumn("description", F.concat(
              F.col("style_name"), F.lit(" - "), F.col("category"), F.lit(" "),
              F.lower(F.col("sub_category")), F.lit(" built with "), F.col("technology"), F.lit(".")))
          .withColumn("launch_date", F.date_format(F.col("launch_date").cast("date"), "yyyy-MM-dd"))
          .withColumn("_event_ts", F.greatest(F.col("launch_date").cast("timestamp") - F.expr("INTERVAL 30 DAYS"), ts0)))
    write_bronze(pr, "products_raw", "pim_akeneo", "pim/products")
    sk = sdf(skus).join(sdf(products).select("style_id", "launch_date"), "style_id") \
        .withColumn("_event_ts", F.greatest(F.col("launch_date").cast("timestamp") - F.expr("INTERVAL 30 DAYS"), ts0))
    write_bronze(sk, "skus_raw", "pim_akeneo", "pim/skus")
    dr = (sdf(drops).withColumn("release_ts", F.date_format(F.col("release_ts").cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
          .withColumn("_event_ts", F.greatest(F.col("launch_date").cast("timestamp") - F.expr("INTERVAL 21 DAYS"), ts0)))
    write_bronze(dr, "product_drops_raw", "launch_calendar", "merch/drops")
    af = sdf(aff).withColumn("_event_ts", F.greatest(F.col("valid_from").cast("timestamp"), ts0))
    write_bronze(af, "affiliate_codes_raw", "impact_affiliate", "affiliate/codes")
    write_bronze(sdf(cal).withColumn("_event_ts", ts0), "sports_calendar_raw", "marketing_calendar", "ref/sports_calendar")
    write_bronze(sdf(zdemo).withColumn("_event_ts", ts0), "zip_demographics_raw", "census_acs_vendor", "ref/demographics")

    # price history: list price per SKU, markdown windows per style, Black Friday promos
    p_sp = sdf(products).select("style_id", "msrp", "launch_date", "discontinue_date", "md_start", "md_end", "md_pct",
                                "launch_day", "disc_day", "is_limited_drop")
    sk_sp = sdf(skus).select("sku", "style_id").join(p_sp, "style_id")
    day = lambda c: F.date_add(F.lit(str(START)).cast("date"), F.col(c).cast("int"))
    lst = sk_sp.select("sku", F.lit("list").alias("price_type"), F.col("msrp").alias("price"),
                       F.col("launch_date").alias("effective_from"), F.col("discontinue_date").alias("effective_to"),
                       F.lit("all").alias("channel"), F.col("launch_date").cast("timestamp").alias("_event_ts"))
    md = sk_sp.where("md_start < 99999").select(
        "sku", F.lit("markdown").alias("price_type"), F.round(F.col("msrp") * (1 - F.col("md_pct")), 2).alias("price"),
        F.date_format(day("md_start"), "yyyy-MM-dd").alias("effective_from"),
        F.date_format(day("md_end"), "yyyy-MM-dd").alias("effective_to"), F.lit("all").alias("channel"),
        day("md_start").cast("timestamp").alias("_event_ts"))
    bfs = spark.createDataFrame(pd.DataFrame({"bf_day": [di(b) for b in BF_DAYS]}))
    bfp = (sk_sp.where("NOT is_limited_drop").crossJoin(bfs)
           .where("launch_day <= bf_day AND disc_day > bf_day")
           .select("sku", F.lit("promo").alias("price_type"), F.round(F.col("msrp") * 0.75, 2).alias("price"),
                   F.date_format(F.date_sub(day("bf_day"), 6), "yyyy-MM-dd").alias("effective_from"),
                   F.date_format(F.date_add(day("bf_day"), 3), "yyyy-MM-dd").alias("effective_to"),
                   F.lit("all").alias("channel"), F.date_sub(day("bf_day"), 6).cast("timestamp").alias("_event_ts")))
    ph = lst.unionByName(md).unionByName(bfp).withColumn(
        "_event_ts", F.greatest(F.col("_event_ts"), ts0))
    write_bronze(ph, "price_history_raw", "pricing_engine", "pricing/price_history")
