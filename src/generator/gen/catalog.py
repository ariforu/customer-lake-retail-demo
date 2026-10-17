"""Picklable product catalog + samplers shared by order and browsing generators."""
import numpy as np

from gen import refdata as R
from gen.config import BF_DAYS, CS3_FIX, CS3_LAUNCH, FX, di, truth

FW_CATS = ["RUN", "TRL", "BBL", "TRN", "LIF", "FTB"]
FW_W = np.array([0.30, 0.05, 0.10, 0.15, 0.30, 0.10])
SPORT_CAT = {"Running": "RUN", "Trail Running": "TRL", "Basketball": "BBL", "Training": "TRN",
             "Lifestyle": "LIF", "Football": "FTB"}
APP_SPORT = {"Running": "Running", "Trail Running": "Running", "Training": "Training", "Lifestyle": "Lifestyle",
             "Basketball": "Other", "Football": "Other", "Multi-Sport": "Other"}
BF_IDX = [di(b) for b in BF_DAYS]
CS3_L, CS3_F = di(CS3_LAUNCH), di(CS3_FIX)


def build_catalog():
    p = truth("products").toPandas().sort_values("style_id").reset_index(drop=True)
    n = len(p)
    C = {k: p[k].to_numpy() for k in ["style_id", "cat_code", "division", "gender", "msrp", "unit_cost", "n_colorways",
                                       "launch_day", "disc_day", "next_day", "md_start", "md_end", "md_pct",
                                       "is_limited_drop", "size_system", "sport", "line", "style_name", "sub_category"]}
    C["n"] = n
    acc_sized = {a[0] for a in R.ACCESSORIES if a[3]}
    C["acc_sized"] = np.array([any(nm.startswith(a) for a in acc_sized) for nm in p.style_name])
    C["is_cs3"] = np.array([nm.startswith("CloudStride 3") and c == "RUN" for nm, c in zip(p.style_name, p.cat_code)])
    # successor: same line/gender/variant, next version
    key = {}
    for i, r in p.iterrows():
        base = r.style_name.replace(f"{r.line} ", "", 1)
        parts = base.split(" ", 1)
        if parts[0].isdigit():
            key[(r.cat_code, r.line, r.gender, int(parts[0]), parts[1] if len(parts) > 1 else "")] = i
    succ = np.full(n, -1)
    for (c, l, g, v, rest), i in key.items():
        succ[i] = key.get((c, l, g, v + 1, rest), -1)
    C["succ"] = succ
    pools = {}
    w = p.weight.to_numpy()
    sport_grp = np.array([APP_SPORT.get(s, "Other") for s in p.sport])
    for cat in FW_CATS + ["KDF", "APP", "KDA", "ACC"]:
        for gg, genders in [("M", ("Men", "Unisex")), ("F", ("Women", "Unisex")), ("K", ("Kids",))]:
            if cat in ("KDF", "KDA"):
                genders = ("Kids",)
            if cat == "ACC":
                genders = ("Unisex",)
            m = (p.cat_code.to_numpy() == cat) & p.gender.isin(genders).to_numpy() & ~p.is_limited_drop.to_numpy()
            idx = np.where(m)[0]
            if len(idx) == 0:
                continue
            if cat == "APP":
                for sg in ["Running", "Training", "Lifestyle", "Other"]:
                    ww = w[idx] * np.where(sport_grp[idx] == sg, 3.0, 1.0)
                    pools[(cat, gg, sg)] = (idx, np.cumsum(ww), idx[np.argsort(-ww)])
            pools[(cat, gg, None)] = (idx, np.cumsum(w[idx]), idx[np.argsort(-w[idx])])
    C["pools"] = pools
    return C


def available(C, j, day):
    return C["launch_day"][j] <= day < C["disc_day"][j]


def sample_style(C, rng, cat, gg, day, sport=None):
    pool = C["pools"].get((cat, gg, sport)) or C["pools"].get((cat, gg, None)) or C["pools"].get((cat, "M", None))
    idx, cw, ranked = pool
    for _ in range(8):
        j = idx[min(len(idx) - 1, np.searchsorted(cw, rng.random() * cw[-1], side="right"))]
        if available(C, j, day):
            return int(j)
    for j in ranked[:80]:
        if available(C, j, day):
            return int(j)
    return -1


def footwear_cat(rng, sport):
    if sport in SPORT_CAT:
        return SPORT_CAT[sport]
    return FW_CATS[rng.choice(6, p=FW_W)]


def size_label(C, j, rng, gender, shoe_size, apparel_size, gift=False):
    ss = C["size_system"][j]
    if ss in ("US_M", "US_W"):
        if gift:
            s = rng.normal(10.0, 1.3) if ss == "US_M" else rng.normal(8.0, 1.1)
        else:
            s = shoe_size
            if ss == "US_M" and gender == "F":
                s -= 1.5
            elif ss == "US_W" and gender != "F":
                s += 1.5
        lo, hi = (4.0, 14.0) if ss == "US_M" else (5.0, 12.0)
        s = float(np.clip(np.round(s * 2) / 2, lo, hi))
        return f"{s:g}"
    if ss == "US_K":
        return R.KID_SIZES[int(np.clip(rng.normal(11, 4.5), 0, len(R.KID_SIZES) - 1))]
    if ss == "ALPHA":
        if gift:
            return str(rng.choice(R.APPAREL_SIZES, p=[.05, .2, .32, .27, .12, .04]))
        return apparel_size
    if ss == "ALPHA_K":
        return str(rng.choice(R.KID_APPAREL_SIZES, p=[.1, .25, .3, .25, .1]))
    if C["acc_sized"][j]:
        return "S" if shoe_size < 7 else "M" if shoe_size < 9.5 else "L" if shoe_size < 12 else "XL"
    return "OS"


CW_W = np.array([0.38, 0.25, 0.16, 0.11, 0.06, 0.04])


def colorway(C, rng, j):
    k = int(C["n_colorways"][j])
    w = CW_W[:k] / CW_W[:k].sum()
    return int(rng.choice(k, p=w))


def sku_of(C, j, cw, label):
    return f"{C['style_id'][j]}-{cw + 1:02d}-{label.replace('.', '')}"


def local_price(msrp, country):
    return float(msrp) if country == "US" else float(round(msrp * FX[country]))


def price_at(C, j, day, country, outlet, rng):
    """returns (unit_price, is_markdown)"""
    base = local_price(C["msrp"][j], country)
    if C["is_limited_drop"][j]:
        return base, False
    pct = 0.0
    if C["md_start"][j] <= day <= C["md_end"][j]:
        pct = C["md_pct"][j]
    if any(-6 <= day - b <= 3 for b in BF_IDX):
        pct = max(pct, 0.25)
    if outlet and rng.random() < 0.75:
        pct = max(pct, 0.30 + 0.1 * rng.random())
    if pct > 0:
        return round(base * (1 - pct), 2), True
    return base, False
