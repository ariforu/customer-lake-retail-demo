"""Orders, order lines and returns for identified people + anonymous store traffic.

Story mechanics embedded here:
  * seasonality / BF / holiday via calendar weights
  * CloudStride 3 (launch 2026-03-05) runs small -> 'too small' return spike, refunded runners churn
  * size guidance from 2026-07-15 reduces CS3 fit returns
  * run-app users replace shoes at ~330-500 miles; ~38% don't, half of those lapse
  * sneakerheads + reseller clusters on limited drops (3 bot-hit drops)
"""
from datetime import timedelta

import numpy as np
import pandas as pd

from gen import catalog as K
from gen import messy as M
from gen.config import (TRUTH, CURRENCY, END, FREE_SHIP, N_ANON_POS, N_DAYS, ONLINE_HOUR_W, SEED, SHIP_FEE, START,
                        STORE_HOUR_W, TAX, calendar_weights, cum_weights, di, ecom_id, get_spark, loyalty_card,
                        md5hex, perm, pos_id, sample_days, save_truth, to_ts, truth, write_bronze)
from gen.profiles import app_devices

FAR = 99999
SCHEMA = """person_idx long, order_id string, channel string, store_id string, country string, currency string,
order_day int, order_ts timestamp, order_kind string, line_number int, sku string, style_id string, cat_code string,
division string, quantity int, unit_price double, line_discount double, line_total double, unit_cost double,
is_markdown boolean, subtotal double, discount_amount double, shipping_amount double, tax_amount double,
total_amount double, payment_type string, promo_code string, fulfillment_type string, pickup_store_id string,
shipping_address string, device_type string, utm_source string, utm_medium string, utm_campaign string,
session_id string, ecom_customer_id string, guest_email string, pos_customer_id string, loyalty_card_number string,
loyalty_earn boolean, register_id string, associate_id string, is_gift boolean, returned boolean, return_id string,
return_day int, return_ts timestamp, return_reason string, return_reason_text string, return_type string,
return_channel string, refund_amount double"""
COLS = [c.split()[0] for c in SCHEMA.replace("\n", " ").split(", ")]

REASONS = ["too_small", "too_big", "comfort", "defect", "not_as_pictured", "changed_mind", "other"]
R_BASE = np.array([.20, .17, .12, .06, .10, .28, .07])
R_CS3 = np.array([.68, .04, .10, .03, .03, .10, .02])
R_CS3_FIXED = np.array([.35, .10, .15, .05, .05, .25, .05])
REASON_TEXT = {
    "too_small": ["Runs small, need half size up", "Too tight in the toe box", "Size too small", "Ordered my usual size but it's tight",
                  "Narrow fit, too small", "Toes hit the front on runs"],
    "too_big": ["Too big", "Heel slips, size down needed", "Runs large", "Too roomy"],
    "comfort": ["Not comfortable", "Caused blisters", "Arch support not right for me", "Too firm"],
    "defect": ["Sole separating", "Stitching came apart", "Arrived damaged", "Glue marks on upper"],
    "not_as_pictured": ["Color different from photos", "Not as pictured", "Material looks cheaper than online"],
    "changed_mind": ["Changed my mind", "Found a different style", "Gift not needed", "Bought two sizes, returning one"],
    "other": ["Other", "Late delivery", "Duplicate order"],
}
PAY = {"US": (["credit_card", "debit_card", "paypal", "apple_pay", "afterpay", "gift_card"], [.52, .15, .12, .12, .06, .03]),
       "CA": (["credit_card", "debit_card", "paypal", "apple_pay", "gift_card"], [.55, .2, .1, .12, .03]),
       "UK": (["credit_card", "debit_card", "paypal", "apple_pay", "klarna", "gift_card"], [.3, .35, .13, .12, .07, .03]),
       "DE": (["paypal", "credit_card", "debit_card", "klarna", "apple_pay", "gift_card"], [.38, .18, .14, .18, .09, .03])}
UTM = [("direct", None), ("google", "organic"), ("email", "email"), ("google", "cpc"), ("meta", "paid_social"),
       ("tiktok", "paid_social"), ("affiliate", "affiliate"), ("sms", "sms"), ("youtube", "paid_video"), ("referral", "referral")]
UTM_W = np.array([.28, .20, .17, .10, .09, .04, .04, .03, .02, .03])


def _ctx():
    C = K.build_catalog()
    drops = truth("drops").toPandas()
    sid = {s: i for i, s in enumerate(C["style_id"])}
    drop_list = [(int(r.release_day), sid[r.style_id], bool(r.bot_hit)) for r in drops.itertuples()]
    aff = truth("affiliates").toPandas()
    paid = truth("paid_campaigns").toPandas()
    paid_idx = {}
    for (plat, ctry), g in paid.groupby(["platform", "country"]):
        paid_idx[(plat, ctry)] = (g.start_day.to_numpy(), g.end_day.to_numpy(), g.campaign_id.to_numpy(), g.burst.to_numpy())
    stores = truth("stores").toPandas()
    st = {}
    for c, g in stores.groupby("country"):
        w = g.store_type.map({"flagship": 4.0, "standard": 1.0, "outlet": 1.4, "shop_in_shop": 0.4}).to_numpy()
        st[c] = (g.store_id.to_numpy(), w / w.sum(), (g.store_type == "outlet").to_numpy())
    outlets = set(stores[stores.store_type == "outlet"].store_id)
    cw = calendar_weights()
    return dict(C=C, drops=drop_list, aff=(aff.promo_code.to_numpy(), (aff.weight / aff.weight.sum()).to_numpy(),
                                          aff.discount_pct.to_numpy()), paid=paid_idx, stores=st, outlets=outlets,
                cw=cum_weights(cw), store_country={r.store_id: r.country for r in stores.itertuples()})


def _iso_week_nl(day):
    d = START + timedelta(days=int(day))
    y, w, _ = d.isocalendar()
    return f"EM-{y}-W{w:02d}-NL"


def _paid_campaign(ctx, rng, plat, ctry, day):
    s, e, ids, burst = ctx["paid"][(plat, ctry)]
    m = (s <= day) & (e >= day)
    if not m.any():
        return None
    cand = np.where(m & burst)[0] if (m & burst).any() and rng.random() < 0.6 else np.where(m)[0]
    return str(ids[cand[rng.integers(len(cand))]])


class OrderBuilder:
    """Turns (person, day, kind) into order line rows."""

    def __init__(self, ctx, rng):
        self.ctx, self.C, self.rng = ctx, ctx["C"], rng

    def line(self, j, p_gender, shoe, app_size, day, ctry, outlet, gift=False, qty=1, size=None):
        C, rng = self.C, self.rng
        cw = K.colorway(C, rng, j)
        lab = size or K.size_label(C, j, rng, p_gender, shoe, app_size, gift)
        price, md = K.price_at(C, j, day, ctry, outlet, rng)
        return dict(j=j, sku=K.sku_of(C, j, cw, lab), qty=qty, price=price, md=md, gift=gift)

    def regular_lines(self, p, day, ctry, outlet, gg):
        C, rng = self.C, self.rng
        n = int(rng.choice([1, 2, 3, 4, 5], p=[.64, .23, .08, .03, .02]))
        d = START + timedelta(days=int(day))
        holiday = (d.month == 12 and d.day <= 24) or (d.month == 11 and d.day >= 20)
        kids_p = 0.0 if not len(p.kids_ages) else (0.30 if d.month in (8, 12) else 0.15)
        out = []
        for _ in range(n):
            gift = holiday and rng.random() < 0.22
            u = rng.random()
            g = gg
            if rng.random() < kids_p:
                cat, g = ("KDF" if rng.random() < 0.6 else "KDA"), "K"
            elif u < 0.42:
                cat = K.footwear_cat(rng, p.primary_sport)
            elif u < 0.52:
                cat = K.footwear_cat(rng, p.secondary_sport)
            elif u < 0.60:
                cat = K.FW_CATS[rng.choice(6, p=K.FW_W)]
            elif u < 0.88:
                cat = "APP"
            else:
                cat = "ACC"
            if gift and g != "K":
                g = "M" if rng.random() < 0.5 else "F"
            sp = K.APP_SPORT.get(p.primary_sport, "Other") if cat == "APP" else None
            j = K.sample_style(C, rng, cat, g, day, sp)
            if j < 0:
                continue
            qty = 1 if rng.random() < 0.94 else (2 if rng.random() < 0.8 else 3)
            out.append(self.line(j, p.gender if not gift else g.replace("M", "M").replace("F", "F"), p.shoe_size,
                                 p.apparel_size, day, ctry, outlet, gift=gift, qty=qty))
        return out


def gen_orders(batches, ctx):
    C = ctx["C"]
    cw = ctx["cw"]
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 7 + int(b["person_idx"].iloc[0]))
        ob = OrderBuilder(ctx, rng)
        rows = []
        for p in b.itertuples():
            lo, hi = max(p.acq_day, 0), min(p.end_day, N_DAYS - 1)
            if lo > hi:
                continue
            plan = []
            if p.n_orders > 0:
                if p.acq_day >= 0:
                    plan.append((min(hi, lo + int(rng.integers(0, 4))), "regular", None))
                    plan += [(int(d), "regular", None) for d in sample_days(rng, cw, lo, hi, p.n_orders - 1)]
                else:
                    plan += [(int(d), "regular", None) for d in sample_days(rng, cw, lo, hi, p.n_orders)]
            for rd, j, bot in ctx["drops"]:
                if lo <= rd <= hi:
                    if p.is_reseller and rng.random() < 0.85:
                        plan.append((rd, "drop_resell", (j, bot)))
                    elif p.is_sneakerhead and rng.random() < 0.15:
                        plan.append((rd, "drop", (j, bot)))
            if p.is_fitness:
                mpd = p.weekly_km * 0.621 / 7
                t = max(p.fitness_day, lo)
                if t <= hi:
                    plan.append((t, "run_shoe", None))
                while True:
                    t_next = t + int(rng.uniform(330, 500) / mpd)
                    if t_next > hi:
                        break
                    if rng.random() < 0.62:
                        t = t_next + int(rng.integers(0, 30))
                    else:
                        if rng.random() < 0.5:
                            hi = min(hi, t_next + 60)          # worn-out shoes, no replacement -> lapse
                        t = t_next + int(rng.integers(90, 160))
                    if t <= hi:
                        plan.append((t, "replacement", None))
            plan.sort(key=lambda x: x[0])
            gg = "F" if p.gender == "F" else "M"
            devs = app_devices(p) if p.is_app_user else []
            fav = -1
            first_online = True
            for k, (day, kind, extra) in enumerate(plan):
                if day > hi or k >= 250:
                    continue
                ctry = p.country
                # ---- channel / store
                if kind in ("drop", "drop_resell"):
                    online = rng.random() < 0.92
                else:
                    online = rng.random() < p.online_share
                store_id, outlet = None, False
                if online:
                    channel = "app" if (devs and day >= devs[0][1] and rng.random() < (0.75 if kind.startswith("drop") else 0.5)) else "web"
                else:
                    channel = "store"
                    if rng.random() < (0.25 if p.price_sensitivity == "deal_seeker" else 0.08):
                        ids, w, is_out = ctx["stores"][ctry]
                        oi = np.where(is_out)[0]
                        store_id = str(ids[oi[rng.integers(len(oi))]]) if len(oi) else p.home_store_id
                    elif rng.random() < 0.85:
                        store_id = p.home_store_id
                    else:
                        ids, w, _ = ctx["stores"][ctry]
                        store_id = str(ids[rng.choice(len(ids), p=w)])
                    outlet = store_id in ctx["outlets"]
                # ---- lines
                if kind == "regular":
                    lines = ob.regular_lines(p, day, ctry, outlet, gg)
                elif kind in ("run_shoe", "replacement"):
                    cat = "TRL" if (p.primary_sport == "Trail Running" and rng.random() < 0.7) else "RUN"
                    j = -1
                    if fav >= 0 and C["succ"][fav] >= 0 and day >= C["launch_day"][C["succ"][fav]] and rng.random() < 0.75:
                        j = int(C["succ"][fav])
                    elif fav >= 0 and K.available(C, fav, day) and rng.random() < 0.6:
                        j = fav
                    if j < 0:
                        j = K.sample_style(C, rng, cat, gg, day)
                    if j < 0:
                        continue
                    if fav < 0 or rng.random() < 0.6:
                        fav = j
                    lines = [ob.line(j, p.gender, p.shoe_size, p.apparel_size, day, ctry, outlet)]
                    if rng.random() < 0.35:
                        js = K.sample_style(C, rng, "ACC", gg, day)
                        if js >= 0:
                            lines.append(ob.line(js, p.gender, p.shoe_size, p.apparel_size, day, ctry, outlet))
                    if rng.random() < 0.25:
                        ja = K.sample_style(C, rng, "APP", gg, day, "Running")
                        if ja >= 0:
                            lines.append(ob.line(ja, p.gender, p.shoe_size, p.apparel_size, day, ctry, outlet))
                elif kind == "drop":
                    j, _ = extra
                    lines = [ob.line(j, p.gender, p.shoe_size, p.apparel_size, day, ctry, False)]
                    if rng.random() < 0.15:
                        lines.append(ob.line(j, p.gender, p.shoe_size + 0.5, p.apparel_size, day, ctry, False))
                else:  # reseller: many sizes of one SKU family
                    j, bot = extra
                    nl = int(rng.integers(6, 11)) if bot else int(rng.integers(3, 7))
                    sizes = rng.choice(np.arange(7, 13.5, 0.5), nl, replace=False)
                    lines = [ob.line(j, "M", 10, "M", day, ctry, False, qty=int(rng.choice([1, 2], p=[.6, .4])),
                                     size=f"{s:g}") for s in sizes]
                if not lines:
                    continue
                # ---- header
                hour_w = STORE_HOUR_W if channel == "store" else ONLINE_HOUR_W
                sec = int(rng.choice(24, p=hour_w)) * 3600 + int(rng.integers(0, 3600))
                if kind.startswith("drop"):
                    sec = 10 * 3600 + int(rng.exponential(240 if kind == "drop_resell" else 1500))
                ts = to_ts(day, sec)
                seq = p.person_idx * 256 + k
                if channel == "store":
                    order_id = f"TX{perm(seq, 7_919_011, 104_729, 10**12):012d}"
                else:
                    order_id = f"SF{ctry}{perm(seq, 4_256_249, 9_999_991, 10**11):011d}"
                ecom_cid = guest = pos_cid = card = None
                earn = False
                reg = assoc = None
                if channel != "store":
                    if p.is_reseller:
                        n_acc = 3 + p.person_idx % 6
                        ecom_cid = ecom_id(p.person_idx, int(rng.choice([0] + list(range(2, 2 + n_acc)))))
                    elif (not p.has_ecom_account) or p.guest_only or rng.random() < 0.05:
                        guest = p.email if (p.email_alt is None or rng.random() < 0.8) else p.email_alt
                    else:
                        ecom_cid = ecom_id(p.person_idx, 1 if (p.ecom_dup and rng.random() < 0.3) else 0)
                    earn = bool(ecom_cid and p.is_loyalty and p.loyalty_day <= day)
                else:
                    if p.has_pos_profile and rng.random() < 0.55:
                        pos_cid = pos_id(p.person_idx, 1 if (p.pos_dup and rng.random() < 0.15) else 0)
                    if p.is_loyalty and p.loyalty_day <= day and rng.random() < 0.85:
                        card = loyalty_card(p.person_idx)
                        earn = True
                    reg = f"{int(rng.integers(1, 9)):02d}"
                    assoc = f"A{int(md5hex(store_id + str(rng.integers(0, 40)))[:5], 16) % 100000:05d}"
                # promo
                promo, pct = None, 0.0
                d = START + timedelta(days=int(day))
                if channel != "store":
                    if first_online and p.acq_day >= 0 and rng.random() < 0.3:
                        promo, pct = "WELCOME15", 0.15
                    elif rng.random() < (0.08 if p.price_sensitivity == "deal_seeker" else 0.035):
                        codes, w, disc = ctx["aff"]
                        ai = rng.choice(len(codes), p=w)
                        promo, pct = str(codes[ai]), float(disc[ai]) / 100
                    elif d.month == 1 and d.day <= 7 and rng.random() < 0.15:
                        promo, pct = "NEWYEAR20", 0.20
                    first_online = False
                else:
                    if earn and rng.random() < 0.04:
                        promo, pct = "MEMBER15", 0.15
                    elif p.age < 24 and rng.random() < 0.06:
                        promo, pct = "STUDENT10", 0.10
                if kind.startswith("drop"):
                    promo, pct = None, 0.0
                subtotal = round(sum(l["price"] * l["qty"] for l in lines), 2)
                discount = round(subtotal * pct, 2)
                fulfil, pickup, ship_addr = "in_store", None, None
                street = p.new_street if p.moved_day <= day else p.street
                postal = p.new_postal_code if p.moved_day <= day else p.postal_code
                if channel != "store":
                    u = rng.random()
                    fulfil = "ship" if u < 0.82 or p.is_reseller else ("bopis" if u < 0.95 else "ship_from_store")
                    if fulfil == "bopis":
                        pickup = p.home_store_id
                    else:
                        ship_addr = f"{street}, {p.city}, {p.state} {postal}, {ctry}"
                ship_fee = SHIP_FEE[ctry] if (fulfil in ("ship", "ship_from_store") and subtotal - discount < FREE_SHIP[ctry]
                                              and not earn) else 0.0
                tax = round((subtotal - discount) * TAX[ctry], 2)
                total = round(subtotal - discount + ship_fee + tax, 2)
                pays, pw = PAY[ctry]
                pay = str(rng.choice(pays, p=pw)) if channel == "store" or rng.random() > 0.0 else None
                if channel == "store" and rng.random() < 0.12:
                    pay = "cash"
                utm_s = utm_m = utm_c = sess = dev = None
                if channel != "store":
                    sess = "s_" + md5hex(order_id)[:24]
                    if channel == "app":
                        dev = devs[0][3]
                        if rng.random() < 0.25:
                            utm_s, utm_m, utm_c = "braze", "push", f"PUSH-{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"
                    else:
                        dev = str(rng.choice(["mobile", "desktop", "tablet"], p=[.62, .33, .05]))
                        ui = rng.choice(len(UTM), p=UTM_W)
                        utm_s, utm_m = UTM[ui]
                        if utm_s == "email" and not (p.email_optin and p.email_optout_day > day):
                            utm_s, utm_m = "direct", None
                        if utm_s == "sms" and not p.sms_optin:
                            utm_s, utm_m = "direct", None
                        if promo and promo in ctx["aff"][0]:
                            utm_s, utm_m = "affiliate", "affiliate"
                        if utm_s == "email":
                            utm_c = _iso_week_nl(day)
                        elif utm_m in ("cpc", "paid_social", "paid_video"):
                            utm_c = _paid_campaign(ctx, rng, utm_s, ctry, day)
                        elif utm_s == "affiliate":
                            utm_c = promo
                        elif utm_s == "sms":
                            utm_c = f"SMS-{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"
                        if utm_s == "direct":
                            utm_s = None
                # ---- returns + rows
                churn_after = None
                for ln, l in enumerate(lines, start=1):
                    j = l["j"]
                    div = C["division"][j]
                    is_cs3 = bool(C["is_cs3"][j])
                    if channel != "store":
                        rate = {"Footwear": .18, "Apparel": .27}.get(div, .05)
                    else:
                        rate = {"Footwear": .07, "Apparel": .09}.get(div, .02)
                    probs = R_BASE
                    if is_cs3 and day < K.CS3_F:
                        rate, probs = (.42 if channel != "store" else .26), R_CS3
                    elif is_cs3:
                        rate, probs = .20, R_CS3_FIXED
                    if C["is_limited_drop"][j]:
                        rate = 0.0 if p.is_reseller else 0.03
                    rate += (0.10 if l["gift"] else 0) + (0.03 if p.price_sensitivity == "deal_seeker" else 0)
                    returned = rng.random() < rate
                    rday = rts = rid = reason = rtext = rtype = rchan = None
                    refund = None
                    if returned:
                        rday = day + int(rng.integers(4, 29) if channel != "store" else rng.integers(1, 22))
                        if rday > N_DAYS - 1:
                            returned = False
                            rday = None
                    if returned:
                        reason = REASONS[rng.choice(7, p=probs)]
                        rtext = REASON_TEXT[reason][rng.integers(len(REASON_TEXT[reason]))]
                        fit = reason in ("too_small", "too_big")
                        rtype = "exchange" if rng.random() < (0.45 if fit else 0.2) else "refund"
                        rchan = "store" if channel == "store" else ("mail" if rng.random() < 0.72 else "store")
                        rts = to_ts(rday, int(rng.choice(24, p=STORE_HOUR_W if rchan == "store" else ONLINE_HOUR_W)) * 3600 + int(rng.integers(0, 3600)))
                        rid = "RT" + md5hex(f"{order_id}-{ln}")[:12].upper()
                        refund = round(l["price"] * l["qty"] * (1 - pct), 2)
                        if is_cs3 and reason == "too_small" and rtype == "refund" and rng.random() < 0.35:
                            churn_after = rday + 7
                    line_disc = round(l["price"] * l["qty"] * pct, 2)
                    rows.append((p.person_idx, order_id, channel, store_id, ctry, CURRENCY[ctry], int(day), ts, kind, ln,
                                 l["sku"], str(C["style_id"][j]), str(C["cat_code"][j]), str(div), l["qty"], l["price"],
                                 line_disc, round(l["price"] * l["qty"] - line_disc, 2),
                                 round(float(C["unit_cost"][j]) * (1 if ctry == "US" else K.FX[ctry]), 2), l["md"],
                                 subtotal, discount, ship_fee, tax, total, pay, promo, fulfil, pickup, ship_addr, dev,
                                 utm_s, utm_m, utm_c, sess, ecom_cid, guest, pos_cid, card, earn, reg, assoc, l["gift"],
                                 bool(returned), rid, rday, rts, reason, rtext, rtype, rchan, refund))
                if churn_after is not None:
                    hi = min(hi, churn_after)
        yield _frame(rows)


def gen_anon(batches, ctx):
    """Store transactions with no customer identifier (no loyalty scan / no profile)."""
    C, cw = ctx["C"], ctx["cw"]
    countries = ["US", "UK", "DE", "CA"]
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 11 + int(b["id"].iloc[0]))
        ob = OrderBuilder(ctx, rng)
        rows = []
        days = sample_days(rng, cw, 0, N_DAYS - 1, len(b))
        for i, oid in enumerate(b["id"].to_numpy()):
            ctry = countries[rng.choice(4, p=[.70, .12, .10, .08])]
            ids, w, _ = ctx["stores"][ctry]
            store_id = str(ids[rng.choice(len(ids), p=w)])
            outlet = store_id in ctx["outlets"]
            day = int(days[i])
            gg = "F" if rng.random() < 0.48 else "M"
            shoe = rng.normal(8, 1.05) if gg == "F" else rng.normal(10, 1.25)
            n = int(rng.choice([1, 2, 3], p=[.6, .28, .12]))
            lines = []
            for _ in range(n):
                u = rng.random()
                cat = K.FW_CATS[rng.choice(6, p=K.FW_W)] if u < 0.55 else ("APP" if u < 0.85 else "ACC")
                j = K.sample_style(C, rng, cat, gg, day)
                if j >= 0:
                    lines.append(ob.line(j, gg, shoe, str(rng.choice(["S", "M", "L"])), day, ctry, outlet))
            if not lines:
                continue
            ts = to_ts(day, int(rng.choice(24, p=STORE_HOUR_W)) * 3600 + int(rng.integers(0, 3600)))
            order_id = f"TX{perm(400_000_000 + int(oid), 7_919_011, 104_729, 10**12):012d}"
            subtotal = round(sum(l["price"] * l["qty"] for l in lines), 2)
            tax = round(subtotal * TAX[ctry], 2)
            pays, pw = PAY[ctry]
            pay = "cash" if rng.random() < 0.22 else str(rng.choice(pays, p=pw))
            reg = f"{int(rng.integers(1, 9)):02d}"
            assoc = f"A{int(md5hex(store_id + str(rng.integers(0, 40)))[:5], 16) % 100000:05d}"
            for ln, l in enumerate(lines, start=1):
                j = l["j"]
                returned = rng.random() < 0.05 and day < N_DAYS - 22
                rday = day + int(rng.integers(1, 22)) if returned else None
                reason = REASONS[rng.choice(7, p=R_BASE)] if returned else None
                rows.append((None, order_id, "store", store_id, ctry, CURRENCY[ctry], day, ts, "anonymous", ln, l["sku"],
                             str(C["style_id"][j]), str(C["cat_code"][j]), str(C["division"][j]), l["qty"], l["price"], 0.0,
                             l["price"] * l["qty"], round(float(C["unit_cost"][j]), 2), l["md"], subtotal, 0.0, 0.0, tax,
                             round(subtotal + tax, 2), pay, None, "in_store", None, None, None, None, None, None, None,
                             None, None, None, None, False, reg, assoc, False, bool(returned),
                             ("RT" + md5hex(f"{order_id}-{ln}")[:12].upper()) if returned else None, rday,
                             to_ts(rday, 50000) if returned else None, reason,
                             REASON_TEXT[reason][0] if returned else None, "refund" if returned else None,
                             "store" if returned else None, l["price"] if returned else None))
        yield _frame(rows)


def _frame(rows):
    df = pd.DataFrame(rows, columns=COLS)
    for c in ("order_ts", "return_ts"):
        df[c] = pd.to_datetime(df[c])
    return df


RC_POS = {"too_small": "RC01", "too_big": "RC02", "comfort": "RC03", "defect": "RC04", "not_as_pictured": "RC05",
          "changed_mind": "RC06", "other": "RC99"}


def run():
    from pyspark.sql import functions as F
    spark = get_spark()
    ctx = _ctx()
    persons = truth("persons").where("n_orders > 0 OR is_fitness OR is_sneakerhead OR is_reseller")
    P = persons.repartition(128, "person_idx").sortWithinPartitions("person_idx")
    print("commerce: generating identified orders", flush=True)
    ident = P.mapInPandas(lambda it: gen_orders(it, ctx), SCHEMA)
    save_truth(ident, "order_lines")
    print("commerce: generating anonymous store transactions", flush=True)
    anon = spark.range(0, N_ANON_POS, numPartitions=max(8, N_ANON_POS // 40000)).mapInPandas(lambda it: gen_anon(it, ctx), SCHEMA)
    anon.write.mode("append").saveAsTable(f"{TRUTH}.order_lines")
    L = truth("order_lines")
    print(f"  truth.order_lines {L.count():,}", flush=True)

    on = L.where("channel <> 'store'")
    st = L.where("channel = 'store'")
    iso = lambda c: F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    # ---- e-commerce
    hdr = (on.groupBy("order_id").agg(
        F.first("ecom_customer_id").alias("ecom_customer_id"), F.first("guest_email").alias("guest_email"),
        F.first("order_ts").alias("_event_ts"), F.first("order_day").alias("order_day"),
        F.first("fulfillment_type").alias("fulfillment_type"),
        F.sum(F.col("returned").cast("int")).alias("n_ret"), F.count("*").alias("n_lines"),
        *[F.first(c).alias(c) for c in ["subtotal", "discount_amount", "shipping_amount", "tax_amount", "total_amount",
                                         "currency", "payment_type", "promo_code", "pickup_store_id", "shipping_address",
                                         "device_type", "utm_source", "utm_medium", "utm_campaign", "session_id"]]))
    hdr = (hdr.withColumn("order_ts", iso(F.col("_event_ts")))
              .withColumn("order_status", F.when(F.col("order_day") >= N_DAYS - 2, F.lit("processing"))
                          .when(F.col("order_day") >= N_DAYS - 5, F.lit("shipped"))
                          .when(F.col("n_ret") == F.col("n_lines"), F.lit("returned"))
                          .when(F.col("n_ret") > 0, F.lit("partially_returned"))
                          .when(F.col("fulfillment_type") == "bopis", F.lit("picked_up")).otherwise(F.lit("delivered")))
              .withColumnRenamed("subtotal", "subtotal_amount"))
    write_bronze(hdr, "ecom_orders_raw", "salesforce_commerce_cloud", "sfcc/orders")
    ol = on.select("order_id", "line_number", "sku", "quantity", "unit_price", F.col("line_discount").alias("discount_amount"),
                   "line_total", "is_markdown", F.col("order_ts").alias("_event_ts"))
    write_bronze(ol, "ecom_order_lines_raw", "salesforce_commerce_cloud", "sfcc/order_lines")
    er = (on.where("returned").select(
        "return_id", "order_id", "line_number", "sku", "quantity", F.col("refund_amount"),
        F.upper("return_reason").alias("return_reason_code"), "return_reason_text", "return_type", "return_channel",
        iso(F.col("return_ts")).alias("requested_ts"),
        F.when(F.col("return_day") + 5 < N_DAYS, iso(F.col("return_ts") + F.expr("INTERVAL 5 DAYS"))).alias("received_ts"),
        F.col("return_ts").alias("_event_ts")))
    write_bronze(er, "ecom_returns_raw", "narvar_returns", "narvar/returns")
    # ---- POS
    pos_fmt = F.when(F.col("country") == "US", F.date_format("order_ts", "MM/dd/yyyy HH:mm:ss")) \
        .otherwise(F.date_format("order_ts", "dd/MM/yyyy HH:mm:ss"))
    th = (st.where("line_number = 1").select(
        F.col("order_id").alias("transaction_id"), "store_id", "register_id", "associate_id", "pos_customer_id",
        "loyalty_card_number", pos_fmt.alias("transaction_ts"), F.col("subtotal").alias("subtotal_amount"),
        "discount_amount", "tax_amount", "total_amount", "currency", "payment_type", "promo_code",
        F.lit("SALE").alias("transaction_type"), F.col("order_ts").alias("_event_ts")))
    write_bronze(th, "pos_transactions_raw", "oracle_xstore_pos", "xstore/transactions")
    tl = st.select(F.col("order_id").alias("transaction_id"), "line_number", "sku", "quantity", "unit_price",
                   F.col("line_discount").alias("discount_amount"), "line_total", "is_markdown",
                   F.col("order_ts").alias("_event_ts"))
    write_bronze(tl, "pos_transaction_lines_raw", "oracle_xstore_pos", "xstore/transaction_lines")
    rc = F.create_map(*[x for k, v in RC_POS.items() for x in (F.lit(k), F.lit(v))])
    pr = (st.where("returned").select(
        "return_id", F.col("order_id").alias("original_transaction_id"), "store_id", "sku", "quantity", "refund_amount",
        rc[F.col("return_reason")].alias("return_reason_code"), "return_reason_text", "return_type",
        F.date_format("return_ts", "MM/dd/yyyy HH:mm:ss").alias("return_ts"), F.col("return_ts").alias("_event_ts")))
    write_bronze(pr, "pos_returns_raw", "oracle_xstore_pos", "xstore/returns")
