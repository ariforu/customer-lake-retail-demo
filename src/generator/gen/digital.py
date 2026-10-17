"""Web clickstream + app events for the analytics retention window (last ~6 months).

Converting sessions are built from the actual web/app orders (same session_id); non-converting known
sessions follow each person's browse propensity; anonymous cookie-only traffic fills the funnel to ~3% CVR."""
from datetime import timedelta

import numpy as np
import pandas as pd

from gen import catalog as K
from gen import refdata as R
from gen.config import (TRUTH, N_ANON_SESSIONS, N_DAYS, ONLINE_HOUR_W, SEED, START, WEB_START, app_user_id, calendar_weights,
                        cookie_id, cum_weights, device_id, di, get_spark, md5hex, sample_days, save_truth, to_ts,
                        truth, write_bronze)
from gen.profiles import app_devices

W0 = di(WEB_START)
SCHEMA = """platform string, event_id string, session_id string, cookie_id string, device_id string,
app_user_id string, ecom_customer_id string, person_idx long, event_type string, event_ts timestamp,
page_url string, page_type string, referrer_url string, sku string, search_term string, selected_size string,
utm_source string, utm_medium string, utm_campaign string, utm_content string, device_type string, browser string,
geo_city string, geo_region string, geo_postal string, cart_id string, quantity int, unit_price double,
screen_name string, app_version string, geo_lat double, geo_lon double, properties string"""
COLS = [c.split()[0] for c in SCHEMA.replace("\n", " ").split(", ")]
SITE = {"US": "us", "UK": "uk", "DE": "de", "CA": "ca"}
NON_CONV_UTM = [("google", "organic", None), (None, None, None), ("meta", "paid_social", "META"),
                ("google", "cpc", "GOOG"), ("email", "email", "NL"), ("tiktok", "paid_social", "TIKT"),
                ("instagram", "social", None), ("affiliate", "affiliate", None)]
NON_CONV_W = np.array([.30, .26, .12, .09, .10, .05, .05, .03])
COOKIE_DEV = [("mobile", "Safari Mobile"), ("desktop", "Chrome"), ("mobile", "Chrome Mobile"), ("tablet", "Safari")]


def slug(s):
    return "".join(ch if ch.isalnum() else "-" for ch in s.lower()).strip("-").replace("--", "-")


class Emitter:
    def __init__(self, ctx, rng):
        self.ctx, self.C, self.rng, self.rows = ctx, ctx["C"], rng, []

    def sku_for(self, P, day):
        C, rng = self.C, self.rng
        u = rng.random()
        cat = (K.footwear_cat(rng, P["sport"]) if u < 0.6 else K.footwear_cat(rng, P["sport2"]) if u < 0.72
               else "APP" if u < 0.92 else "ACC")
        j = K.sample_style(C, rng, cat, P["gg"], day, K.APP_SPORT.get(P["sport"], "Other") if cat == "APP" else None)
        if j < 0:
            return None
        lab = K.size_label(C, j, rng, P["gender"], P["shoe"], P["asize"])
        cw = K.colorway(C, rng, j)
        price, _ = K.price_at(C, j, day, P["country"], False, rng)
        return j, K.sku_of(C, j, cw, lab), lab, price

    def emit(self, base, t, etype, **kw):
        r = dict(base)
        r.update(event_id=md5hex(f"{base['session_id']}-{len(self.rows)}-{etype}")[:24], event_type=etype,
                 event_ts=to_ts(t[0], t[1]))
        r.update(kw)
        self.rows.append(r)
        t[1] += int(self.rng.exponential(35)) + 3

    def session(self, P, platform, day, sec, sid, ids, utm, order=None):
        """ids: dict(cookie_id, device_id, app_user_id, ecom_customer_id, device_type, browser, app_version)"""
        C, rng, ctx = self.C, self.rng, self.ctx
        site = f"https://www.strideforge.com/{SITE[P['country']]}"
        base = dict(platform=platform, session_id=sid, person_idx=P.get("idx"), utm_source=utm[0], utm_medium=utm[1],
                    utm_campaign=utm[2], utm_content=None, geo_city=P["city"], geo_region=P["state"],
                    geo_postal=P["postal"] if rng.random() < 0.85 else None, geo_lat=P["lat"], geo_lon=P["lon"],
                    **ids)
        t = [day, sec]
        web = platform == "web"
        if web:
            lp = rng.choice(["home", "category", "product", "search"], p=[.4, .25, .25, .1]) if order is None else "home"
            ref = {"google": "https://www.google.com/", "meta": "https://l.facebook.com/", "tiktok": "https://www.tiktok.com/",
                   "instagram": "https://l.instagram.com/", "email": None, "affiliate": "https://www.retailmenot.com/"}.get(utm[0])
            url = {"home": f"{site}/", "category": f"{site}/c/{slug(P['sport'])}-shoes", "product": f"{site}/p/new-arrivals",
                   "search": f"{site}/search"}[lp]
            self.emit(base, t, "page_view", page_url=url, page_type=lp, referrer_url=ref)
        else:
            if utm[1] == "push":
                self.emit(base, t, "push_open", screen_name="home", properties='{"campaign":"%s"}' % utm[2])
            self.emit(base, t, "app_open", screen_name="home")
        cart = None
        if order is not None:
            for sku, price in order["items"]:
                j = ctx["sid"].get(sku.split("-")[0], 0)
                if web:
                    self.emit(base, t, "product_view", page_url=f"{site}/p/{slug(C['style_name'][j])}/{C['style_id'][j]}.html",
                              page_type="product", sku=sku)
                    self.emit(base, t, "size_select", page_type="product", sku=sku, selected_size=sku.split("-")[2])
                else:
                    self.emit(base, t, "product_view", screen_name="pdp", sku=sku)
                cart = cart or "cart_" + md5hex(sid)[:16]
                self.emit(base, t, "add_to_cart", sku=sku, cart_id=cart, quantity=1, unit_price=price,
                          page_type="product" if web else None, screen_name=None if web else "pdp")
            self.emit(base, t, "checkout_start", cart_id=cart, page_type="checkout" if web else None,
                      screen_name=None if web else "checkout", page_url=f"{site}/checkout" if web else None)
            self.emit(base, t, "purchase", cart_id=cart, page_type="confirmation" if web else None,
                      screen_name=None if web else "order_confirmation",
                      properties='{"order_id":"%s","revenue":%.2f}' % (order["order_id"], order["total"]))
            return
        n_views = int(min(12, rng.geometric(0.38))) if rng.random() > 0.42 else 0
        if rng.random() < 0.2:
            terms = R.SEARCH_TERMS.get(P["sport"], R.SEARCH_TERMS["Apparel"])
            term = terms[rng.integers(len(terms))]
            self.emit(base, t, "search", search_term=term, page_type="search" if web else None,
                      screen_name=None if web else "search", page_url=f"{site}/search?q={term.replace(' ', '+')}" if web else None)
        for _ in range(n_views):
            x = self.sku_for(P, day)
            if x is None:
                continue
            j, sku, lab, price = x
            if web:
                self.emit(base, t, "product_view", page_url=f"{site}/p/{slug(C['style_name'][j])}/{C['style_id'][j]}.html",
                          page_type="product", sku=sku)
            else:
                self.emit(base, t, "product_view", screen_name="pdp", sku=sku)
            if rng.random() < 0.5:
                self.emit(base, t, "size_select", sku=sku, selected_size=lab, page_type="product" if web else None,
                          screen_name=None if web else "pdp")
                oos = ctx["oos"].get(sku)
                if oos and oos[0] <= day <= oos[1] and rng.random() < 0.45:
                    self.emit(base, t, "back_in_stock_signup", sku=sku, selected_size=lab,
                              page_type="product" if web else None, screen_name=None if web else "pdp")
                    continue
            if rng.random() < 0.04:
                self.emit(base, t, "wishlist_add", sku=sku, page_type="product" if web else None,
                          screen_name=None if web else "pdp")
            if rng.random() < 0.11:
                cart = cart or "cart_" + md5hex(sid)[:16]
                self.emit(base, t, "add_to_cart", sku=sku, cart_id=cart, quantity=1, unit_price=price,
                          page_type="product" if web else None, screen_name=None if web else "pdp")
                if rng.random() < 0.1:
                    self.emit(base, t, "remove_from_cart", sku=sku, cart_id=cart, quantity=1, unit_price=price,
                              page_type="cart" if web else None, screen_name=None if web else "cart")
        if cart and rng.random() < 0.35:
            self.emit(base, t, "checkout_start", cart_id=cart, page_type="checkout" if web else None,
                      screen_name=None if web else "checkout", page_url=f"{site}/checkout" if web else None)

    def frame(self):
        df = pd.DataFrame(self.rows, columns=COLS)
        df["event_ts"] = pd.to_datetime(df["event_ts"])
        self.rows = []
        return df


def _pinfo(p):
    return dict(idx=int(p.person_idx), country=p.country, gender=p.gender, gg="F" if p.gender == "F" else "M",
                shoe=p.shoe_size, asize=p.apparel_size, sport=p.primary_sport, sport2=p.secondary_sport, city=p.city,
                state=p.state, postal=p.postal_code, lat=p.latitude, lon=p.longitude)


def gen_known(batches, ctx):
    cw = ctx["cw"]
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 23 + int(b["person_idx"].iloc[0]))
        em = Emitter(ctx, rng)
        for p in b.itertuples():
            P = _pinfo(p)
            active = p.end_day >= W0
            online_cap = p.channel_pref != "store" or p.has_ecom_account
            cookies = [cookie_id(p.person_idx, k) for k in range(p.n_cookies)]
            devs = app_devices(p) if p.is_app_user else []
            ver = f"{4 + N_DAYS // 300}.{(N_DAYS // 30) % 10}.{int(rng.integers(0, 5))}"
            orders = p.orders if isinstance(p.orders, (list, np.ndarray)) else []
            for o in orders:
                d = (pd.Timestamp(o["order_ts"]).date() - START).days
                s0 = pd.Timestamp(o["order_ts"])
                sec = max(0, s0.hour * 3600 + s0.minute * 60 + s0.second - int(rng.integers(120, 1500)))
                items = list(zip(o["skus"], o["prices"]))
                order = dict(order_id=o["order_id"], total=o["total"], items=items)
                if o["channel"] == "web":
                    k = int(rng.integers(len(cookies)))
                    dt, br = COOKIE_DEV[k % 4] if o["device_type"] is None else (o["device_type"], COOKIE_DEV[k % 4][1])
                    ids = dict(cookie_id=cookies[k], device_id=None, app_user_id=None, ecom_customer_id=o["ecom_customer_id"],
                               device_type=dt, browser=br, app_version=None)
                    em.session(P, "web", d, sec, o["session_id"], ids, (o["utm_source"], o["utm_medium"], o["utm_campaign"]), order)
                elif devs:
                    dv = devs[0]
                    ids = dict(cookie_id=None, device_id=device_id(p.person_idx, dv[0]), app_user_id=app_user_id(p.person_idx),
                               ecom_customer_id=o["ecom_customer_id"], device_type=dv[3], browser=None, app_version=ver)
                    em.session(P, dv[3], d, sec, o["session_id"], ids, (o["utm_source"], o["utm_medium"], o["utm_campaign"]), order)
            f = 1.0 if active else 0.12
            n_web = rng.poisson(p.browse_rate * 6 * f * (1.0 if online_cap else 0.3))
            if n_web:
                days = sample_days(rng, cw, W0, N_DAYS - 1, n_web)
                for s, d in enumerate(days):
                    k = int(rng.integers(len(cookies)))
                    logged = p.has_ecom_account and rng.random() < 0.45
                    u = NON_CONV_UTM[rng.choice(len(NON_CONV_UTM), p=NON_CONV_W)]
                    if u[0] == "email" and not p.email_optin:
                        u = NON_CONV_UTM[1]
                    camp = None
                    if u[2] == "NL":
                        dt = START + timedelta(days=int(d))
                        camp = f"EM-{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}-NL"
                    elif u[2]:
                        camp = f"{u[2][:2]}-{p.country}-PROSPECTING"
                    ids = dict(cookie_id=cookies[k], device_id=None, app_user_id=None,
                               ecom_customer_id=ctx["ecom_id"](p.person_idx) if logged else None,
                               device_type=COOKIE_DEV[k % 4][0], browser=COOKIE_DEV[k % 4][1], app_version=None)
                    sec = int(rng.choice(24, p=ONLINE_HOUR_W)) * 3600 + int(rng.integers(0, 3600))
                    em.session(P, "web", int(d), sec, "s_" + md5hex(f"{p.person_idx}-w{s}")[:24], ids, (u[0], u[1], camp))
            if devs and active:
                n_app = rng.poisson(p.app_rate * 8)
                days = sample_days(rng, cw, W0, N_DAYS - 1, n_app) if n_app else []
                for s, d in enumerate(days):
                    dv = devs[int(rng.integers(len(devs)))]
                    if dv[1] > d:
                        continue
                    ids = dict(cookie_id=None, device_id=device_id(p.person_idx, dv[0]), app_user_id=app_user_id(p.person_idx),
                               ecom_customer_id=ctx["ecom_id"](p.person_idx) if p.has_ecom_account else None,
                               device_type=dv[3], browser=None, app_version=ver)
                    push = p.push_enabled and rng.random() < 0.18
                    dt = START + timedelta(days=int(d))
                    utm = ("braze", "push", f"PUSH-{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}") if push else (None, None, None)
                    sec = int(rng.choice(24, p=ONLINE_HOUR_W)) * 3600 + int(rng.integers(0, 3600))
                    em.session(P, dv[3], int(d), sec, "a_" + md5hex(f"{p.person_idx}-a{s}")[:24], ids, utm)
            if len(em.rows) > 200_000:
                yield em.frame()
        if em.rows:
            yield em.frame()


def gen_anon_sessions(batches, ctx):
    cw, geo = ctx["cw"], ctx["geo"]
    for b in batches:
        if b.empty:
            continue
        rng = np.random.default_rng(SEED + 29 + int(b["id"].iloc[0]))
        em = Emitter(ctx, rng)
        days = sample_days(rng, cw, W0, N_DAYS - 1, len(b))
        gi = rng.choice(len(geo["postal"]), len(b), p=geo["w"])
        for i, sid in enumerate(b["id"].to_numpy()):
            g = int(gi[i])
            gender = "F" if rng.random() < 0.48 else "M"
            P = dict(idx=None, country=geo["country"][g], gender=gender, gg=gender, shoe=10.0 if gender == "M" else 8.0,
                     asize="M", sport=["Running", "Lifestyle", "Training", "Basketball", "Football"][rng.choice(5, p=[.3, .35, .15, .1, .1])],
                     sport2="Lifestyle", city=geo["city"][g], state=geo["state"][g], postal=geo["postal"][g],
                     lat=float(geo["lat"][g]), lon=float(geo["lon"][g]))
            k = int(rng.integers(4))
            ck = "ck_" + md5hex(f"anon-{int(rng.integers(0, max(1000, int(len(b) * 60))))}-{int(b['id'].iloc[0]) // 100000}")[:20]
            u = NON_CONV_UTM[rng.choice(len(NON_CONV_UTM), p=NON_CONV_W)]
            if u[0] == "email":
                u = NON_CONV_UTM[0]
            ids = dict(cookie_id=ck, device_id=None, app_user_id=None, ecom_customer_id=None,
                       device_type=COOKIE_DEV[k][0], browser=COOKIE_DEV[k][1], app_version=None)
            sec = int(rng.choice(24, p=ONLINE_HOUR_W)) * 3600 + int(rng.integers(0, 3600))
            em.session(P, "web", int(days[i]), sec, "s_" + md5hex(f"anon-{int(sid)}")[:24], ids,
                       (u[0], u[1], f"{u[2][:2]}-{P['country']}-PROSPECTING" if u[2] else None))
            if len(em.rows) > 200_000:
                yield em.frame()
        if em.rows:
            yield em.frame()


def _ctx():
    from gen.config import ecom_id
    C = K.build_catalog()
    oos = truth("oos_skus").toPandas()
    geo = truth("geo").toPandas()
    w = geo.pop_weight.to_numpy()
    return dict(C=C, sid={s: i for i, s in enumerate(C["style_id"])},
                oos={r.sku: (int(r.oos_start_day), int(r.oos_end_day)) for r in oos.itertuples()},
                cw=cum_weights(calendar_weights()), ecom_id=ecom_id,
                geo=dict(postal=geo.postal_code.to_numpy(), country=geo.country.to_numpy(), city=geo.city.to_numpy(),
                         state=geo.state.to_numpy(), lat=geo.latitude.to_numpy(), lon=geo.longitude.to_numpy(), w=w / w.sum()))


def run():
    from pyspark.sql import functions as F
    spark = get_spark()
    ctx = _ctx()
    L = truth("order_lines").where(f"channel IN ('web','app') AND order_day >= {W0} AND person_idx IS NOT NULL")
    orders = (L.groupBy("person_idx", "order_id").agg(
        F.first("order_ts").alias("order_ts"), F.first("channel").alias("channel"),
        F.first("session_id").alias("session_id"), F.first("total_amount").alias("total"),
        F.first("device_type").alias("device_type"), F.first("ecom_customer_id").alias("ecom_customer_id"),
        F.first("utm_source").alias("utm_source"), F.first("utm_medium").alias("utm_medium"),
        F.first("utm_campaign").alias("utm_campaign"),
        F.collect_list("sku").alias("skus"), F.collect_list("unit_price").alias("prices"))
        .groupBy("person_idx").agg(F.collect_list(F.struct(
            "order_id", "order_ts", "channel", "session_id", "total", "device_type", "ecom_customer_id",
            "utm_source", "utm_medium", "utm_campaign", "skus", "prices")).alias("orders")))
    persons = truth("persons").where(f"NOT is_prospect OR acq_day >= {W0 - 90}").join(orders, "person_idx", "left")
    P = persons.repartition(192, "person_idx").sortWithinPartitions("person_idx")
    print("digital: generating known-visitor sessions", flush=True)
    save_truth(P.mapInPandas(lambda it: gen_known(it, ctx), SCHEMA), "digital_events")
    print("digital: generating anonymous sessions", flush=True)
    anon = spark.range(0, N_ANON_SESSIONS, numPartitions=max(8, N_ANON_SESSIONS // 50000)) \
        .mapInPandas(lambda it: gen_anon_sessions(it, ctx), SCHEMA)
    anon.write.mode("append").saveAsTable(f"{TRUTH}.digital_events")
    E = truth("digital_events")
    iso = lambda c: F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    web = E.where("platform = 'web'")
    write_bronze(web.select("event_id", "cookie_id", "session_id", "ecom_customer_id", "event_type",
                            iso(F.col("event_ts")).alias("event_ts"), "page_url", "page_type", "referrer_url", "sku",
                            "search_term", "selected_size", "utm_source", "utm_medium", "utm_campaign", "utm_content",
                            "device_type", "browser", F.col("geo_city").alias("ip_geo_city"),
                            F.col("geo_region").alias("ip_geo_region"), F.col("geo_postal").alias("ip_geo_postal_code"),
                            F.col("event_ts").alias("_event_ts")),
                 "web_clickstream_raw", "segment_web", "segment/web/tracks")
    sess = (web.groupBy("session_id").agg(
        F.first("cookie_id").alias("cookie_id"), F.max("ecom_customer_id").alias("ecom_customer_id"),
        F.min("event_ts").alias("_event_ts"), F.max("event_ts").alias("_end"),
        F.min_by("page_url", "event_ts").alias("landing_page"), F.max_by("page_url", "event_ts").alias("exit_page"),
        F.sum(F.col("event_type").isin("page_view", "product_view").cast("int")).alias("page_views"),
        F.first("device_type").alias("device_type"), F.first("utm_source").alias("utm_source"),
        F.first("utm_campaign").alias("utm_campaign"))
        .select("session_id", "cookie_id", "ecom_customer_id", iso(F.col("_event_ts")).alias("session_start_ts"),
                iso(F.col("_end")).alias("session_end_ts"), "landing_page", "exit_page", "page_views", "device_type",
                F.coalesce("utm_source", F.lit("direct")).alias("traffic_source"), "utm_campaign", "_event_ts"))
    write_bronze(sess, "web_sessions_raw", "segment_web", "segment/web/sessions")
    carts = (E.where("event_type IN ('add_to_cart','remove_from_cart')")
             .select("cart_id", "session_id", "ecom_customer_id", F.coalesce("cookie_id", "device_id").alias("cookie_id"),
                     "sku", "quantity", "unit_price",
                     F.when(F.col("event_type") == "add_to_cart", "add").otherwise("remove").alias("action"),
                     iso(F.col("event_ts")).alias("event_ts"), F.col("event_ts").alias("_event_ts")))
    write_bronze(carts, "ecom_carts_raw", "salesforce_commerce_cloud", "sfcc/carts")
    app = E.where("platform <> 'web'")
    props = F.coalesce("properties", F.to_json(F.struct(
        F.col("utm_source").alias("source"), F.col("utm_campaign").alias("campaign"), F.col("cart_id"),
        F.col("unit_price").alias("price"), F.col("search_term"), F.col("selected_size").alias("size"))))
    write_bronze(app.select("event_id", "app_user_id", "device_id", "session_id", F.col("event_type").alias("event_name"),
                            iso(F.col("event_ts")).alias("event_ts"), "screen_name", "sku", props.alias("properties"),
                            "platform", "geo_lat", "geo_lon", F.col("event_ts").alias("_event_ts")),
                 "app_events_raw", "segment_mobile", "segment/mobile/tracks")
