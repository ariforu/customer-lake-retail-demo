"""Source-system customer profiles (deliberately fragmented) + consent events + identity answer key."""
from datetime import date, timedelta

import numpy as np
import pandas as pd

from gen import messy as M
from gen.config import (END, N_DAYS, SEED, app_user_id, device_id, ecom_id, fitness_id, get_spark, loyalty_card,
                        loyalty_id, md5hex, pos_id, save_truth, truth, write_bronze)

FAR = 99999
GDPR_DAY = (date(2025, 5, 25) - date(2023, 10, 1)).days


def _rng(b, salt):
    return np.random.default_rng(SEED + salt * 1_000_003 + int(b["person_idx"].iloc[0]))


def _addr(p, use_new):
    if use_new:
        return p.new_street, p.new_postal_code
    return p.street, p.postal_code


# ---------------------------------------------------------------- e-commerce accounts
ECOM_SCHEMA = """ecom_customer_id string, email string, first_name string, last_name string, phone string,
gender string, birth_date string, default_address string, accepts_marketing boolean, account_status string,
created_at string, updated_at string, person_idx long, _event_ts timestamp"""


def gen_ecom(batches):
    for b in batches:
        if b.empty:
            continue
        rng, out = _rng(b, 1), []
        for p in b.itertuples():
            if not p.has_ecom_account:
                continue
            accts = [0] + ([1] if p.ecom_dup else []) + (list(range(2, 5 + p.person_idx % 6)) if p.is_reseller else [])
            for k in accts:
                c_day = max(p.acq_day, -2500) + (0 if k == 0 else int(rng.integers(20, 500)))
                c_day = min(c_day, N_DAYS - 1)
                u_day = min(N_DAYS - 1, c_day + int(rng.exponential(200)))
                street, postal = _addr(p, p.moved_day < u_day)
                if k == 0:
                    email = M.email_variant(rng, p.email, p_case=0.05, p_typo=0.0)
                    first, last = p.first_name, p.last_name
                elif k == 1:
                    email = p.email_alt or M.alias_email(rng, p.email)
                    first, last = M.name_variant(rng, p.first_name, p.last_name, p_nick=0.3)
                else:   # reseller burner accounts
                    email = f"{p.first_name[0].lower()}{rng.choice(['kicks', 'sole', 'drop', 'hype'])}{int(rng.integers(10, 9999))}@{rng.choice(['proton.me', 'gmail.com', 'outlook.com'])}"
                    first, last = M.name_variant(rng, p.first_name, p.last_name, p_nick=0.3, p_typo=0.3)
                    if rng.random() < 0.5:
                        first = str(rng.choice(["Mike", "Chris", "Alex", "Jay", "Sam", "Dee", "Tony", "Kai"]))
                out.append(dict(
                    ecom_customer_id=ecom_id(p.person_idx, k), email=email, first_name=first, last_name=last,
                    phone=M.phone_format(rng, p.phone, "e164") if (rng.random() < 0.65 and k < 2) else None,
                    gender={"F": "female", "M": "male", "X": "non-binary"}[p.gender] if rng.random() < 0.7 else None,
                    birth_date=p.birth_date.strftime("%Y-%m-%d") if rng.random() < 0.45 and k < 2 else None,
                    default_address=f"{M.street_variant(rng, street, 0.2)}, {p.city}, {p.state} {postal}, {p.country}"
                    if rng.random() < 0.85 else None,
                    accepts_marketing=bool(p.email_optin) if k == 0 else bool(rng.random() < 0.3),
                    account_status="active" if rng.random() < 0.97 else "disabled",
                    created_at=M.fmt_ts(M.ts_of(c_day, rng.integers(0, 86400)), "iso_z"),
                    updated_at=M.fmt_ts(M.ts_of(u_day, rng.integers(0, 86400)), "iso_z"),
                    person_idx=p.person_idx, _event_ts=M.ts_of(max(u_day, 0), 3600)))
        yield pd.DataFrame(out, columns=[c.split()[0] for c in ECOM_SCHEMA.replace("\n", " ").split(", ")])


# ---------------------------------------------------------------- POS (clienteling) profiles
POS_SCHEMA = """pos_customer_id string, store_id string, first_name string, last_name string, email string,
phone string, address_line1 string, city string, state string, postal_code string, country string,
birth_date string, loyalty_card_number string, created_at string, updated_at string, person_idx long,
_event_ts timestamp"""


def gen_pos(batches, stores_by_country):
    for b in batches:
        if b.empty:
            continue
        rng, out = _rng(b, 2), []
        for p in b.itertuples():
            if not p.has_pos_profile:
                continue
            for k in ([0, 1] if p.pos_dup else [0]):
                c_day = min(N_DAYS - 1, max(p.acq_day, -2500) + int(rng.exponential(90)) + (int(rng.integers(30, 600)) if k else 0))
                u_day = min(N_DAYS - 1, c_day + int(rng.exponential(150)))
                street, postal = _addr(p, p.moved_day < c_day)
                first, last = M.name_variant(rng, p.first_name, p.last_name, p_upper=0.25, p_nick=0.08 + 0.2 * k,
                                             p_typo=0.04, p_initial=0.03)
                store = p.home_store_id if k == 0 and rng.random() < 0.85 else str(rng.choice(stores_by_country[p.country]))
                ds = M.date_style(p.country)
                out.append(dict(
                    pos_customer_id=pos_id(p.person_idx, k), store_id=store, first_name=first, last_name=last,
                    email=M.email_variant(rng, p.email if (k == 0 or not p.email_alt) else p.email_alt,
                                          p_case=0.15, p_typo=0.04) if rng.random() < 0.55 else None,
                    phone=M.phone_format(rng, p.phone, str(rng.choice(["national", "dotted", "digits", "dashed"])))
                    if rng.random() < 0.85 else None,
                    address_line1=M.street_variant(rng, street, 0.6) if rng.random() < 0.6 else None,
                    city=p.city.upper() if rng.random() < 0.2 else p.city, state=p.state,
                    postal_code=postal if rng.random() < 0.9 else None, country=p.country,
                    birth_date=M.fmt_date(p.birth_date, ds) if rng.random() < 0.2 else None,
                    loyalty_card_number=loyalty_card(p.person_idx) if (p.is_loyalty and p.loyalty_day <= u_day and rng.random() < 0.7) else None,
                    created_at=M.fmt_ts(M.ts_of(c_day, rng.integers(36000, 75600)), "pos_us" if p.country == "US" else "pos_eu"),
                    updated_at=M.fmt_ts(M.ts_of(u_day, rng.integers(36000, 75600)), "pos_us" if p.country == "US" else "pos_eu"),
                    person_idx=p.person_idx, _event_ts=M.ts_of(max(u_day, 0), 79200)))
        yield pd.DataFrame(out, columns=[c.split()[0] for c in POS_SCHEMA.replace("\n", " ").split(", ")])


# ---------------------------------------------------------------- mobile app users + push tokens
APP_SCHEMA = """app_user_id string, device_id string, email string, phone string, first_name string,
last_name string, platform string, app_version string, install_ts string, last_open_ts string,
push_enabled boolean, location_enabled boolean, person_idx long, _event_ts timestamp"""
TOKEN_SCHEMA = """device_id string, app_user_id string, push_token string, platform string, token_status string,
updated_ts string, person_idx long, _event_ts timestamp"""


def app_devices(p):
    """Deterministic per-person device list: (k, install_day, last_open_day, platform)."""
    rng = np.random.default_rng(SEED + 31 * int(p.person_idx))
    rows = []
    for k in range(p.app_devices):
        inst = min(N_DAYS - 1, max(p.app_day, -700) + (int(rng.integers(60, 500)) if k else 0))
        last = min(N_DAYS - 1, max(inst, min(p.end_day, N_DAYS - 1) - int(rng.exponential(20))))
        ios_p = {"US": 0.58, "CA": 0.55, "UK": 0.52, "DE": 0.38}[p.country]
        plat = "ios" if rng.random() < ios_p else "android"
        rows.append((k, inst, last, plat))
    return rows


def gen_app(batches):
    for b in batches:
        if b.empty:
            continue
        rng, out = _rng(b, 3), []
        for p in b.itertuples():
            if not p.is_app_user:
                continue
            for k, inst, last, plat in app_devices(p):
                ver = f"{4 + max(0, last) // 300}.{(max(0, last) // 30) % 10}.{int(rng.integers(0, 5))}"
                out.append(dict(app_user_id=app_user_id(p.person_idx), device_id=device_id(p.person_idx, k),
                                email=M.email_variant(rng, p.email, p_case=0.03, p_typo=0.0),
                                phone=p.phone if rng.random() < 0.3 else None, first_name=p.first_name,
                                last_name=p.last_name if rng.random() < 0.8 else None, platform=plat, app_version=ver,
                                install_ts=M.fmt_ts(M.ts_of(inst, rng.integers(0, 86400)), "iso_ms"),
                                last_open_ts=M.fmt_ts(M.ts_of(last, rng.integers(0, 86400)), "iso_ms"),
                                push_enabled=bool(p.push_enabled), location_enabled=bool(rng.random() < 0.35),
                                person_idx=p.person_idx, _event_ts=M.ts_of(max(last, 0), 80000)))
        yield pd.DataFrame(out, columns=[c.split()[0] for c in APP_SCHEMA.replace("\n", " ").split(", ")])


def n_push_tokens(p, inst, last):
    return 2 if (last - inst > 400 and p.person_idx % 2 == 0) else 1


def push_token(p, k, t, plat):
    tok = md5hex(f"tok-{p.person_idx}-{k}-{t}") + md5hex(f"tok2-{p.person_idx}-{k}-{t}")
    return tok if plat == "ios" else f"f{tok[:21]}:APA91b{tok[21:]}"


def gen_tokens(batches):
    for b in batches:
        if b.empty:
            continue
        rng, out = _rng(b, 5), []
        for p in b.itertuples():
            if not p.is_app_user:
                continue
            for k, inst, last, plat in app_devices(p):
                n_tok = n_push_tokens(p, inst, last)
                for t in range(n_tok):
                    active = t == n_tok - 1 and p.push_enabled
                    upd = inst if t == 0 else inst + (last - inst) // 2
                    out.append(dict(device_id=device_id(p.person_idx, k), app_user_id=app_user_id(p.person_idx),
                                    push_token=push_token(p, k, t, plat),
                                    platform=plat, token_status="active" if active else ("revoked" if t == n_tok - 1 else "expired"),
                                    updated_ts=M.fmt_ts(M.ts_of(upd, 43200), "iso_ms"), person_idx=p.person_idx,
                                    _event_ts=M.ts_of(max(upd, 0), 43300)))
        yield pd.DataFrame(out, columns=[c.split()[0] for c in TOKEN_SCHEMA.replace("\n", " ").split(", ")])


# ---------------------------------------------------------------- consent events
CONSENT_SCHEMA = """consent_event_id string, subject_identifier string, identifier_type string, purpose string,
channel string, status string, jurisdiction string, capture_source string, policy_version string,
event_ts string, person_idx long, _event_ts timestamp"""


def _jur(p):
    if p.country == "DE":
        return "GDPR"
    if p.country == "UK":
        return "UK-GDPR"
    if p.country == "CA":
        return "PIPEDA"
    return "CCPA" if p.state == "CA" else "US-STATE"


def gen_consent(batches):
    for b in batches:
        if b.empty:
            continue
        rng, out = _rng(b, 4), []
        for p in b.itertuples():
            jur, gdpr = _jur(p), p.country in ("UK", "DE")
            src = "ecom_signup" if p.has_ecom_account else "pos_registration" if p.has_pos_profile else "newsletter_popup"
            d0 = max(p.acq_day, -2500)

            def ev(day, ident, itype, purpose, channel, status, source, ver):
                t = M.ts_of(day, rng.integers(0, 86400))
                out.append(dict(consent_event_id="CE-" + md5hex(f"{p.person_idx}-{len(out)}-{day}-{purpose}")[:16].upper(),
                                subject_identifier=ident, identifier_type=itype, purpose=purpose, channel=channel,
                                status=status, jurisdiction=jur, capture_source=source, policy_version=ver,
                                event_ts=M.fmt_ts(t, "iso_z"), person_idx=p.person_idx, _event_ts=t))
            ver0 = "v2.1" if d0 < GDPR_DAY else "v3.0"
            if p.email_optin:
                ev(d0, p.email, "email", "marketing", "email", "opted_in", src, ver0)
                if p.email_optout_day < FAR:
                    ev(p.email_optout_day, p.email, "email", "marketing", "email", "opted_out",
                       "esp_unsubscribe" if rng.random() < 0.8 else "preference_center", ver0)
            elif gdpr or rng.random() < 0.5:
                ev(d0, p.email, "email", "marketing", "email", "declined", src, ver0)
            if p.sms_optin:
                ev(min(N_DAYS - 1, d0 + int(rng.exponential(120))), p.phone, "phone", "marketing", "sms", "opted_in",
                   "sms_keyword_join" if rng.random() < 0.5 else "checkout_checkbox", ver0)
            if p.is_app_user and p.push_enabled:
                ev(min(N_DAYS - 1, max(p.app_day, -700)), device_id(p.person_idx, 0), "device_id", "marketing", "push",
                   "opted_in", "app_os_prompt", ver0)
            if gdpr:
                ev(d0, p.email, "email", "profiling", "all", "opted_in" if p.profiling_optin else "declined", "cookie_banner", ver0)
                ev(d0, p.email, "email", "ad_targeting", "paid_media", "opted_in" if p.paid_optin else "declined", "cookie_banner", ver0)
                if d0 < GDPR_DAY and rng.random() < 0.6:
                    ev(GDPR_DAY + int(rng.integers(0, 120)), p.email, "email", "profiling", "all",
                       "opted_in" if p.profiling_optin else "declined", "policy_reconsent", "v3.0")
            elif p.do_not_sell:
                ev(min(N_DAYS - 1, d0 + int(rng.integers(0, 700))), p.email, "email", "sale_of_data", "paid_media",
                   "opted_out", "do_not_sell_link", ver0)
        yield pd.DataFrame(out, columns=[c.split()[0] for c in CONSENT_SCHEMA.replace("\n", " ").split(", ")])


def run():
    from pyspark.sql import functions as F
    spark = get_spark()
    persons = truth("persons")
    stores = truth("stores").where("store_type IN ('standard','flagship','outlet')").toPandas()
    sbc = {c: g.store_id.tolist() for c, g in stores.groupby("country")}
    P = persons.repartition(64, "person_idx").sortWithinPartitions("person_idx")

    ecom = save_truth(P.mapInPandas(gen_ecom, ECOM_SCHEMA), "src_ecom_customers")
    write_bronze(ecom.drop("person_idx"), "ecom_customers_raw", "salesforce_commerce_cloud", "sfcc/customers")
    pos = save_truth(P.mapInPandas(lambda it: gen_pos(it, sbc), POS_SCHEMA), "src_pos_customers")
    write_bronze(pos.drop("person_idx"), "pos_customers_raw", "oracle_xstore_pos", "xstore/customers")
    app = save_truth(P.mapInPandas(gen_app, APP_SCHEMA), "src_app_users")
    write_bronze(app.drop("person_idx"), "app_users_raw", "strideforge_app_backend", "app/users")
    tok = P.mapInPandas(gen_tokens, TOKEN_SCHEMA)
    write_bronze(tok.drop("person_idx"), "app_push_tokens_raw", "braze", "braze/push_tokens")
    con = P.mapInPandas(gen_consent, CONSENT_SCHEMA)
    write_bronze(con.drop("person_idx"), "consent_events_raw", "onetrust_cmp", "onetrust/consent")

    # identity answer key: every source identifier -> true person
    idk = (ecom.select(F.lit("ecom").alias("source_system"), F.col("ecom_customer_id").alias("source_id"), "person_idx")
           .unionByName(pos.select(F.lit("pos").alias("source_system"), F.col("pos_customer_id").alias("source_id"), "person_idx"))
           .unionByName(app.select(F.lit("app_device").alias("source_system"), F.col("device_id").alias("source_id"), "person_idx"))
           .unionByName(app.select(F.lit("app_user").alias("source_system"), F.col("app_user_id").alias("source_id"), "person_idx").distinct()))
    save_truth(idk, "identity_answer_key_profiles")
