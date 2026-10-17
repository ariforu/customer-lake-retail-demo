"""Source-system formatting quirks and data-quality noise for bronze records."""
from datetime import date, datetime, timedelta

import numpy as np

from gen import refdata as R
from gen.config import START


def day_to_date(d):
    return START + timedelta(days=int(d))


def email_variant(rng, email, p_case=0.1, p_typo=0.02, p_space=0.02):
    if email is None:
        return None
    local, dom = email.split("@", 1)
    u = rng.random()
    if u < p_typo and dom in R.DOMAIN_TYPOS:
        dom = R.DOMAIN_TYPOS[dom]
    elif u < p_typo + p_case:
        local = ".".join(x.capitalize() for x in local.split("."))
        dom = dom.capitalize()
    out = f"{local}@{dom}"
    if rng.random() < p_space:
        out = out + " "
    return out


def alias_email(rng, email):
    """Same mailbox, different string (gmail dots / plus addressing / upper-case)."""
    local, dom = email.split("@", 1)
    u = rng.random()
    if dom == "gmail.com" and u < 0.4:
        return f"{local.replace('.', '')}@{dom}"
    if u < 0.7:
        return f"{local}+strideforge@{dom}"
    return email.upper()


def phone_format(rng, e164, style):
    if e164 is None:
        return None
    if e164.startswith("+1"):
        d = e164[2:]
        return {"e164": e164, "national": f"({d[:3]}) {d[3:6]}-{d[6:]}", "dotted": f"{d[:3]}.{d[3:6]}.{d[6:]}",
                "digits": d, "dashed": f"1-{d[:3]}-{d[3:6]}-{d[6:]}"}[style]
    if e164.startswith("+44"):
        d = e164[3:]
        return {"e164": e164, "national": f"0{d[:4]} {d[4:]}", "dotted": f"0{d}", "digits": f"0{d}",
                "dashed": f"+44 {d[:4]} {d[4:]}"}[style]
    d = e164[3:]
    return {"e164": e164, "national": f"0{d[:3]} {d[3:]}", "dotted": f"0{d[:3]}/{d[3:]}", "digits": f"0{d}",
            "dashed": f"+49 {d[:3]} {d[3:]}"}[style]


def name_variant(rng, first, last, p_upper=0.0, p_nick=0.05, p_typo=0.02, p_initial=0.0):
    u = rng.random()
    if u < p_nick and first in R.NICKNAMES:
        first = R.NICKNAMES[first]
    elif u < p_nick + p_typo and len(last) > 3:
        i = int(rng.integers(1, len(last) - 1))
        last = last[:i] + last[i + 1] + last[i] + last[i + 2:]
    elif u < p_nick + p_typo + p_initial:
        first = first[0] + "."
    if rng.random() < p_upper:
        first, last = first.upper(), last.upper()
    return first, last


def street_variant(rng, street, p=0.5):
    if street is None:
        return None
    if rng.random() < p:
        for a, b in R.STREET_ABBR:
            if a in street:
                street = street.replace(a, b)
                break
    if rng.random() < 0.08:
        street = street.upper()
    return street


def fmt_date(d, style):
    if d is None:
        return None
    return {"iso": d.strftime("%Y-%m-%d"), "us": d.strftime("%m/%d/%Y"), "eu": d.strftime("%d/%m/%Y"),
            "de": d.strftime("%d.%m.%Y")}[style]


def date_style(country):
    return {"US": "us", "CA": "iso", "UK": "eu", "DE": "de"}[country]


def fmt_ts(ts, style):
    """ts: numpy datetime64 or datetime."""
    if ts is None:
        return None
    t = ts if isinstance(ts, datetime) else ts.astype("datetime64[s]").astype(datetime)
    return {"iso_z": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "iso_ms": t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z",
            "pos_us": t.strftime("%m/%d/%Y %H:%M:%S"), "pos_eu": t.strftime("%d/%m/%Y %H:%M:%S"),
            "space": t.strftime("%Y-%m-%d %H:%M:%S"), "epoch": str(int(t.timestamp()))}[style]


def ts_of(day, sec):
    return datetime.combine(day_to_date(day), datetime.min.time()) + timedelta(seconds=int(sec))
