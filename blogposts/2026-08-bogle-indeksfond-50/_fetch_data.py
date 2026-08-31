"""Fetch the public Pensionikeskus data used in analysis.ipynb and write data/ii_pillar_monthly.csv.

Sources (all public, aggregate):
  * II pillar fund assets, month-end totals  -> /statistika/ii-sammas/kogumispensioni-fondide-maht/ (json=1, f[]=-1)
  * II pillar contributions by month         -> /ws/et/stats/receipt-statistics?period=<id>  (type F = funds, P = PIK)
  * EPI-II index (II samba üldindeks)        -> /statistika/ii-sammas/epi-graafikud/ (json=1, one year per request)

Gotcha: receipt-statistics returns pre-2011 amounts in kroons; converted with 15.6466 EEK/EUR.
Run: .venv/bin/python3 blogposts/2026-08-bogle-indeksfond-50/_fetch_data.py
"""
import calendar
import csv
import datetime as dt
import json
import time
from collections import OrderedDict, defaultdict
from pathlib import Path

import requests

BASE = "https://www.pensionikeskus.ee"
HDR = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
OUT = Path(__file__).parent / "data" / "ii_pillar_monthly.csv"
EEK = 15.6466


def get(url, **params):
    r = requests.get(url, params=params, headers=HDR, timeout=60)
    r.raise_for_status()
    return r.json()


def month_ends(start=(2002, 7), end=None):
    y, m = start
    end = end or (dt.date.today().year, dt.date.today().month)
    while (y, m) <= end:
        yield y, m
        m += 1
        if m == 13:
            y, m = y + 1, 1


def fund_assets():
    out = {}
    for y, m in month_ends():
        last = dt.date(y, m, calendar.monthrange(y, m)[1])
        if last > dt.date.today():
            last = dt.date.today() - dt.timedelta(days=1)
        d = get(f"{BASE}/statistika/ii-sammas/kogumispensioni-fondide-maht/",
                date_from=(last - dt.timedelta(days=6)).isoformat(), date_to=last.isoformat(), json=1, **{"f[]": -1})
        out[f"{y}-{m:02d}"] = sum((c.get("end_val") or 0) for c in d["data"]["charts"])
        time.sleep(0.1)
    return out


def contributions():
    d = get(f"{BASE}/ws/et/stats/receipt-statistics")
    out = {}
    for p in d["data"]["periods"]:
        r = get(f"{BASE}/ws/et/stats/receipt-statistics", period=p["id"])
        by = defaultdict(float)
        for row in r["data"]["stats"]:
            by[row.get("type")] += row.get("amount") or 0
        k = EEK if int(p["start_date"][:4]) < 2011 else 1.0
        out[p["start_date"][:7]] = (by.get("F", 0) / k, by.get("P", 0) / k)
        time.sleep(0.1)
    return out


def epi_ii():
    vals = {}
    for y in range(2002, dt.date.today().year + 1):
        a = "2002-07-01" if y == 2002 else f"{y}-01-01"
        b = f"{y}-12-31"
        d = get(f"{BASE}/statistika/ii-sammas/epi-graafikud/", date_from=a, date_to=b, json=1)
        ch = [c for c in d["data"]["charts"] if c.get("shortname") == "EPI-II"][0]
        for ts, v, _ in ch["data"]:
            day = dt.datetime.fromtimestamp(ts / 1000, dt.timezone.utc).date()
            vals[day] = ch["start_val"] * v / 100.0
        time.sleep(0.1)
    me = OrderedDict()
    for day in sorted(vals):
        me[f"{day.year}-{day.month:02d}"] = (day.isoformat(), vals[day])
    return me


if __name__ == "__main__":
    A, C, E = fund_assets(), contributions(), epi_ii()
    months = sorted(set(A) | set(E))
    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["month", "fund_assets_eur", "contributions_funds_eur", "contributions_pik_eur", "epi_ii", "epi_ii_date"])
        for mth in months:
            c = C.get(mth)
            e = E.get(mth)
            w.writerow([mth, round(A.get(mth, 0), 2), round(c[0], 2) if c else "", round(c[1], 2) if c else "",
                        round(e[1], 6) if e else "", e[0] if e else ""])
    print("wrote", OUT, len(months), "rows")
