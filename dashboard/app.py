"""
FastAPI backend for Adjust Analytics Dashboard.
Session-based authentication with itsdangerous.
"""

import asyncio
import json as _json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, BackgroundTasks, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv()

from adjust_client.fetcher import fetch_all_data, load_cache, ADJUST_APP_TOKENS, ADJUST_TOKEN
from adjust_client.analyzer import (
    compute_overview, daily_by_app, app_comparison,
    country_breakdown, campaign_breakdown, retention_by_app,
    detect_anomalies,
)

app = FastAPI(title="Adjust Analytics Dashboard")
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

# Auth config
DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "admin")
SESSION_SECRET = os.getenv("SESSION_SECRET", "change-me-in-production")
SESSION_MAX_AGE = 8 * 3600  # 8 hours
COOKIE_NAME = "adjust_session"

serializer = URLSafeTimedSerializer(SESSION_SECRET)


def _create_session(username: str) -> str:
    return serializer.dumps({"user": username, "t": int(time.time())})


def _verify_session(token: str) -> Optional[str]:
    try:
        data = serializer.loads(token, max_age=SESSION_MAX_AGE)
        return data.get("user")
    except (BadSignature, SignatureExpired):
        return None


def _is_authenticated(request: Request) -> bool:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return False
    return _verify_session(token) is not None


def _require_auth(request: Request):
    """Return RedirectResponse if not authenticated, else None."""
    if not _is_authenticated(request):
        return RedirectResponse("/login", status_code=302)
    return None


def _filter_rows(rows, app_filter, start_date=None, end_date=None):
    """Filter rows by app name and date range.
    Rows without a 'day' field skip the date filter (e.g. country/campaign aggregates).
    """
    result = rows
    if app_filter:
        f = app_filter.lower()
        result = [r for r in result if f in r.get("app", "").lower()]
    if start_date or end_date:
        filtered = []
        for r in result:
            day = r.get("day", "")
            if not day:
                # Row has no date (aggregate) — always include
                filtered.append(r)
                continue
            if start_date and day < start_date:
                continue
            if end_date and day > end_date:
                continue
            filtered.append(r)
        result = filtered
    return result


# ─── Auth endpoints ───

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if _is_authenticated(request):
        return RedirectResponse("/", status_code=302)
    html_file = TEMPLATES_DIR / "login.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if username == DASHBOARD_USERNAME and password == DASHBOARD_PASSWORD:
        token = _create_session(username)
        response = RedirectResponse("/", status_code=302)
        response.set_cookie(
            COOKIE_NAME, token,
            max_age=SESSION_MAX_AGE,
            httponly=True,
            samesite="lax",
        )
        return response
    return JSONResponse({"error": "Invalid username or password"}, status_code=401)


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie(COOKIE_NAME)
    return response


# ─── Protected pages ───

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redirect = _require_auth(request)
    if redirect:
        return redirect
    html_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/roas", response_class=HTMLResponse)
async def roas_page(request: Request):
    redirect = _require_auth(request)
    if redirect:
        return redirect
    html_file = TEMPLATES_DIR / "roas.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/brazil", response_class=HTMLResponse)
async def brazil_page(request: Request):
    redirect = _require_auth(request)
    if redirect:
        return redirect
    html_file = TEMPLATES_DIR / "brazil.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


# ─── Protected API endpoints ───

@app.get("/api/apps")
async def get_apps(request: Request):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    cached = load_cache()
    apps = cached.get("apps", [])
    if not apps:
        apps = [{"token": t, "name": t} for t in ADJUST_APP_TOKENS]
    return JSONResponse({"apps": apps})


@app.get("/api/overview")
async def get_overview(
    request: Request,
    app_filter: Optional[str] = Query(None, alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    cached = load_cache()
    rows = _filter_rows(cached.get("all_rows", []), app_filter, start, end)
    overview = compute_overview(rows)
    anomalies = detect_anomalies(overview["daily"])
    return JSONResponse({
        **overview,
        "anomalies": anomalies,
        "fetched_at": cached.get("fetched_at"),
        "errors": cached.get("errors", []),
    })


@app.get("/api/trend")
async def get_trend(
    request: Request,
    app_filter: Optional[str] = Query(None, alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    cached = load_cache()
    rows = _filter_rows(cached.get("all_rows", []), app_filter, start, end)
    trend = daily_by_app(rows)
    return JSONResponse({"trend": trend, "fetched_at": cached.get("fetched_at")})


@app.get("/api/performance")
async def get_performance(
    request: Request,
    app_filter: Optional[str] = Query(None, alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    cached = load_cache()
    rows = _filter_rows(cached.get("all_rows", []), app_filter, start, end)
    apps = app_comparison(rows)
    return JSONResponse({"apps": apps, "fetched_at": cached.get("fetched_at")})


@app.get("/api/countries")
async def get_countries(
    request: Request,
    app_filter: Optional[str] = Query(None, alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    if start or end:
        # Date filter active: fetch live from API with day dimension
        s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        e = end or datetime.today().strftime("%Y-%m-%d")
        dp = f"{s}:{e}"
        app_tokens_str = os.getenv("ADJUST_APP_TOKENS", "")
        headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={app_tokens_str}",
            f"date_period={dp}",
            "dimensions=country,app",
            "metrics=installs,clicks,cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
            "sort=-installs", "limit=1000", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return JSONResponse({"error": resp.text[:300]}, status_code=502)
            rows = resp.json().get("rows", [])
        if app_filter:
            f = app_filter.lower()
            rows = [r for r in rows if f in r.get("app", "").lower()]
        countries = country_breakdown(rows)
    else:
        cached = load_cache()
        rows = _filter_rows(cached.get("country_rows", cached.get("all_rows", [])), app_filter)
        countries = country_breakdown(rows)

    return JSONResponse({"countries": countries, "fetched_at": datetime.now(timezone.utc).isoformat()})


@app.get("/api/country_daily")
async def country_daily(
    request: Request,
    country: str = Query(...),
    app_filter: Optional[str] = Query("APL389", alias="app"),
):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    end = datetime.today()
    start = end - timedelta(days=30)
    dp = f"{start.strftime('%Y-%m-%d')}:{end.strftime('%Y-%m-%d')}"
    app_tokens_str = os.getenv("ADJUST_APP_TOKENS", "")
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    q = "&".join([
        f"app_token__in={app_tokens_str}",
        f"date_period={dp}",
        "dimensions=day,country,app",
        "metrics=installs,cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
        "limit=5000",
        "format=json",
    ])
    url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return JSONResponse({"error": resp.text[:500]}, status_code=502)
        rows = resp.json().get("rows", [])

    filtered = []
    for r in rows:
        if r.get("country", "").lower() != country.lower():
            continue
        if app_filter and app_filter.lower() not in r.get("app", "").lower():
            continue
        inst = float(r.get("installs", 0))
        cost = float(r.get("cost", 0))
        rev = float(r.get("revenue", 0))
        rd0 = float(r.get("revenue_total_d0", 0))
        rd1 = float(r.get("revenue_total_d1", 0))
        rd3 = float(r.get("revenue_total_d3", 0))
        rd7 = float(r.get("revenue_total_d7", 0))
        filtered.append({
            "date": r.get("day", ""),
            "app": r.get("app", ""),
            "installs": int(inst),
            "cost": round(cost, 2),
            "revenue": round(rev, 2),
            "rev_d0": round(rd0, 2), "rev_d1": round(rd1, 2),
            "rev_d3": round(rd3, 2), "rev_d7": round(rd7, 2),
            "roas_d0": round(rd0 / cost, 2) if cost > 0 else 0,
            "roas_d1": round(rd1 / cost, 2) if cost > 0 else 0,
            "roas_d3": round(rd3 / cost, 2) if cost > 0 else 0,
            "roas_d7": round(rd7 / cost, 2) if cost > 0 else 0,
            "ltv_d0": round(rd0 / inst, 4) if inst > 0 else 0,
            "ltv_d3": round(rd3 / inst, 4) if inst > 0 else 0,
            "ltv_d7": round(rd7 / inst, 4) if inst > 0 else 0,
            "ecpi": round(cost / inst, 3) if inst > 0 else 0,
        })

    filtered.sort(key=lambda x: x["date"])
    t_inst = sum(r["installs"] for r in filtered)
    t_cost = sum(r["cost"] for r in filtered)
    t_rev = sum(r["revenue"] for r in filtered)
    t_rd0 = sum(r["rev_d0"] for r in filtered)
    t_rd7 = sum(r["rev_d7"] for r in filtered)

    return JSONResponse({
        "country": country, "date_period": dp, "days": filtered,
        "totals": {
            "installs": t_inst, "cost": round(t_cost, 2), "revenue": round(t_rev, 2),
            "rev_d0": round(t_rd0, 2), "rev_d7": round(t_rd7, 2),
            "roas_d0": round(t_rd0 / t_cost, 2) if t_cost > 0 else 0,
            "roas_d7": round(t_rd7 / t_cost, 2) if t_cost > 0 else 0,
            "ltv_d0": round(t_rd0 / t_inst, 4) if t_inst > 0 else 0,
            "ltv_d7": round(t_rd7 / t_inst, 4) if t_inst > 0 else 0,
            "ecpi": round(t_cost / t_inst, 3) if t_inst > 0 else 0,
        },
    })


@app.get("/api/campaigns")
async def get_campaigns(
    request: Request,
    app_filter: Optional[str] = Query(None, alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    if start or end:
        # Date filter active: fetch live from API
        s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        e = end or datetime.today().strftime("%Y-%m-%d")
        dp = f"{s}:{e}"
        app_tokens_str = os.getenv("ADJUST_APP_TOKENS", "")
        headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={app_tokens_str}",
            f"date_period={dp}",
            "dimensions=campaign,app",
            "metrics=installs,clicks,cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
            "sort=-installs", "limit=1000", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return JSONResponse({"error": resp.text[:300]}, status_code=502)
            rows = resp.json().get("rows", [])
        if app_filter:
            f = app_filter.lower()
            rows = [r for r in rows if f in r.get("app", "").lower()]
        campaigns = campaign_breakdown(rows)
    else:
        cached = load_cache()
        rows = _filter_rows(cached.get("campaign_rows", cached.get("all_rows", [])), app_filter)
        campaigns = campaign_breakdown(rows)

    return JSONResponse({"campaigns": campaigns, "fetched_at": datetime.now(timezone.utc).isoformat()})


@app.get("/api/retention")
async def get_retention(request: Request, app_filter: Optional[str] = Query(None, alias="app")):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    cached = load_cache()
    rows = _filter_rows(cached.get("all_rows", []), app_filter)
    retention = retention_by_app(rows)
    return JSONResponse({"retention": retention, "fetched_at": cached.get("fetched_at")})


@app.get("/api/cohort_report")
async def cohort_report(
    request: Request,
    app_filter: Optional[str] = Query("APL389", alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
):
    """Fetch full cohort marketing report from Adjust API."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    e = end or datetime.today().strftime("%Y-%m-%d")
    dp = f"{s}:{e}"
    app_tokens_str = os.getenv("ADJUST_APP_TOKENS", "")
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    cohort_metrics = "installs,daus,cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7,revenue_total_d14,revenue_total_d21,revenue_total_d28,revenue_total_d35,revenue_total_d45,revenue_total_d60,revenue_total_d90"

    async def fetch_cohort(dims):
        q = "&".join([
            f"app_token__in={app_tokens_str}",
            f"date_period={dp}",
            f"dimensions={dims}",
            f"metrics={cohort_metrics}",
            "sort=-installs", "limit=5000", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return []
            return resp.json().get("rows", [])

    # Fetch by day and by country
    daily_rows = await fetch_cohort("day,app")
    country_rows = await fetch_cohort("country,app")

    # Filter by app
    af = (app_filter or "").lower()
    if af:
        daily_rows = [r for r in daily_rows if af in r.get("app", "").lower()]
        country_rows = [r for r in country_rows if af in r.get("app", "").lower()]

    # Filter by country if specified
    if country:
        daily_rows_filtered = []
        # Need day+country query
        dc_rows = await fetch_cohort("day,country,app")
        if af:
            dc_rows = [r for r in dc_rows if af in r.get("app", "").lower()]
        daily_rows = [r for r in dc_rows if r.get("country", "").lower() == country.lower()]

    def parse_row(r, group_key):
        inst = float(r.get("installs", 0))
        daus = float(r.get("daus", 0))
        cost = float(r.get("cost", 0))
        rev = float(r.get("revenue", 0))
        cpi = cost / inst if inst > 0 else 0
        cpu = cost / daus if daus > 0 else 0

        cohorts = {}
        for d in [0, 1, 3, 7, 14, 21, 28, 35, 45, 60, 90]:
            rv = float(r.get(f"revenue_total_d{d}", 0))
            ltv = rv / inst if inst > 0 else 0
            roas_pct = (ltv / cpi * 100) if cpi > 0 else 0
            cohorts[f"ltv_d{d}"] = round(ltv, 4)
            cohorts[f"roas_d{d}"] = round(roas_pct, 2)
            cohorts[f"rev_d{d}"] = round(rv, 2)

        roas_all = (rev / cost * 100) if cost > 0 else 0

        return {
            "group": r.get(group_key, ""),
            "installs": int(inst),
            "cohort_users_d0": int(daus),
            "install_per_user": round(inst / daus, 2) if daus > 0 else 0,
            "cost": round(cost, 2),
            "cpi": round(cpi, 4),
            "cpu": round(cpu, 4),
            "revenue": round(rev, 2),
            "roas_all": round(roas_all, 2),
            **cohorts,
        }

    by_date = [parse_row(r, "day") for r in sorted(daily_rows, key=lambda x: x.get("day", ""))]
    by_country = [parse_row(r, "country") for r in sorted(country_rows, key=lambda x: float(x.get("installs", 0)), reverse=True)]

    # Grand totals
    def calc_totals(rows_list, raw_rows):
        t_inst = sum(float(r.get("installs", 0)) for r in raw_rows)
        t_daus = sum(float(r.get("daus", 0)) for r in raw_rows)
        t_cost = sum(float(r.get("cost", 0)) for r in raw_rows)
        t_rev = sum(float(r.get("revenue", 0)) for r in raw_rows)
        cpi = t_cost / t_inst if t_inst > 0 else 0
        cpu = t_cost / t_daus if t_daus > 0 else 0
        total = {
            "group": "TOTAL",
            "installs": int(t_inst), "cohort_users_d0": int(t_daus),
            "install_per_user": round(t_inst / t_daus, 2) if t_daus > 0 else 0,
            "cost": round(t_cost, 2), "cpi": round(cpi, 4), "cpu": round(cpu, 4),
            "revenue": round(t_rev, 2),
            "roas_all": round(t_rev / t_cost * 100, 2) if t_cost > 0 else 0,
        }
        for d in [0, 1, 3, 7, 14, 21, 28, 35, 45, 60, 90]:
            t_rv = sum(float(r.get(f"revenue_total_d{d}", 0)) for r in raw_rows)
            ltv = t_rv / t_inst if t_inst > 0 else 0
            roas_pct = (ltv / cpi * 100) if cpi > 0 else 0
            total[f"ltv_d{d}"] = round(ltv, 4)
            total[f"roas_d{d}"] = round(roas_pct, 2)
            total[f"rev_d{d}"] = round(t_rv, 2)
        return total

    return JSONResponse({
        "date_period": dp,
        "by_date": by_date,
        "by_country": by_country,
        "totals_date": calc_totals(by_date, daily_rows),
        "totals_country": calc_totals(by_country, country_rows),
    })


@app.get("/api/roas")
async def roas_data(
    request: Request,
    app_filter: Optional[str] = Query("APL389", alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Fetch ROAS data live from Adjust API based on date filter."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    s = start or (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    e = end or datetime.today().strftime("%Y-%m-%d")
    dp = f"{s}:{e}"
    app_tokens_str = os.getenv("ADJUST_APP_TOKENS", "")
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    # Fetch daily data with cohort
    q = "&".join([
        f"app_token__in={app_tokens_str}",
        f"date_period={dp}",
        "dimensions=day,app",
        "metrics=installs,cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
        "sort=-installs", "limit=1000", "format=json",
    ])
    url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return JSONResponse({"error": resp.text[:300]}, status_code=502)
        daily_rows = resp.json().get("rows", [])

    # Fetch country data with cohort
    q2 = "&".join([
        f"app_token__in={app_tokens_str}",
        f"date_period={dp}",
        "dimensions=country,app",
        "metrics=installs,cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
        "sort=-installs", "limit=1000", "format=json",
    ])
    url2 = f"https://dash.adjust.com/control-center/reports-service/report?{q2}"
    async with httpx.AsyncClient(timeout=120) as client:
        resp2 = await client.get(url2, headers=headers)
        country_rows = resp2.json().get("rows", []) if resp2.status_code == 200 else []

    # Filter by app
    af = (app_filter or "").lower()
    if af:
        daily_rows = [r for r in daily_rows if af in r.get("app", "").lower()]
        country_rows = [r for r in country_rows if af in r.get("app", "").lower()]

    # Build daily ROAS
    daily = []
    for r in sorted(daily_rows, key=lambda x: x.get("day", "")):
        cost = float(r.get("cost", 0))
        rd0 = float(r.get("revenue_total_d0", 0))
        rd1 = float(r.get("revenue_total_d1", 0))
        rd3 = float(r.get("revenue_total_d3", 0))
        rd7 = float(r.get("revenue_total_d7", 0))
        daily.append({
            "date": r.get("day", ""),
            "installs": int(float(r.get("installs", 0))),
            "cost": round(cost, 2),
            "revenue": round(float(r.get("revenue", 0)), 2),
            "rev_d0": round(rd0, 2), "rev_d7": round(rd7, 2),
            "roas_d0": round(rd0 / cost, 2) if cost > 0 else 0,
            "roas_d1": round(rd1 / cost, 2) if cost > 0 else 0,
            "roas_d3": round(rd3 / cost, 2) if cost > 0 else 0,
            "roas_d7": round(rd7 / cost, 2) if cost > 0 else 0,
        })

    # Build country ROAS
    by_country = []
    for r in sorted(country_rows, key=lambda x: float(x.get("installs", 0)), reverse=True)[:15]:
        cost = float(r.get("cost", 0))
        rd0 = float(r.get("revenue_total_d0", 0))
        rd7 = float(r.get("revenue_total_d7", 0))
        rev = float(r.get("revenue", 0))
        by_country.append({
            "country": r.get("country", ""),
            "installs": int(float(r.get("installs", 0))),
            "cost": round(cost, 2),
            "revenue": round(rev, 2),
            "roas": round(rd7 / cost, 2) if cost > 0 else 0,
            "roas_d0": round(rd0 / cost, 2) if cost > 0 else 0,
            "roas_d7": round(rd7 / cost, 2) if cost > 0 else 0,
        })

    # Totals
    t_cost = sum(d["cost"] for d in daily)
    t_rev = sum(d["revenue"] for d in daily)
    t_rd0 = sum(d["rev_d0"] for d in daily)
    t_rd7 = sum(d["rev_d7"] for d in daily)

    return JSONResponse({
        "date_period": dp,
        "apl389_daily": daily,
        "apl389_by_country": by_country,
        "all_apps_roas": [],  # not needed in single-app mode
        "totals": {
            "cost": round(t_cost, 2),
            "revenue": round(t_rev, 2),
            "roas_d0": round(t_rd0 / t_cost, 2) if t_cost > 0 else 0,
            "roas_d7": round(t_rd7 / t_cost, 2) if t_cost > 0 else 0,
        },
    })


@app.get("/api/brazil")
async def brazil_analysis(request: Request):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    end = datetime.today()
    start = end - timedelta(days=7)
    dp = f"{start.strftime('%Y-%m-%d')}:{end.strftime('%Y-%m-%d')}"
    app_tokens = os.getenv("ADJUST_APP_TOKENS", "")
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    q = "&".join([
        f"app_token__in={app_tokens}",
        f"date_period={dp}",
        "dimensions=day,app,country",
        "metrics=installs,clicks,cost,revenue,sessions,daus",
        "limit=5000",
        "format=json",
    ])
    url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return JSONResponse({"error": resp.text[:500]}, status_code=502)
        rows = resp.json().get("rows", [])

    br_rows = [r for r in rows if r.get("country") == "Brazil"]
    apps = {}
    for r in br_rows:
        app_name = r.get("app", "?")
        day = r.get("day", "")
        if app_name not in apps:
            apps[app_name] = {"daily": {}, "totals": {
                "installs": 0, "cost": 0.0, "revenue": 0.0,
                "sessions": 0, "daus": 0, "clicks": 0,
            }}
        t = apps[app_name]["totals"]
        t["installs"] += float(r.get("installs", 0))
        t["cost"] += float(r.get("cost", 0))
        t["revenue"] += float(r.get("revenue", 0))
        t["sessions"] += float(r.get("sessions", 0))
        t["daus"] += float(r.get("daus", 0))
        t["clicks"] += float(r.get("clicks", 0))
        if day:
            apps[app_name]["daily"][day] = {
                "installs": float(r.get("installs", 0)),
                "cost": float(r.get("cost", 0)),
                "revenue": float(r.get("revenue", 0)),
            }

    result = []
    for name, data in apps.items():
        t = data["totals"]
        clean_rev = sum(v["revenue"] for v in data["daily"].values() if v["revenue"] < 1_000_000)
        clean_ratio = round(clean_rev / t["cost"], 2) if t["cost"] > 0 else 0
        result.append({
            "app": name, "installs": int(t["installs"]),
            "cost": round(t["cost"], 2), "revenue": round(clean_rev, 2),
            "sessions": int(t["sessions"]), "daus": int(t["daus"]),
            "ecpi": round(t["cost"] / t["installs"], 3) if t["installs"] > 0 else 0,
            "rev_cost_ratio": clean_ratio,
            "daily": dict(sorted(data["daily"].items())),
        })

    result.sort(key=lambda x: x["installs"], reverse=True)
    return JSONResponse({"date_period": dp, "country": "Brazil", "apps": result})


@app.post("/api/refresh")
async def refresh_data(request: Request, background_tasks: BackgroundTasks):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    background_tasks.add_task(_run_refresh)
    return {"status": "started", "message": "Fetching last 30 days from Adjust API..."}


def _run_refresh():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(fetch_all_data())
    finally:
        loop.close()
