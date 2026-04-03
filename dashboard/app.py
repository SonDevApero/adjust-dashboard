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

# Map app filter name to specific token for fast single-token queries
# All app token mappings
APP_TOKEN_MAP = {
    "apl389": "p9aujhwyqvi8",
    "asm044": "aeupo1b24f0g",
    "apb855": "r1tcow9gnk74",
    "apb518": "y83milcrp2ww",
    "apl868": "im4w2s8bowzk",
    "asm035": "og7ep0ixuzgg",
    "apb508": "yrfr0dt381s0",
    "asm014": "kbupv7xsze2o",
    "apb666": "zrqw3h1f0n40",
    "apl789": "jo3hic4y2g3k",
    "apl469": "47pyi6fniq4g",
    "asm057": "hxylh1wy6mm8",
    "asm069": "f8gfj20m8mio",
}
APP_NAME_MAP = {
    "p9aujhwyqvi8": "APL389 - Photo Video Maker",
    "aeupo1b24f0g": "ASM044 - Birthday Video Maker",
    "r1tcow9gnk74": "APB855 - AI Chat",
    "y83milcrp2ww": "APB518 - AI Face Swap AI Avatar Magic",
    "im4w2s8bowzk": "APL868 - Vidix AI Photo Video",
    "og7ep0ixuzgg": "ASM035 - Photo Video Maker with Music",
    "yrfr0dt381s0": "APB508 - AI Chat",
    "kbupv7xsze2o": "ASM014 - Mica - AI Photo & Video Maker",
    "zrqw3h1f0n40": "APB666 - Picshiner",
    "jo3hic4y2g3k": "APL789 - Snap Tune AI",
    "47pyi6fniq4g": "APL469 - AI Photo Creator",
    "hxylh1wy6mm8": "ASM057 - Micy Prank",
    "f8gfj20m8mio": "ASM069 - FotoPro",
}
DEFAULT_TOKEN = "p9aujhwyqvi8"


async def _resolve_app_token(app_filter):
    """Return the single token for the app. Default: APL389."""
    if not app_filter:
        return DEFAULT_TOKEN
    af = app_filter.lower()
    # Try direct match from map
    for prefix, token in APP_TOKEN_MAP.items():
        if prefix in af:
            return token
    return DEFAULT_TOKEN


@app.get("/api/app_list")
async def app_list(request: Request):
    """Return all available apps with tokens and names."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    apps = [{"token": t, "code": c, "name": APP_NAME_MAP.get(t, c)}
            for c, t in APP_TOKEN_MAP.items()]
    return JSONResponse({"apps": apps, "default": "apl389"})

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

    # Try cache first
    cached = load_cache()
    rows = _filter_rows(cached.get("all_rows", []), app_filter, start, end)

    # If cache empty for this app, fetch live
    if not rows:
        s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        e = end or datetime.today().strftime("%Y-%m-%d")
        dp = f"{s}:{e}"
        tk = await _resolve_app_token(app_filter)
        headers_api = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={tk}", f"date_period={dp}",
            "dimensions=day,app",
            "metrics=installs,clicks,impressions,network_cost,revenue,ecpi_all,sessions,daus,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
            "attribution_source=first", "utc_offset=%2B07:00",
            "sort=-installs", "limit=1000", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=headers_api)
            if resp.status_code == 200:
                rows = resp.json().get("rows", [])

    overview = compute_overview(rows)
    anomalies = detect_anomalies(overview["daily"])
    return JSONResponse({
        **overview,
        "anomalies": anomalies,
        "fetched_at": cached.get("fetched_at") or datetime.now(timezone.utc).isoformat(),
        "errors": [],
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
    if not rows:
        s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        e = end or datetime.today().strftime("%Y-%m-%d")
        dp = f"{s}:{e}"
        tk = await _resolve_app_token(app_filter)
        headers_api = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={tk}", f"date_period={dp}",
            "dimensions=day,app",
            "metrics=installs,network_cost,revenue,revenue_total_d0,revenue_total_d7",
            "attribution_source=first", "utc_offset=%2B07:00",
            "sort=-installs", "limit=1000", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=headers_api)
            if resp.status_code == 200:
                rows = resp.json().get("rows", [])
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
    if not rows:
        s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        e = end or datetime.today().strftime("%Y-%m-%d")
        dp = f"{s}:{e}"
        tk = await _resolve_app_token(app_filter)
        headers_api = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={tk}", f"date_period={dp}",
            "dimensions=day,app",
            "metrics=installs,network_cost,revenue,sessions,daus,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
            "attribution_source=first", "utc_offset=%2B07:00",
            "sort=-installs", "limit=1000", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=headers_api)
            if resp.status_code == 200:
                rows = resp.json().get("rows", [])
    apps = app_comparison(rows)
    return JSONResponse({"apps": apps, "fetched_at": cached.get("fetched_at") or datetime.now(timezone.utc).isoformat()})


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
        # Date filter active: fetch live from API
        s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        e = end or datetime.today().strftime("%Y-%m-%d")
        dp = f"{s}:{e}"
        app_tokens_str = await _resolve_app_token(app_filter)
        headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={app_tokens_str}",
            f"date_period={dp}",
            "dimensions=country,app",
            "metrics=installs,clicks,network_cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
            "attribution_source=first", "utc_offset=%2B07:00", "sort=-installs", "limit=1000", "format=json",
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
    app_tokens_str = await _resolve_app_token(app_filter)
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    q = "&".join([
        f"app_token__in={app_tokens_str}",
        f"date_period={dp}",
        "dimensions=day,country,app",
        "metrics=installs,network_cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
        "attribution_source=first", "utc_offset=%2B07:00",
        "limit=5000", "format=json",
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
        cost = float(r.get("network_cost", r.get("cost", 0)))
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
            "roas_d0": round(t_rd0 / t_cost * 100, 2) if t_cost > 0 else 0,
            "roas_d7": round(t_rd7 / t_cost * 100, 2) if t_cost > 0 else 0,
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
        app_tokens_str = await _resolve_app_token(app_filter)
        headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
        q = "&".join([
            f"app_token__in={app_tokens_str}",
            f"date_period={dp}",
            "dimensions=campaign,app",
            "metrics=installs,clicks,network_cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
            "attribution_source=first", "utc_offset=%2B07:00", "sort=-installs", "limit=1000", "format=json",
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


_cohort_cache = {}  # key -> {ts, data}

@app.get("/api/cohort_report")
async def cohort_report(
    request: Request,
    app_filter: Optional[str] = Query("APL389", alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
):
    """Fetch cohort marketing report. Uses single app token for speed."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    e = end or datetime.today().strftime("%Y-%m-%d")
    dp = f"{s}:{e}"

    # Check cache (5 min TTL)
    cache_key = f"{dp}|{app_filter}|{country}"
    if cache_key in _cohort_cache:
        cached = _cohort_cache[cache_key]
        if time.time() - cached["ts"] < 300:
            return JSONResponse(cached["data"])

    headers_api = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    # Resolve to single token for speed (13 tokens -> 1 token = 10x faster)
    af = (app_filter or "").lower()
    app_tokens_str = await _resolve_app_token(app_filter)

    cohort_m = "installs,daus,network_cost,ecpi_all,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7,revenue_total_d14,revenue_total_d21,revenue_total_d28"

    async def _fetch(dims, limit=1000):
        q = "&".join([
            f"app_token__in={app_tokens_str}",
            f"date_period={dp}",
            f"dimensions={dims}",
            f"metrics={cohort_m}",
            "attribution_source=first", "utc_offset=%2B07:00",
            f"sort=-installs", f"limit={limit}", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=180) as client:
            resp = await client.get(url, headers=headers_api)
            if resp.status_code != 200:
                return {"rows": [], "totals": {}}
            data = resp.json()
            return {"rows": data.get("rows", []), "totals": data.get("totals", {})}

    import asyncio as _aio
    DAYS = [0, 1, 3, 7, 14, 21, 28]

    # Fetch daily and country in parallel
    if country:
        results = await _aio.gather(
            _fetch("day,country,app", 2000),
            _fetch("country,app", 500),
        )
        daily_resp = results[0]
        daily_resp["rows"] = [r for r in daily_resp["rows"]
                              if r.get("country", "").lower() == country.lower()]
        country_resp = results[1]
    else:
        results = await _aio.gather(
            _fetch("day,app", 500),
            _fetch("country,app", 500),
        )
        daily_resp = results[0]
        country_resp = results[1]

    # Filter by app name
    if af:
        daily_resp["rows"] = [r for r in daily_resp["rows"] if af in r.get("app", "").lower()]
        country_resp["rows"] = [r for r in country_resp["rows"] if af in r.get("app", "").lower()]

    def passthrough(r, key):
        """Convert API row. ROAS = revenue_total_dX / network_cost * 100."""
        inst = float(r.get("installs", 0))
        daus = float(r.get("daus", 0))
        ncost = float(r.get("network_cost", 0))  # Ad Spend (network)
        rev = float(r.get("revenue", 0))
        row = {
            "group": r.get(key, ""),
            "installs": float(r.get("installs", 0)),
            "cohort_users_d0": float(r.get("daus", 0)),
            "install_per_user": round(inst / daus, 2) if daus > 0 else 0,
            "cost": round(ncost, 2),  # Ad Spend (network)
            "ecpi": float(r.get("ecpi_all", 0)),
            "cpu": round(ncost / daus, 4) if daus > 0 else 0,
            "revenue": float(r.get("revenue", 0)),
            "roas_all": round(rev / ncost * 100, 2) if ncost > 0 else 0,
        }
        for d in DAYS:
            rv = float(r.get(f"revenue_total_d{d}", 0))
            row[f"ltv_d{d}"] = round(rv / inst, 4) if inst > 0 else 0
            # ROAS Dx = Ad Revenue Cohort Dx / Ad Spend (network) * 100
            row[f"roas_d{d}"] = round(rv / ncost * 100, 2) if ncost > 0 else 0
        return row

    def totals_from_api(api_totals):
        """Use totals directly from Adjust API response."""
        t = api_totals
        if not t:
            return {"group": "TOTAL"}
        inst = float(t.get("installs", 0))
        daus = float(t.get("daus", 0))
        ncost = float(t.get("network_cost", 0))
        rev = float(t.get("revenue", 0))
        row = {
            "group": "TOTAL",
            "installs": inst,
            "cohort_users_d0": daus,
            "install_per_user": round(inst / daus, 2) if daus > 0 else 0,
            "cost": round(ncost, 2),
            "ecpi": float(t.get("ecpi_all", 0)),
            "cpu": round(ncost / daus, 4) if daus > 0 else 0,
            "revenue": rev,
            "roas_all": round(rev / ncost * 100, 2) if ncost > 0 else 0,
        }
        for d in DAYS:
            rv = float(t.get(f"revenue_total_d{d}", 0))
            row[f"ltv_d{d}"] = round(rv / inst, 4) if inst > 0 else 0
            row[f"roas_d{d}"] = round(rv / ncost * 100, 2) if ncost > 0 else 0
        return row

    by_date = [passthrough(r, "day") for r in sorted(daily_resp["rows"], key=lambda x: x.get("day", ""))]
    by_country = [passthrough(r, "country") for r in sorted(country_resp["rows"], key=lambda x: float(x.get("installs", 0)), reverse=True)]

    result = {
        "date_period": dp,
        "by_date": by_date,
        "by_country": by_country,
        "totals_date": totals_from_api(daily_resp.get("totals", {})),
        "totals_country": totals_from_api(country_resp.get("totals", {})),
    }
    _cohort_cache[cache_key] = {"ts": time.time(), "data": result}
    return JSONResponse(result)


@app.get("/api/business_view")
async def business_view(
    request: Request,
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Business View: KPIs, trends, patterns, alerts for CEO."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    s30 = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    e30 = end or datetime.today().strftime("%Y-%m-%d")
    dp = f"{s30}:{e30}"

    # Previous period for comparison
    days_range = (datetime.strptime(e30, "%Y-%m-%d") - datetime.strptime(s30, "%Y-%m-%d")).days
    prev_end = (datetime.strptime(s30, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_start = (datetime.strptime(prev_end, "%Y-%m-%d") - timedelta(days=days_range)).strftime("%Y-%m-%d")
    dp_prev = f"{prev_start}:{prev_end}"

    tk = await _resolve_app_token("APL389")
    hdrs = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
    metrics = "installs,network_cost,revenue,sessions,daus,ecpi_all,revenue_total_d0,revenue_total_d7"

    import asyncio as _aio3

    async def _bv_fetch(period, dims, limit=1000):
        q = "&".join([
            f"app_token__in={tk}", f"date_period={period}",
            f"dimensions={dims}", f"metrics={metrics}",
            "attribution_source=first", "utc_offset=%2B07:00",
            f"sort=-installs", f"limit={limit}", "format=json",
        ])
        url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, headers=hdrs)
            if resp.status_code != 200:
                return {"rows": [], "totals": {}}
            data = resp.json()
            return {"rows": data.get("rows", []), "totals": data.get("totals", {})}

    # Parallel fetch: current daily, current country, previous daily
    cur_daily, cur_country, prev_daily = await _aio3.gather(
        _bv_fetch(dp, "day,app"), _bv_fetch(dp, "country,app", 500), _bv_fetch(dp_prev, "day,app"),
    )

    def _f(v): return float(v) if v else 0.0

    def _agg(rows):
        t = {"installs": 0, "cost": 0, "revenue": 0, "sessions": 0, "daus": 0, "rd0": 0, "rd7": 0}
        for r in rows:
            t["installs"] += _f(r.get("installs"))
            t["cost"] += _f(r.get("network_cost", r.get("cost", 0)))
            t["revenue"] += _f(r.get("revenue"))
            t["sessions"] += _f(r.get("sessions"))
            t["daus"] += _f(r.get("daus"))
            t["rd0"] += _f(r.get("revenue_total_d0"))
            t["rd7"] += _f(r.get("revenue_total_d7"))
        return t

    cur = _agg(cur_daily["rows"])
    prev = _agg(prev_daily["rows"])

    def _pct(c, p): return round((c - p) / p * 100, 1) if p else 0

    profit = cur["revenue"] - cur["cost"]
    prev_profit = prev["revenue"] - prev["cost"]
    ecpi_cur = cur["cost"] / cur["installs"] if cur["installs"] else 0
    ecpi_prev = prev["cost"] / prev["installs"] if prev["installs"] else 0
    roas_cur = cur["rd7"] / cur["cost"] * 100 if cur["cost"] else 0
    roas_prev = prev["rd7"] / prev["cost"] * 100 if prev["cost"] else 0

    kpis = {
        "revenue": {"current": round(cur["revenue"], 2), "previous": round(prev["revenue"], 2), "change": _pct(cur["revenue"], prev["revenue"])},
        "cost": {"current": round(cur["cost"], 2), "previous": round(prev["cost"], 2), "change": _pct(cur["cost"], prev["cost"])},
        "profit": {"current": round(profit, 2), "previous": round(prev_profit, 2), "change": _pct(profit, prev_profit)},
        "roas": {"current": round(roas_cur, 2), "previous": round(roas_prev, 2), "change": round(roas_cur - roas_prev, 2)},
        "installs": {"current": int(cur["installs"]), "previous": int(prev["installs"]), "change": _pct(cur["installs"], prev["installs"])},
        "ecpi": {"current": round(ecpi_cur, 4), "previous": round(ecpi_prev, 4), "change": _pct(ecpi_cur, ecpi_prev)},
    }

    # Daily series
    daily = []
    for r in sorted(cur_daily["rows"], key=lambda x: x.get("day", "")):
        cost = _f(r.get("network_cost", r.get("cost", 0)))
        rev = _f(r.get("revenue"))
        rd7 = _f(r.get("revenue_total_d7"))
        daily.append({
            "date": r.get("day", ""),
            "revenue": round(rev, 2), "cost": round(cost, 2),
            "profit": round(rev - cost, 2),
            "installs": int(_f(r.get("installs"))),
            "roas_d7": round(rd7 / cost * 100, 2) if cost else 0,
            "daus": int(_f(r.get("daus"))),
        })

    # 7-day moving average ROAS
    for i, d in enumerate(daily):
        window = daily[max(0, i - 6):i + 1]
        d["roas_ma7"] = round(sum(x["roas_d7"] for x in window) / len(window), 2) if window else 0

    # Top countries
    countries = []
    for r in sorted(cur_country["rows"], key=lambda x: _f(x.get("revenue")), reverse=True)[:10]:
        cost = _f(r.get("network_cost", r.get("cost", 0)))
        rev = _f(r.get("revenue"))
        rd7 = _f(r.get("revenue_total_d7"))
        countries.append({
            "country": r.get("country", ""),
            "revenue": round(rev, 2), "cost": round(cost, 2),
            "profit": round(rev - cost, 2),
            "roas_d7": round(rd7 / cost * 100, 2) if cost else 0,
            "installs": int(_f(r.get("installs"))),
        })

    # Week comparison (last 7 vs previous 7)
    if len(daily) >= 14:
        w1 = daily[-7:]
        w2 = daily[-14:-7]
    elif len(daily) >= 7:
        w1 = daily[-7:]
        w2 = daily[:len(daily) - 7] if len(daily) > 7 else w1
    else:
        w1 = daily
        w2 = daily

    def _wagg(ds):
        return {
            "installs": sum(d["installs"] for d in ds),
            "revenue": round(sum(d["revenue"] for d in ds), 2),
            "cost": round(sum(d["cost"] for d in ds), 2),
            "profit": round(sum(d["profit"] for d in ds), 2),
            "roas": round(sum(d["roas_d7"] for d in ds) / len(ds), 2) if ds else 0,
            "daus": round(sum(d["daus"] for d in ds) / len(ds)),
        }

    week_cur = _wagg(w1)
    week_prev = _wagg(w2)
    week_comp = {}
    for k in week_cur:
        c, p = week_cur[k], week_prev[k]
        week_comp[k] = {"current": c, "previous": p, "change": _pct(c, p)}

    # Alerts
    alerts = []
    if roas_cur < 100:
        alerts.append({"type": "danger", "msg": f"ROAS trung b\u00ECnh \u0111ang d\u01B0\u1EDBi 100% ({roas_cur:.1f}%). C\u1EA7n t\u1ED1i \u01B0u chi\u1EBFn l\u01B0\u1EE3c UA."})
    if kpis["cost"]["change"] > 20 and kpis["revenue"]["change"] < 5:
        alerts.append({"type": "warning", "msg": f"Chi ph\u00ED t\u0103ng {kpis['cost']['change']}% nh\u01B0ng doanh thu ch\u1EC9 t\u0103ng {kpis['revenue']['change']}%."})
    if kpis["installs"]["change"] < -15:
        alerts.append({"type": "danger", "msg": f"L\u01B0\u1EE3t c\u00E0i \u0111\u1EB7t gi\u1EA3m {abs(kpis['installs']['change'])}% so v\u1EDBi k\u1EF3 tr\u01B0\u1EDBc."})
    if profit > 0:
        alerts.append({"type": "success", "msg": f"S\u1EA3n ph\u1EA9m \u0111ang c\u00F3 l\u00E3i r\u00F2ng ${profit:,.0f} trong k\u1EF3 n\u00E0y."})
    for c in countries[:5]:
        if c["roas_d7"] > 150:
            alerts.append({"type": "success", "msg": f"Th\u1ECB tr\u01B0\u1EDDng {c['country']} \u0111ang t\u0103ng tr\u01B0\u1EDFng m\u1EA1nh v\u1EDBi ROAS {c['roas_d7']}%."})
    for c in countries:
        if c["roas_d7"] < 80 and c["cost"] > 500:
            alerts.append({"type": "danger", "msg": f"Th\u1ECB tr\u01B0\u1EDDng {c['country']} ROAS ch\u1EC9 {c['roas_d7']}% v\u1EDBi chi ph\u00ED ${c['cost']:,.0f}."})
    # 3 consecutive days ROAS < 100
    if len(daily) >= 3 and all(d["roas_d7"] < 100 for d in daily[-3:]):
        alerts.append({"type": "danger", "msg": "ROAS d\u01B0\u1EDBi 100% trong 3 ng\u00E0y li\u00EAn ti\u1EBFp. C\u1EA7n h\u00E0nh \u0111\u1ED9ng ngay."})

    return JSONResponse({
        "kpis": kpis, "daily": daily, "countries": countries,
        "week_comparison": week_comp, "alerts": alerts,
        "period": {"current": dp, "previous": dp_prev},
    })


@app.post("/api/ai_insights")
async def ai_insights(request: Request):
    """Call Claude API for Vietnamese business insights."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    body = await request.json()
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return JSONResponse({"error": "ANTHROPIC_API_KEY not configured"}, status_code=500)

    kpis = body.get("kpis", {})
    countries = body.get("countries", [])[:5]
    alerts = body.get("alerts", [])
    week = body.get("week_comparison", {})

    data_summary = f"""
D\u1EEF li\u1EC7u APL389 - Photo Video Maker:

KPI ch\u00EDnh:
- Doanh thu: ${kpis.get('revenue',{}).get('current',0):,.0f} (thay \u0111\u1ED5i: {kpis.get('revenue',{}).get('change',0)}%)
- Chi ph\u00ED QC: ${kpis.get('cost',{}).get('current',0):,.0f} (thay \u0111\u1ED5i: {kpis.get('cost',{}).get('change',0)}%)
- L\u1EE3i nhu\u1EADn: ${kpis.get('profit',{}).get('current',0):,.0f} (thay \u0111\u1ED5i: {kpis.get('profit',{}).get('change',0)}%)
- ROAS D7: {kpis.get('roas',{}).get('current',0)}%
- T\u1ED5ng installs: {kpis.get('installs',{}).get('current',0):,}
- eCPI: ${kpis.get('ecpi',{}).get('current',0)}

So s\u00E1nh 7 ng\u00E0y g\u1EA7n nh\u1EA5t vs 7 ng\u00E0y tr\u01B0\u1EDBc:
- Installs: {week.get('installs',{}).get('current',0):,} vs {week.get('installs',{}).get('previous',0):,} ({week.get('installs',{}).get('change',0)}%)
- Revenue: ${week.get('revenue',{}).get('current',0):,.0f} vs ${week.get('revenue',{}).get('previous',0):,.0f} ({week.get('revenue',{}).get('change',0)}%)
- ROAS: {week.get('roas',{}).get('current',0)}% vs {week.get('roas',{}).get('previous',0)}%

Top 5 th\u1ECB tr\u01B0\u1EDDng:
""" + "\n".join(f"- {c['country']}: Revenue ${c['revenue']:,.0f}, Cost ${c['cost']:,.0f}, ROAS {c['roas_d7']}%" for c in countries)

    if alerts:
        data_summary += "\n\nC\u1EA3nh b\u00E1o:\n" + "\n".join(f"- {a['msg']}" for a in alerts[:5])

    prompt = f"""B\u1EA1n l\u00E0 chuy\u00EAn gia ph\u00E2n t\u00EDch kinh doanh mobile app. D\u1EF1a tr\u00EAn d\u1EEF li\u1EC7u sau, h\u00E3y ph\u00E2n t\u00EDch v\u00E0 \u0111\u01B0a ra g\u1EE3i \u00FD b\u1EB1ng ti\u1EBFng Vi\u1EC7t c\u00F3 d\u1EA5u.

{data_summary}

H\u00E3y tr\u1EA3 l\u1EDDi theo 3 ph\u1EA7n:
1. \u0110I\u1EC2M T\u1ED0T - \u0110i\u1EC1u g\u00EC \u0111ang ho\u1EA1t \u0111\u1ED9ng t\u1ED1t
2. \u0110I\u1EC2M C\u1EA6N C\u1EA2I THI\u1EC6N - \u0110i\u1EC1u g\u00EC \u0111ang c\u00F3 v\u1EA5n \u0111\u1EC1
3. G\u1EE2I \u00DD H\u00C0NH \u0110\u1ED8NG - G\u1EE3i \u00FD c\u1EE5 th\u1EC3 \u0111\u1EC3 c\u1EA3i thi\u1EC7n

M\u1ED7i ph\u1EA7n 3-5 \u0111i\u1EC3m ng\u1EAFn g\u1ECDn, th\u1EF1c t\u1EBF."""

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1500,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            if resp.status_code != 200:
                return JSONResponse({"error": f"Claude API: {resp.text[:300]}"}, status_code=502)
            result = resp.json()
            text = result.get("content", [{}])[0].get("text", "")
            return JSONResponse({"insights": text, "generated_at": datetime.now(timezone.utc).isoformat()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


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
    app_tokens_str = await _resolve_app_token(app_filter)
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    import asyncio as _aio2

    # Fetch daily + country in parallel with single token
    q = "&".join([
        f"app_token__in={app_tokens_str}",
        f"date_period={dp}",
        "dimensions=day,app",
        "metrics=installs,network_cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
        "attribution_source=first", "utc_offset=%2B07:00", "sort=-installs", "limit=1000", "format=json",
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
        "metrics=installs,network_cost,revenue,revenue_total_d0,revenue_total_d1,revenue_total_d3,revenue_total_d7",
        "attribution_source=first", "utc_offset=%2B07:00", "sort=-installs", "limit=1000", "format=json",
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
        cost = float(r.get("network_cost", r.get("cost", 0)))
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
            "roas_d0": round(rd0 / cost * 100, 2) if cost > 0 else 0,
            "roas_d1": round(rd1 / cost * 100, 2) if cost > 0 else 0,
            "roas_d3": round(rd3 / cost * 100, 2) if cost > 0 else 0,
            "roas_d7": round(rd7 / cost * 100, 2) if cost > 0 else 0,
        })

    # Build country ROAS
    by_country = []
    for r in sorted(country_rows, key=lambda x: float(x.get("installs", 0)), reverse=True)[:15]:
        cost = float(r.get("network_cost", r.get("cost", 0)))
        rd0 = float(r.get("revenue_total_d0", 0))
        rd7 = float(r.get("revenue_total_d7", 0))
        rev = float(r.get("revenue", 0))
        by_country.append({
            "country": r.get("country", ""),
            "installs": int(float(r.get("installs", 0))),
            "cost": round(cost, 2),
            "revenue": round(rev, 2),
            "roas": round(rd7 / cost * 100, 2) if cost > 0 else 0,
            "roas_d0": round(rd0 / cost * 100, 2) if cost > 0 else 0,
            "roas_d7": round(rd7 / cost * 100, 2) if cost > 0 else 0,
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
            "roas_d0": round(t_rd0 / t_cost * 100, 2) if t_cost > 0 else 0,
            "roas_d7": round(t_rd7 / t_cost * 100, 2) if t_cost > 0 else 0,
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
        "metrics=installs,clicks,network_cost,revenue,sessions,daus",
        "attribution_source=first", "utc_offset=%2B07:00",
        "limit=5000", "format=json",
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
        t["cost"] += float(r.get("network_cost", r.get("cost", 0)))
        t["revenue"] += float(r.get("revenue", 0))
        t["sessions"] += float(r.get("sessions", 0))
        t["daus"] += float(r.get("daus", 0))
        t["clicks"] += float(r.get("clicks", 0))
        if day:
            apps[app_name]["daily"][day] = {
                "installs": float(r.get("installs", 0)),
                "cost": float(r.get("network_cost", r.get("cost", 0))),
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
