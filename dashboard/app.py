"""
FastAPI backend for Adjust Analytics Dashboard.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

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


@app.get("/", response_class=HTMLResponse)
async def index():
    html_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/api/apps")
async def get_apps():
    cached = load_cache()
    apps = cached.get("apps", [])
    if not apps:
        apps = [{"token": t, "name": t} for t in ADJUST_APP_TOKENS]
    return JSONResponse({"apps": apps})


@app.get("/api/overview")
async def get_overview():
    cached = load_cache()
    rows = cached.get("all_rows", [])
    overview = compute_overview(rows)
    anomalies = detect_anomalies(overview["daily"])
    return JSONResponse({
        **overview,
        "anomalies": anomalies,
        "fetched_at": cached.get("fetched_at"),
        "errors": cached.get("errors", []),
    })


@app.get("/api/trend")
async def get_trend():
    cached = load_cache()
    rows = cached.get("all_rows", [])
    trend = daily_by_app(rows)
    return JSONResponse({"trend": trend, "fetched_at": cached.get("fetched_at")})


@app.get("/api/performance")
async def get_performance():
    cached = load_cache()
    rows = cached.get("all_rows", [])
    apps = app_comparison(rows)
    return JSONResponse({"apps": apps, "fetched_at": cached.get("fetched_at")})


@app.get("/api/countries")
async def get_countries():
    cached = load_cache()
    rows = cached.get("country_rows", cached.get("all_rows", []))
    countries = country_breakdown(rows)
    return JSONResponse({"countries": countries, "fetched_at": cached.get("fetched_at")})


@app.get("/api/campaigns")
async def get_campaigns():
    cached = load_cache()
    rows = cached.get("campaign_rows", cached.get("all_rows", []))
    campaigns = campaign_breakdown(rows)
    return JSONResponse({"campaigns": campaigns, "fetched_at": cached.get("fetched_at")})


@app.get("/api/retention")
async def get_retention():
    cached = load_cache()
    rows = cached.get("all_rows", [])
    retention = retention_by_app(rows)
    return JSONResponse({"retention": retention, "fetched_at": cached.get("fetched_at")})


@app.get("/roas", response_class=HTMLResponse)
async def roas_page():
    html_file = TEMPLATES_DIR / "roas.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/api/roas")
async def roas_data():
    """Serve cached ROAS analysis data."""
    roas_file = Path(__file__).resolve().parent.parent / "data" / "roas_analysis.json"
    if roas_file.exists():
        import json as _json
        with open(roas_file) as f:
            return JSONResponse(_json.load(f))
    return JSONResponse({"error": "No ROAS data. Run scripts/roas_fetch.py first."}, status_code=404)


@app.get("/brazil", response_class=HTMLResponse)
async def brazil_page():
    html_file = TEMPLATES_DIR / "brazil.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/api/brazil")
async def brazil_analysis():
    """Fetch APL389 Brazil 7-day analysis live from Adjust API."""
    end = datetime.today()
    start = end - timedelta(days=7)
    dp = f"{start.strftime('%Y-%m-%d')}:{end.strftime('%Y-%m-%d')}"
    app_tokens = os.getenv("ADJUST_APP_TOKENS", "")
    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}

    # Fetch country+app+day breakdown
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

    # Build per-app daily data
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

    # Build response
    result = []
    for name, data in apps.items():
        t = data["totals"]
        ecpi = round(t["cost"] / t["installs"], 3) if t["installs"] > 0 else 0
        rev_cost = round(t["revenue"] / t["cost"], 2) if t["cost"] > 0 else 0
        # Filter anomalous revenue (>$1M per day is likely a bug)
        clean_rev = sum(
            v["revenue"] for v in data["daily"].values() if v["revenue"] < 1_000_000
        )
        clean_ratio = round(clean_rev / t["cost"], 2) if t["cost"] > 0 else 0
        result.append({
            "app": name,
            "installs": int(t["installs"]),
            "cost": round(t["cost"], 2),
            "revenue": round(clean_rev, 2),
            "sessions": int(t["sessions"]),
            "daus": int(t["daus"]),
            "ecpi": ecpi,
            "rev_cost_ratio": clean_ratio,
            "daily": dict(sorted(data["daily"].items())),
        })

    result.sort(key=lambda x: x["installs"], reverse=True)
    return JSONResponse({
        "date_period": dp,
        "country": "Brazil",
        "apps": result,
    })


@app.post("/api/refresh")
async def refresh_data(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_refresh)
    return {"status": "started", "message": "Fetching last 30 days from Adjust API..."}


def _run_refresh():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(fetch_all_data())
    finally:
        loop.close()
