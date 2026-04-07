"""
FastAPI backend for Adjust Analytics Dashboard.
Session-based authentication with itsdangerous.
Simplified: Data Gateway only.
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv()

from adjust_client.fetcher import ADJUST_TOKEN

app = FastAPI(title="Adjust Analytics Dashboard")
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

# App token mappings
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

# Auth config
DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "admin")
SESSION_SECRET = os.getenv("SESSION_SECRET", "change-me-in-production")
SESSION_MAX_AGE = 8 * 3600
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
    if not _is_authenticated(request):
        return RedirectResponse("/login", status_code=302)
    return None


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


# ─── API endpoints ───

@app.get("/api/app_list")
async def app_list(request: Request):
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    apps = [{"token": t, "code": c, "name": APP_NAME_MAP.get(t, c)}
            for c, t in APP_TOKEN_MAP.items()]
    return JSONResponse({"apps": apps, "default": "apl389"})


@app.get("/api/data_gateway")
async def data_gateway(
    request: Request,
    app_filter: Optional[str] = Query(None, alias="app"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Fetch raw data from Adjust API: date, app, country, installs, cost, revenue, sessions, ecpi."""
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    s = start or (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    e = end or datetime.today().strftime("%Y-%m-%d")
    dp = f"{s}:{e}"

    # Resolve app tokens
    if app_filter and app_filter.lower() != "all":
        af = app_filter.lower()
        tokens = [t for c, t in APP_TOKEN_MAP.items() if af in c]
        if not tokens:
            tokens = [list(APP_TOKEN_MAP.values())[0]]
        app_tokens_str = ",".join(tokens)
    else:
        app_tokens_str = ",".join(APP_TOKEN_MAP.values())

    headers_api = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
    q = "&".join([
        f"app_token__in={app_tokens_str}",
        f"date_period={dp}",
        "dimensions=day,app,country",
        "metrics=installs,network_cost,revenue,sessions,ecpi_all",
        "attribution_source=first",
        "utc_offset=%2B07:00",
        "sort=-day",
        "limit=5000",
        "format=json",
    ])
    url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"

    async with httpx.AsyncClient(timeout=180) as client:
        resp = await client.get(url, headers=headers_api)
        if resp.status_code != 200:
            return JSONResponse({"error": f"Adjust API error: {resp.text[:300]}"}, status_code=502)
        data = resp.json()

    rows = data.get("rows", [])
    result = []
    for r in rows:
        inst = float(r.get("installs", 0))
        cost = float(r.get("network_cost", r.get("cost", 0)))
        rev = float(r.get("revenue", 0))
        sessions = float(r.get("sessions", 0))
        ecpi = float(r.get("ecpi_all", 0))
        result.append({
            "date": r.get("day", ""),
            "app": r.get("app", ""),
            "country": r.get("country", ""),
            "installs": int(inst),
            "cost": round(cost, 2),
            "revenue": round(rev, 2),
            "sessions": int(sessions),
            "ecpi": round(ecpi, 4),
        })

    return JSONResponse({
        "rows": result,
        "date_period": dp,
        "total_rows": len(result),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    })
