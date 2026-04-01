"""
Adjust Reports Service API client.
Docs: https://dev.adjust.com/en/api/rs-api/

Makes one API call per app token with full dimensions and metrics.
Default date range: last 30 days from today.
"""

import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()

REPORT_URL = "https://dash.adjust.com/control-center/reports-service/report"
DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "adjust_data.json"

ADJUST_TOKEN = os.getenv("ADJUST_TOKEN", "")
ADJUST_APP_TOKENS = [
    t.strip() for t in os.getenv("ADJUST_APP_TOKENS", "").split(",") if t.strip()
]


def _headers():
    return {
        "Authorization": f"Bearer {ADJUST_TOKEN}",
        "Content-Type": "application/json",
    }


def _date_period():
    """Calculate date_period for last 30 days: 'YYYY-MM-DD:YYYY-MM-DD'."""
    end = datetime.today()
    start = end - timedelta(days=30)
    return f"{start.strftime('%Y-%m-%d')}:{end.strftime('%Y-%m-%d')}"


async def fetch_report(app_token, date_period, dimensions, metrics):
    """
    Fetch a single report for one app token and dimension set.
    Builds the URL with raw query string to preserve the colon in date_period.
    """
    params = {
        "app_token__in": app_token,
        "date_period": date_period,
        "dimensions": dimensions,
        "metrics": metrics,
        "sort": "-installs",
        "limit": "1000",
        "format": "json",
    }

    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    full_url = f"{REPORT_URL}?{query_string}"

    print(f"  [API] {app_token} dims={dimensions} -> {full_url[:120]}...")

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(full_url, headers=_headers())

        if resp.status_code != 200:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:500]}"
            print(f"  [API] ERROR: {error_msg}")
            return {"error": error_msg, "rows": []}

        data = resp.json()
        rows = data.get("rows", [])
        print(f"  [API] OK: {len(rows)} rows")
        return {"rows": rows}


async def fetch_all_data(app_tokens=None, start_date=None, end_date=None):
    """
    Fetch data for all app tokens and cache results.
    Makes separate calls per dimension set to keep queries fast.
    """
    tokens = app_tokens or ADJUST_APP_TOKENS
    if not tokens:
        return {"error": "No app tokens configured. Set ADJUST_APP_TOKENS in .env"}

    if start_date and end_date:
        date_period = f"{start_date}:{end_date}"
    else:
        date_period = _date_period()

    print(f"\n[FETCHER] Fetching {len(tokens)} apps, date_period={date_period}")

    results = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "date_period": date_period,
        "app_tokens": tokens,
        "all_rows": [],
        "country_rows": [],
        "campaign_rows": [],
        "errors": [],
        "apps": [{"token": t, "name": t} for t in tokens],
    }

    for token in tokens:
        # 1. Daily metrics per app (for overview + trend chart)
        try:
            report = await fetch_report(
                token, date_period,
                dimensions="day,app",
                metrics="installs,clicks,impressions,cost,revenue,ecpi_all,sessions,daus,waus,maus",
            )
            if report.get("error"):
                results["errors"].append({"source": f"daily:{token}", "error": report["error"]})
            else:
                results["all_rows"].extend(report["rows"])
        except Exception as e:
            print(f"  [FETCHER] daily:{token} EXCEPTION: {e}")
            results["errors"].append({"source": f"daily:{token}", "error": str(e)})

        # 2. Country breakdown
        try:
            report = await fetch_report(
                token, date_period,
                dimensions="country,app",
                metrics="installs,clicks,cost,revenue",
            )
            if report.get("error"):
                results["errors"].append({"source": f"country:{token}", "error": report["error"]})
            else:
                results["country_rows"].extend(report["rows"])
        except Exception as e:
            print(f"  [FETCHER] country:{token} EXCEPTION: {e}")
            results["errors"].append({"source": f"country:{token}", "error": str(e)})

        # 3. Campaign breakdown
        try:
            report = await fetch_report(
                token, date_period,
                dimensions="campaign,app",
                metrics="installs,clicks,cost,revenue",
            )
            if report.get("error"):
                results["errors"].append({"source": f"campaign:{token}", "error": report["error"]})
            else:
                results["campaign_rows"].extend(report["rows"])
        except Exception as e:
            print(f"  [FETCHER] campaign:{token} EXCEPTION: {e}")
            results["errors"].append({"source": f"campaign:{token}", "error": str(e)})

    total = len(results["all_rows"]) + len(results["country_rows"]) + len(results["campaign_rows"])
    print(f"[FETCHER] Done: {len(results['all_rows'])} daily, {len(results['country_rows'])} country, {len(results['campaign_rows'])} campaign rows, {len(results['errors'])} errors")
    save_cache(results)
    return results


def save_cache(data):
    """Save fetched data to JSON cache."""
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_cache():
    """Load cached data from disk."""
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}
