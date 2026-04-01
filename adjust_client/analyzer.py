"""
Business analytics and anomaly detection for Adjust data.
All functions operate on the flat all_rows list from the cache.
"""


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def compute_overview(rows):
    """
    Aggregate KPI totals and daily trend from all rows.
    Returns {totals, daily}.
    """
    totals = {
        "installs": 0, "clicks": 0, "impressions": 0,
        "cost": 0.0, "revenue": 0.0, "sessions": 0, "daus": 0,
    }
    day_agg = {}

    for row in rows:
        installs = safe_float(row.get("installs"))
        clicks = safe_float(row.get("clicks"))
        impressions = safe_float(row.get("impressions"))
        cost = safe_float(row.get("cost"))
        revenue = safe_float(row.get("revenue"))
        sessions = safe_float(row.get("sessions"))
        daus = safe_float(row.get("daus"))

        totals["installs"] += installs
        totals["clicks"] += clicks
        totals["impressions"] += impressions
        totals["cost"] += cost
        totals["revenue"] += revenue
        totals["sessions"] += sessions
        totals["daus"] += daus

        day = row.get("day", "")
        if day:
            if day not in day_agg:
                day_agg[day] = {"date": day, "installs": 0, "clicks": 0,
                                "impressions": 0, "cost": 0.0, "revenue": 0.0,
                                "sessions": 0, "daus": 0}
            day_agg[day]["installs"] += installs
            day_agg[day]["clicks"] += clicks
            day_agg[day]["impressions"] += impressions
            day_agg[day]["cost"] += cost
            day_agg[day]["revenue"] += revenue
            day_agg[day]["sessions"] += sessions
            day_agg[day]["daus"] += daus

    totals["cost"] = round(totals["cost"], 2)
    totals["revenue"] = round(totals["revenue"], 2)
    totals["installs"] = int(totals["installs"])
    totals["clicks"] = int(totals["clicks"])
    totals["impressions"] = int(totals["impressions"])
    totals["sessions"] = int(totals["sessions"])
    totals["daus"] = int(totals["daus"])
    totals["ecpi"] = (
        round(totals["cost"] / totals["installs"], 2)
        if totals["installs"] > 0 else 0.0
    )

    daily = sorted(day_agg.values(), key=lambda x: x["date"])
    for d in daily:
        d["cost"] = round(d["cost"], 2)
        d["revenue"] = round(d["revenue"], 2)

    return {"totals": totals, "daily": daily}


def daily_by_app(rows):
    """
    Build per-app daily series for the trend chart.
    Returns {dates: [...], apps: {app_name: {installs: [...], revenue: [...]}}}
    """
    app_day = {}  # app -> day -> {installs, revenue}
    all_dates = set()

    for row in rows:
        app = row.get("app", "unknown")
        day = row.get("day", "")
        if not day:
            continue
        all_dates.add(day)
        if app not in app_day:
            app_day[app] = {}
        if day not in app_day[app]:
            app_day[app][day] = {"installs": 0, "revenue": 0.0}
        app_day[app][day]["installs"] += safe_float(row.get("installs"))
        app_day[app][day]["revenue"] += safe_float(row.get("revenue"))

    dates = sorted(all_dates)
    apps = {}
    for app, days in app_day.items():
        apps[app] = {
            "installs": [days.get(d, {}).get("installs", 0) for d in dates],
            "revenue": [round(days.get(d, {}).get("revenue", 0), 2) for d in dates],
        }

    return {"dates": dates, "apps": apps}


def app_comparison(rows):
    """Aggregate metrics per app for the comparison table."""
    app_agg = {}
    for row in rows:
        app = row.get("app", "unknown")
        if app not in app_agg:
            app_agg[app] = {
                "app": app,
                "installs": 0, "cost": 0.0, "revenue": 0.0,
                "sessions": 0, "daus": 0,
            }
        app_agg[app]["installs"] += safe_float(row.get("installs"))
        app_agg[app]["cost"] += safe_float(row.get("cost"))
        app_agg[app]["revenue"] += safe_float(row.get("revenue"))
        app_agg[app]["sessions"] += safe_float(row.get("sessions"))
        app_agg[app]["daus"] += safe_float(row.get("daus"))

    result = []
    for data in app_agg.values():
        cost = data["cost"]
        revenue = data["revenue"]
        installs = data["installs"]
        result.append({
            "app": data["app"],
            "installs": int(installs),
            "cost": round(cost, 2),
            "revenue": round(revenue, 2),
            "ecpi": round(cost / installs, 2) if installs > 0 else 0.0,
            "sessions": int(data["sessions"]),
            "daus": int(data["daus"]),
        })

    result.sort(key=lambda x: x["revenue"], reverse=True)
    return result


def country_breakdown(rows, limit=20):
    """Aggregate by country, grouped by app."""
    countries = {}
    country_app = {}  # country -> app -> installs

    for row in rows:
        cc = row.get("country", "")
        if not cc:
            continue
        app = row.get("app", "unknown")
        if cc not in countries:
            countries[cc] = {"country": cc, "installs": 0, "clicks": 0,
                             "cost": 0.0, "revenue": 0.0}
        countries[cc]["installs"] += safe_float(row.get("installs"))
        countries[cc]["clicks"] += safe_float(row.get("clicks"))
        countries[cc]["cost"] += safe_float(row.get("cost"))
        countries[cc]["revenue"] += safe_float(row.get("revenue"))

        if cc not in country_app:
            country_app[cc] = {}
        if app not in country_app[cc]:
            country_app[cc][app] = 0
        country_app[cc][app] += safe_float(row.get("installs"))

    result = list(countries.values())
    for r in result:
        r["cost"] = round(r["cost"], 2)
        r["revenue"] = round(r["revenue"], 2)
        r["installs"] = int(r["installs"])
        r["clicks"] = int(r["clicks"])
        r["ecpi"] = round(r["cost"] / r["installs"], 2) if r["installs"] > 0 else 0.0
        r["by_app"] = {app: int(v) for app, v in country_app.get(r["country"], {}).items()}

    result.sort(key=lambda x: x["installs"], reverse=True)
    return result[:limit]


def campaign_breakdown(rows, limit=20):
    """Aggregate by campaign, including app info."""
    campaigns = {}
    for row in rows:
        name = row.get("campaign", "")
        if not name:
            continue
        app = row.get("app", "unknown")
        key = f"{name}||{app}"
        if key not in campaigns:
            campaigns[key] = {
                "campaign": name, "app": app,
                "installs": 0, "clicks": 0, "impressions": 0,
                "cost": 0.0, "revenue": 0.0,
            }
        campaigns[key]["installs"] += safe_float(row.get("installs"))
        campaigns[key]["clicks"] += safe_float(row.get("clicks"))
        campaigns[key]["impressions"] += safe_float(row.get("impressions"))
        campaigns[key]["cost"] += safe_float(row.get("cost"))
        campaigns[key]["revenue"] += safe_float(row.get("revenue"))

    result = list(campaigns.values())
    for r in result:
        r["cost"] = round(r["cost"], 2)
        r["revenue"] = round(r["revenue"], 2)
        r["installs"] = int(r["installs"])
        r["clicks"] = int(r["clicks"])
        r["impressions"] = int(r["impressions"])
        r["ecpi"] = round(r["cost"] / r["installs"], 2) if r["installs"] > 0 else 0.0

    result.sort(key=lambda x: x["revenue"], reverse=True)
    return result[:limit]


def retention_by_app(rows):
    """Aggregate DAU/sessions per app (retention metrics removed)."""
    apps = {}
    for row in rows:
        app = row.get("app", "unknown")
        if app not in apps:
            apps[app] = {"app": app, "installs": 0, "daus": 0, "sessions": 0}
        apps[app]["installs"] += safe_float(row.get("installs"))
        apps[app]["daus"] += safe_float(row.get("daus"))
        apps[app]["sessions"] += safe_float(row.get("sessions"))

    result = []
    for data in apps.values():
        result.append({
            "app": data["app"],
            "installs": int(data["installs"]),
            "daus": int(data["daus"]),
            "sessions": int(data["sessions"]),
        })
    result.sort(key=lambda x: x["installs"], reverse=True)
    return result


def detect_anomalies(daily_data, threshold=0.20):
    """Flag days where a metric spiked/dropped >threshold vs previous day."""
    if len(daily_data) < 2:
        return []

    alerts = []
    monitored = ["installs", "revenue", "cost", "clicks"]

    for i in range(1, len(daily_data)):
        curr = daily_data[i]
        prev = daily_data[i - 1]
        for metric in monitored:
            curr_val = safe_float(curr.get(metric))
            prev_val = safe_float(prev.get(metric))
            if prev_val == 0:
                if curr_val > 0:
                    alerts.append({
                        "date": curr.get("date", ""), "metric": metric,
                        "type": "spike", "previous": 0, "current": curr_val,
                        "change_pct": 100.0,
                    })
                continue
            change = (curr_val - prev_val) / prev_val
            if abs(change) > threshold:
                alerts.append({
                    "date": curr.get("date", ""), "metric": metric,
                    "type": "spike" if change > 0 else "drop",
                    "previous": round(prev_val, 2), "current": round(curr_val, 2),
                    "change_pct": round(change * 100, 1),
                })

    alerts.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
    return alerts
