"""
Business analytics and anomaly detection for Adjust data.
All functions operate on the flat all_rows list from the cache.
Includes cohort revenue and ROAS D0/D1/D3/D7 calculations.
"""

COHORT_KEYS = ["revenue_total_d0", "revenue_total_d1", "revenue_total_d3", "revenue_total_d7"]


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _roas(revenue, cost):
    return round(revenue / cost, 2) if cost > 0 else 0.0


def compute_overview(rows):
    """Aggregate KPI totals and daily trend including cohort metrics."""
    totals = {
        "installs": 0, "clicks": 0, "impressions": 0,
        "cost": 0.0, "revenue": 0.0, "sessions": 0, "daus": 0,
        "rev_d0": 0.0, "rev_d1": 0.0, "rev_d3": 0.0, "rev_d7": 0.0,
    }
    day_agg = {}

    for row in rows:
        installs = safe_float(row.get("installs"))
        clicks = safe_float(row.get("clicks"))
        impressions = safe_float(row.get("impressions"))
        cost = safe_float(row.get("network_cost", row.get("cost")))
        revenue = safe_float(row.get("revenue"))
        sessions = safe_float(row.get("sessions"))
        daus = safe_float(row.get("daus"))
        rev_d0 = safe_float(row.get("revenue_total_d0"))
        rev_d1 = safe_float(row.get("revenue_total_d1"))
        rev_d3 = safe_float(row.get("revenue_total_d3"))
        rev_d7 = safe_float(row.get("revenue_total_d7"))

        totals["installs"] += installs
        totals["clicks"] += clicks
        totals["impressions"] += impressions
        totals["cost"] += cost
        totals["revenue"] += revenue
        totals["sessions"] += sessions
        totals["daus"] += daus
        totals["rev_d0"] += rev_d0
        totals["rev_d1"] += rev_d1
        totals["rev_d3"] += rev_d3
        totals["rev_d7"] += rev_d7

        day = row.get("day", "")
        if day:
            if day not in day_agg:
                day_agg[day] = {
                    "date": day, "installs": 0, "clicks": 0,
                    "impressions": 0, "cost": 0.0, "revenue": 0.0,
                    "sessions": 0, "daus": 0,
                    "rev_d0": 0.0, "rev_d1": 0.0, "rev_d3": 0.0, "rev_d7": 0.0,
                }
            d = day_agg[day]
            d["installs"] += installs
            d["clicks"] += clicks
            d["impressions"] += impressions
            d["cost"] += cost
            d["revenue"] += revenue
            d["sessions"] += sessions
            d["daus"] += daus
            d["rev_d0"] += rev_d0
            d["rev_d1"] += rev_d1
            d["rev_d3"] += rev_d3
            d["rev_d7"] += rev_d7

    totals["cost"] = round(totals["cost"], 2)
    totals["revenue"] = round(totals["revenue"], 2)
    totals["installs"] = int(totals["installs"])
    totals["clicks"] = int(totals["clicks"])
    totals["impressions"] = int(totals["impressions"])
    totals["sessions"] = int(totals["sessions"])
    totals["daus"] = int(totals["daus"])
    totals["ecpi"] = round(totals["cost"] / totals["installs"], 2) if totals["installs"] > 0 else 0.0
    totals["rev_d0"] = round(totals["rev_d0"], 2)
    totals["rev_d1"] = round(totals["rev_d1"], 2)
    totals["rev_d3"] = round(totals["rev_d3"], 2)
    totals["rev_d7"] = round(totals["rev_d7"], 2)
    totals["roas_d0"] = _roas(totals["rev_d0"], totals["cost"])
    totals["roas_d1"] = _roas(totals["rev_d1"], totals["cost"])
    totals["roas_d3"] = _roas(totals["rev_d3"], totals["cost"])
    totals["roas_d7"] = _roas(totals["rev_d7"], totals["cost"])

    daily = sorted(day_agg.values(), key=lambda x: x["date"])
    for d in daily:
        d["cost"] = round(d["cost"], 2)
        d["revenue"] = round(d["revenue"], 2)
        d["rev_d0"] = round(d["rev_d0"], 2)
        d["rev_d1"] = round(d["rev_d1"], 2)
        d["rev_d3"] = round(d["rev_d3"], 2)
        d["rev_d7"] = round(d["rev_d7"], 2)
        d["roas_d0"] = _roas(d["rev_d0"], d["cost"])
        d["roas_d1"] = _roas(d["rev_d1"], d["cost"])
        d["roas_d3"] = _roas(d["rev_d3"], d["cost"])
        d["roas_d7"] = _roas(d["rev_d7"], d["cost"])

    return {"totals": totals, "daily": daily}


def daily_by_app(rows):
    """Build per-app daily series for the trend chart."""
    app_day = {}
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
            app_day[app][day] = {"installs": 0, "revenue": 0.0, "rev_d0": 0.0, "rev_d7": 0.0, "cost": 0.0}
        app_day[app][day]["installs"] += safe_float(row.get("installs"))
        app_day[app][day]["revenue"] += safe_float(row.get("revenue"))
        app_day[app][day]["rev_d0"] += safe_float(row.get("revenue_total_d0"))
        app_day[app][day]["rev_d7"] += safe_float(row.get("revenue_total_d7"))
        app_day[app][day]["cost"] += safe_float(row.get("network_cost", row.get("cost")))

    dates = sorted(all_dates)
    apps = {}
    for app, days in app_day.items():
        apps[app] = {
            "installs": [days.get(d, {}).get("installs", 0) for d in dates],
            "revenue": [round(days.get(d, {}).get("revenue", 0), 2) for d in dates],
            "roas_d0": [_roas(days.get(d, {}).get("rev_d0", 0), days.get(d, {}).get("cost", 0)) for d in dates],
            "roas_d7": [_roas(days.get(d, {}).get("rev_d7", 0), days.get(d, {}).get("cost", 0)) for d in dates],
        }

    return {"dates": dates, "apps": apps}


def app_comparison(rows):
    """Aggregate metrics per app including cohort data."""
    app_agg = {}
    for row in rows:
        app = row.get("app", "unknown")
        if app not in app_agg:
            app_agg[app] = {
                "app": app,
                "installs": 0, "cost": 0.0, "revenue": 0.0,
                "sessions": 0, "daus": 0,
                "rev_d0": 0.0, "rev_d1": 0.0, "rev_d3": 0.0, "rev_d7": 0.0,
            }
        a = app_agg[app]
        a["installs"] += safe_float(row.get("installs"))
        a["cost"] += safe_float(row.get("network_cost", row.get("cost")))
        a["revenue"] += safe_float(row.get("revenue"))
        a["sessions"] += safe_float(row.get("sessions"))
        a["daus"] += safe_float(row.get("daus"))
        a["rev_d0"] += safe_float(row.get("revenue_total_d0"))
        a["rev_d1"] += safe_float(row.get("revenue_total_d1"))
        a["rev_d3"] += safe_float(row.get("revenue_total_d3"))
        a["rev_d7"] += safe_float(row.get("revenue_total_d7"))

    result = []
    for data in app_agg.values():
        cost = data["cost"]
        installs = data["installs"]
        result.append({
            "app": data["app"],
            "installs": int(installs),
            "cost": round(cost, 2),
            "revenue": round(data["revenue"], 2),
            "ecpi": round(cost / installs, 2) if installs > 0 else 0.0,
            "sessions": int(data["sessions"]),
            "daus": int(data["daus"]),
            "rev_d0": round(data["rev_d0"], 2),
            "rev_d1": round(data["rev_d1"], 2),
            "rev_d3": round(data["rev_d3"], 2),
            "rev_d7": round(data["rev_d7"], 2),
            "roas_d0": _roas(data["rev_d0"], cost),
            "roas_d1": _roas(data["rev_d1"], cost),
            "roas_d3": _roas(data["rev_d3"], cost),
            "roas_d7": _roas(data["rev_d7"], cost),
        })

    result.sort(key=lambda x: x["revenue"], reverse=True)
    return result


def country_breakdown(rows, limit=20):
    """Aggregate by country with cohort metrics."""
    countries = {}
    country_app = {}

    for row in rows:
        cc = row.get("country", "")
        if not cc:
            continue
        app = row.get("app", "unknown")
        if cc not in countries:
            countries[cc] = {
                "country": cc, "installs": 0, "clicks": 0,
                "cost": 0.0, "revenue": 0.0,
                "rev_d0": 0.0, "rev_d1": 0.0, "rev_d3": 0.0, "rev_d7": 0.0,
            }
        c = countries[cc]
        c["installs"] += safe_float(row.get("installs"))
        c["clicks"] += safe_float(row.get("clicks"))
        c["cost"] += safe_float(row.get("network_cost", row.get("cost")))
        c["revenue"] += safe_float(row.get("revenue"))
        c["rev_d0"] += safe_float(row.get("revenue_total_d0"))
        c["rev_d1"] += safe_float(row.get("revenue_total_d1"))
        c["rev_d3"] += safe_float(row.get("revenue_total_d3"))
        c["rev_d7"] += safe_float(row.get("revenue_total_d7"))

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
        r["rev_d0"] = round(r["rev_d0"], 2)
        r["rev_d1"] = round(r["rev_d1"], 2)
        r["rev_d3"] = round(r["rev_d3"], 2)
        r["rev_d7"] = round(r["rev_d7"], 2)
        r["roas_d0"] = _roas(r["rev_d0"], r["cost"])
        r["roas_d1"] = _roas(r["rev_d1"], r["cost"])
        r["roas_d3"] = _roas(r["rev_d3"], r["cost"])
        r["roas_d7"] = _roas(r["rev_d7"], r["cost"])
        r["by_app"] = {app: int(v) for app, v in country_app.get(r["country"], {}).items()}

    result.sort(key=lambda x: x["installs"], reverse=True)
    return result[:limit]


def campaign_breakdown(rows, limit=20):
    """Aggregate by campaign with cohort metrics."""
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
                "rev_d0": 0.0, "rev_d1": 0.0, "rev_d3": 0.0, "rev_d7": 0.0,
            }
        c = campaigns[key]
        c["installs"] += safe_float(row.get("installs"))
        c["clicks"] += safe_float(row.get("clicks"))
        c["impressions"] += safe_float(row.get("impressions"))
        c["cost"] += safe_float(row.get("network_cost", row.get("cost")))
        c["revenue"] += safe_float(row.get("revenue"))
        c["rev_d0"] += safe_float(row.get("revenue_total_d0"))
        c["rev_d1"] += safe_float(row.get("revenue_total_d1"))
        c["rev_d3"] += safe_float(row.get("revenue_total_d3"))
        c["rev_d7"] += safe_float(row.get("revenue_total_d7"))

    result = list(campaigns.values())
    for r in result:
        r["cost"] = round(r["cost"], 2)
        r["revenue"] = round(r["revenue"], 2)
        r["installs"] = int(r["installs"])
        r["clicks"] = int(r["clicks"])
        r["impressions"] = int(r["impressions"])
        r["ecpi"] = round(r["cost"] / r["installs"], 2) if r["installs"] > 0 else 0.0
        r["rev_d0"] = round(r["rev_d0"], 2)
        r["rev_d1"] = round(r["rev_d1"], 2)
        r["rev_d3"] = round(r["rev_d3"], 2)
        r["rev_d7"] = round(r["rev_d7"], 2)
        r["roas_d0"] = _roas(r["rev_d0"], r["cost"])
        r["roas_d1"] = _roas(r["rev_d1"], r["cost"])
        r["roas_d3"] = _roas(r["rev_d3"], r["cost"])
        r["roas_d7"] = _roas(r["rev_d7"], r["cost"])

    result.sort(key=lambda x: x["revenue"], reverse=True)
    return result[:limit]


def retention_by_app(rows):
    """Aggregate DAU/sessions per app."""
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
    monitored = ["installs", "revenue", "cost", "clicks", "roas_d0"]

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
