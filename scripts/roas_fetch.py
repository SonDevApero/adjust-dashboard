"""Fetch ROAS analysis data from Adjust API and save to JSON."""
import httpx
import os
import json
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

token = os.getenv('ADJUST_TOKEN')
app_tokens = os.getenv('ADJUST_APP_TOKENS')
headers = {'Authorization': f'Bearer {token}'}

end = datetime.today()
start = end - timedelta(days=7)
dp = f"{start.strftime('%Y-%m-%d')}:{end.strftime('%Y-%m-%d')}"

q = "&".join([
    f"app_token__in={app_tokens}",
    f"date_period={dp}",
    "dimensions=day,app,country",
    "metrics=installs,clicks,cost,revenue,sessions",
    "limit=5000",
    "format=json",
])
url = f"https://dash.adjust.com/control-center/reports-service/report?{q}"
resp = httpx.get(url, headers=headers, timeout=120)
rows = resp.json().get('rows', [])
print(f"Total rows: {len(rows)}")

# Build nested: app -> country -> day -> metrics
data = defaultdict(lambda: defaultdict(lambda: defaultdict(
    lambda: {'cost': 0, 'revenue': 0, 'installs': 0}
)))
app_country_totals = defaultdict(lambda: defaultdict(
    lambda: {'cost': 0, 'revenue': 0, 'installs': 0}
))

for r in rows:
    app = r.get('app', '?')
    country = r.get('country', '?')
    day = r.get('day', '')
    cost = float(r.get('cost', 0))
    rev = float(r.get('revenue', 0))
    inst = float(r.get('installs', 0))
    if rev > 1_000_000:
        rev = 0
    data[app][country][day]['cost'] += cost
    data[app][country][day]['revenue'] += rev
    data[app][country][day]['installs'] += inst
    app_country_totals[app][country]['cost'] += cost
    app_country_totals[app][country]['revenue'] += rev
    app_country_totals[app][country]['installs'] += inst

# Find APL389
apl_key = [k for k in data.keys() if 'APL389' in k][0]
apl_countries = app_country_totals[apl_key]

# Top 15 countries by spend
top_countries = sorted(apl_countries.items(), key=lambda x: x[1]['cost'], reverse=True)[:15]

print(f"\n{'Country':<25} {'Installs':>10} {'Cost':>12} {'Revenue':>12} {'ROAS':>8} {'eCPI':>8}")
print('-' * 80)
for country, t in top_countries:
    roas = round(t['revenue'] / t['cost'], 2) if t['cost'] > 0 else 0
    ecpi = round(t['cost'] / t['installs'], 3) if t['installs'] > 0 else 0
    c_str = f"${t['cost']:,.2f}"
    r_str = f"${t['revenue']:,.2f}"
    e_str = f"${ecpi}"
    print(f"{country:<25} {int(t['installs']):>10,} {c_str:>12} {r_str:>12} {roas}x{'':<4} {e_str:>8}")

# APL389 daily totals
apl_daily = defaultdict(lambda: {'cost': 0, 'revenue': 0, 'installs': 0})
for country in data[apl_key]:
    for day, v in data[apl_key][country].items():
        apl_daily[day]['cost'] += v['cost']
        apl_daily[day]['revenue'] += v['revenue']
        apl_daily[day]['installs'] += v['installs']

print(f"\n{'Date':<12} {'Installs':>10} {'Cost':>12} {'Revenue':>12} {'ROAS':>8}")
print('-' * 55)
for day in sorted(apl_daily.keys()):
    d = apl_daily[day]
    roas = round(d['revenue'] / d['cost'], 2) if d['cost'] > 0 else 0
    print(f"{day:<12} {int(d['installs']):>10,} ${d['cost']:>10,.2f} ${d['revenue']:>10,.2f} {roas}x")

# All apps ROAS
app_totals = {}
for app in data:
    t = {'cost': 0, 'revenue': 0, 'installs': 0}
    for country in data[app]:
        for day in data[app][country]:
            t['cost'] += data[app][country][day]['cost']
            t['revenue'] += data[app][country][day]['revenue']
            t['installs'] += data[app][country][day]['installs']
    app_totals[app] = t

print(f"\n{'App':<45} {'Cost':>14} {'Revenue':>14} {'ROAS':>8}")
print('-' * 85)
for app in sorted(app_totals, key=lambda x: app_totals[x]['cost'], reverse=True):
    t = app_totals[app]
    roas = round(t['revenue'] / t['cost'], 2) if t['cost'] > 0 else 0
    print(f"{app:<45} ${t['cost']:>12,.2f} ${t['revenue']:>12,.2f} {roas}x")

# Build output JSON
days = sorted(set(d for c in data[apl_key].values() for d in c.keys()))
top8 = [c for c, _ in top_countries[:8]]

output = {
    'date_period': dp,
    'apl389_name': apl_key,
    'apl389_by_country': [],
    'apl389_daily': [],
    'apl389_country_daily': {},
    'all_apps_roas': [],
    'days': days,
}

for country, t in top_countries:
    roas = round(t['revenue'] / t['cost'], 2) if t['cost'] > 0 else 0
    ecpi = round(t['cost'] / t['installs'], 3) if t['installs'] > 0 else 0
    output['apl389_by_country'].append({
        'country': country, 'installs': int(t['installs']),
        'cost': round(t['cost'], 2), 'revenue': round(t['revenue'], 2),
        'roas': roas, 'ecpi': ecpi,
    })

for day in sorted(apl_daily.keys()):
    d = apl_daily[day]
    roas = round(d['revenue'] / d['cost'], 2) if d['cost'] > 0 else 0
    output['apl389_daily'].append({
        'date': day, 'installs': int(d['installs']),
        'cost': round(d['cost'], 2), 'revenue': round(d['revenue'], 2), 'roas': roas,
    })

for c in top8:
    output['apl389_country_daily'][c] = []
    for day in days:
        v = data[apl_key][c].get(day, {'cost': 0, 'revenue': 0, 'installs': 0})
        roas = round(v['revenue'] / v['cost'], 2) if v['cost'] > 0 else 0
        output['apl389_country_daily'][c].append({
            'date': day, 'roas': roas,
            'cost': round(v['cost'], 2), 'revenue': round(v['revenue'], 2),
            'installs': int(v['installs']),
        })

for app in sorted(app_totals, key=lambda x: app_totals[x]['cost'], reverse=True):
    t = app_totals[app]
    roas = round(t['revenue'] / t['cost'], 2) if t['cost'] > 0 else 0
    output['all_apps_roas'].append({
        'app': app, 'installs': int(t['installs']),
        'cost': round(t['cost'], 2), 'revenue': round(t['revenue'], 2), 'roas': roas,
    })

out_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'roas_analysis.json')
with open(out_path, 'w') as f:
    json.dump(output, f, indent=2)
print(f"\nSaved to {out_path}")
