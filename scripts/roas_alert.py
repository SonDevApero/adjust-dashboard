"""
ROAS Alert Script — Check today's ROAS and send email if below threshold.
Run via cron or manually: python3 scripts/roas_alert.py
"""

import os
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Load .env
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

ADJUST_TOKEN = os.getenv("ADJUST_TOKEN", "")
APL389_TOKEN = "p9aujhwyqvi8"
EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "")
EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")
EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD", "")
ROAS_THRESHOLD = float(os.getenv("ROAS_THRESHOLD", "100"))

REPORT_URL = "https://dash.adjust.com/control-center/reports-service/report"


def fetch_today_roas():
    """Fetch today's ROAS from Adjust API."""
    today = datetime.today().strftime("%Y-%m-%d")
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    headers = {"Authorization": f"Bearer {ADJUST_TOKEN}"}
    results = {}

    for label, dp in [("today", f"{today}:{today}"), ("yesterday", f"{yesterday}:{yesterday}")]:
        q = "&".join([
            f"app_token__in={APL389_TOKEN}",
            f"date_period={dp}",
            "dimensions=day",
            "metrics=installs,network_cost,revenue,revenue_total_d0,revenue_total_d7,ecpi_all",
            "attribution_source=first",
            "utc_offset=%2B07:00",
            "limit=10", "format=json",
        ])
        url = f"{REPORT_URL}?{q}"
        resp = httpx.get(url, headers=headers, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            rows = data.get("rows", [])
            totals = data.get("totals", {})
            r = totals if totals else (rows[0] if rows else {})
            if r:
                ncost = float(r.get("network_cost", 0))
                rev = float(r.get("revenue", 0))
                rd0 = float(r.get("revenue_total_d0", 0))
                rd7 = float(r.get("revenue_total_d7", 0))
                inst = float(r.get("installs", 0))
                results[label] = {
                    "date": dp.split(":")[0],
                    "installs": int(inst),
                    "cost": round(ncost, 2),
                    "revenue": round(rev, 2),
                    "rev_d0": round(rd0, 2),
                    "roas_d0": round(rd0 / ncost * 100, 2) if ncost > 0 else 0,
                    "roas_d7": round(rd7 / ncost * 100, 2) if ncost > 0 else 0,
                    "roas_all": round(rev / ncost * 100, 2) if ncost > 0 else 0,
                    "ecpi": float(r.get("ecpi_all", 0)),
                }

    return results


def send_alert_email(data):
    """Send ROAS alert email via Gmail SMTP."""
    today_data = data.get("today", {})
    yesterday_data = data.get("yesterday", {})

    roas_d0 = today_data.get("roas_d0", 0)
    roas_all = today_data.get("roas_all", 0)
    y_roas_d0 = yesterday_data.get("roas_d0", 0)

    # Determine alert level
    if roas_d0 < 50:
        level = "CRITICAL"
        color = "#ef4444"
    elif roas_d0 < 80:
        level = "WARNING"
        color = "#f59e0b"
    else:
        level = "ALERT"
        color = "#f97316"

    subject = f"[{level}] APL389 ROAS D0 = {roas_d0}% (threshold: {ROAS_THRESHOLD}%)"

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#1a1d2e;color:#e0e0e0;border-radius:12px;overflow:hidden">
        <div style="background:linear-gradient(135deg,{color},{color}cc);padding:20px;text-align:center">
            <h1 style="margin:0;color:#fff;font-size:20px">{level}: ROAS Below {ROAS_THRESHOLD}%</h1>
            <p style="margin:8px 0 0;color:rgba(255,255,255,.8);font-size:14px">APL389 - Photo Video Maker</p>
        </div>
        <div style="padding:24px">
            <h2 style="color:{color};font-size:36px;text-align:center;margin:0">ROAS D0: {roas_d0}%</h2>
            <p style="text-align:center;color:#6b7280;font-size:13px;margin:4px 0 20px">
                Threshold: {ROAS_THRESHOLD}% | Yesterday: {y_roas_d0}% | Change: {'+' if roas_d0 > y_roas_d0 else ''}{round(roas_d0 - y_roas_d0, 2)}%
            </p>

            <table style="width:100%;border-collapse:collapse;font-size:13px;color:#e0e0e0">
                <tr style="border-bottom:1px solid #252836">
                    <td style="padding:10px;color:#6b7280">Date</td>
                    <td style="padding:10px;font-weight:700">{today_data.get('date', '-')}</td>
                    <td style="padding:10px;color:#6b7280">{yesterday_data.get('date', '-')}</td>
                </tr>
                <tr style="border-bottom:1px solid #252836">
                    <td style="padding:10px;color:#6b7280">Installs</td>
                    <td style="padding:10px">{today_data.get('installs', 0):,}</td>
                    <td style="padding:10px">{yesterday_data.get('installs', 0):,}</td>
                </tr>
                <tr style="border-bottom:1px solid #252836">
                    <td style="padding:10px;color:#6b7280">Ad Spend (network)</td>
                    <td style="padding:10px">${today_data.get('cost', 0):,.2f}</td>
                    <td style="padding:10px">${yesterday_data.get('cost', 0):,.2f}</td>
                </tr>
                <tr style="border-bottom:1px solid #252836">
                    <td style="padding:10px;color:#6b7280">Revenue</td>
                    <td style="padding:10px">${today_data.get('revenue', 0):,.2f}</td>
                    <td style="padding:10px">${yesterday_data.get('revenue', 0):,.2f}</td>
                </tr>
                <tr style="border-bottom:1px solid #252836">
                    <td style="padding:10px;color:#6b7280">ROAS D0</td>
                    <td style="padding:10px;font-weight:700;color:{color}">{roas_d0}%</td>
                    <td style="padding:10px">{y_roas_d0}%</td>
                </tr>
                <tr style="border-bottom:1px solid #252836">
                    <td style="padding:10px;color:#6b7280">ROAS All</td>
                    <td style="padding:10px">{roas_all}%</td>
                    <td style="padding:10px">{yesterday_data.get('roas_all', 0)}%</td>
                </tr>
                <tr>
                    <td style="padding:10px;color:#6b7280">eCPI</td>
                    <td style="padding:10px">${today_data.get('ecpi', 0):.4f}</td>
                    <td style="padding:10px">${yesterday_data.get('ecpi', 0):.4f}</td>
                </tr>
            </table>

            <div style="margin-top:20px;padding:14px;background:rgba(239,68,68,.1);border-radius:8px;border-left:3px solid {color}">
                <strong style="color:{color}">Action Required:</strong>
                <p style="margin:6px 0 0;color:#c0c0c0;font-size:12px;line-height:1.6">
                    {'ROAS cuc ky thap. Can pause ngay cac campaign ROAS kem va kiem tra lai creative + bidding.' if roas_d0 < 50 else
                     'ROAS duoi nguong. Xem xet giam budget cac campaign kem hieu qua va toi uu creative.' if roas_d0 < 80 else
                     'ROAS gan nguong. Theo doi sat va chuan bi plan toi uu neu tiep tuc giam.'}
                </p>
            </div>

            <p style="text-align:center;margin-top:20px;font-size:11px;color:#4b5563">
                Sent by Terasofts Dashboard | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC+7
            </p>
        </div>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    # Send via Gmail SMTP
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())

    print(f"[ALERT] Email sent to {EMAIL_TO}: {subject}")


def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking ROAS for APL389...")

    data = fetch_today_roas()

    if not data.get("today"):
        print("[SKIP] No data for today yet.")
        return

    today = data["today"]
    roas_d0 = today.get("roas_d0", 0)
    print(f"  ROAS D0 today: {roas_d0}%")
    print(f"  Threshold: {ROAS_THRESHOLD}%")

    if roas_d0 < ROAS_THRESHOLD:
        print(f"  BELOW THRESHOLD! Sending alert...")
        try:
            send_alert_email(data)
            print(f"  Email sent successfully!")
        except Exception as e:
            print(f"  Email failed: {e}")
    else:
        print(f"  ROAS OK. No alert needed.")


if __name__ == "__main__":
    main()
