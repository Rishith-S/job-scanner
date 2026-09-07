#!/usr/bin/env python3
"""Headless runner + email alerter for the entry-level CS job monitor.

Designed for GitHub Actions (or any cron host):
  - Loads entry_level_cs_job_monitor_colab.py without IPython/notebook deps.
  - Persists state to ./job_state.sqlite3 (committed back to the repo).
  - Emails NEW/REOPENED jobs when found (Gmail app-password secrets).
  - Always logs a source-health summary so failures are visible in Actions.

Usage:
  python3 run_monitor.py                 # normal scheduled run
  python3 run_monitor.py --selfcheck     # verify wiring, no network scan
  python3 run_monitor.py --send-test-email
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import smtplib
import sys
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from types import ModuleType

HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "job_state.sqlite3"
MODULE_PATH = HERE / "entry_level_cs_job_monitor_colab.py"

# ---------------------------------------------------------------------------
# Notebook-free import: stub IPython.display if the real package is absent.
# ---------------------------------------------------------------------------
def _ensure_notebook_free_imports() -> None:
    try:
        import IPython  # noqa: F401
        return
    except ImportError:
        pass
    ipy = ModuleType("IPython")
    disp = ModuleType("IPython.display")
    disp.display = lambda *args, **kwargs: None  # silent no-op
    ipy.display = disp
    sys.modules["IPython"] = ipy
    sys.modules["IPython.display"] = disp


def load_monitor_module():
    _ensure_notebook_free_imports()
    spec = importlib.util.spec_from_file_location("entry_level_cs_job_monitor", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["entry_level_cs_job_monitor"] = module
    spec.loader.exec_module(module)
    # Redirect persistence to the repo-local SQLite file.
    module.AUTO_MOUNT_GOOGLE_DRIVE = False
    module.STATE_DB_PATH = str(DB_PATH)
    return module


# ---------------------------------------------------------------------------
# Email delivery — Resend API preferred, Gmail SMTP fallback
# ---------------------------------------------------------------------------
def _send_via_resend(subject: str, text_body: str, html_body: str | None,
                     api_key: str, to_addr: str) -> None:
    import json as _json
    import urllib.request

    payload = {
        "from": os.environ.get("RESEND_FROM", "Job Monitor <onboarding@resend.dev>"),
        "to": [to_addr],
        "subject": subject,
        "text": text_body,
    }
    if html_body:
        payload["html"] = html_body
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=_json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status not in (200, 201):
            raise RuntimeError(f"Resend returned HTTP {resp.status}")
    print(f"[email] sent via Resend to {to_addr}")


def _send_via_gmail(subject: str, text_body: str, html_body: str | None,
                    user: str, password: str, to_addr: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addr
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    if html_body:
        msg.attach(MIMEText(html_body, "html", "utf-8"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
        server.login(user, password)
        server.sendmail(user, [to_addr], msg.as_string())
    print(f"[email] sent via Gmail SMTP to {to_addr}")


def send_email(subject: str, text_body: str, html_body: str | None = None) -> None:
    """Deliver via RESEND_API_KEY when present, else GMAIL_USER/GMAIL_APP_PASSWORD."""
    resend_key = os.environ.get("RESEND_API_KEY", "").strip()
    gmail_user = os.environ.get("GMAIL_USER", "").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "").strip()

    if resend_key:
        # Unverified-domain accounts may only send to the Resend signup email;
        # ALERT_TO overrides this after verifying a domain at resend.com/domains.
        to_addr = os.environ.get("ALERT_TO", "").strip()
        if not to_addr:
            print("[email] RESEND_API_KEY set but ALERT_TO empty; skipping.")
            return
        _send_via_resend(subject, text_body, html_body, resend_key, to_addr)
        return

    if gmail_user and gmail_pass:
        to_addr = os.environ.get("ALERT_TO", "").strip() or gmail_user
        _send_via_gmail(subject, text_body, html_body, gmail_user, gmail_pass, to_addr)
        return

    print("[email] no mail credentials found (set RESEND_API_KEY+ALERT_TO or GMAIL_USER/GMAIL_APP_PASSWORD); skipping.")


def fmt_ts(value) -> str:
    import pandas as pd  # local import; guaranteed present after module load
    if value is None or pd.isna(value):
        return "-"
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def jobs_text_table(df) -> str:
    lines = []
    for _, row in df.iterrows():
        lines.append(
            f"* {row['Company']} — {row['Position']}\n"
            f"    {row['Location']} | {row['Entry Evidence']} | {row['Profile Fit']}\n"
            f"    first seen: {fmt_ts(row['First Seen (UTC)'])} | state: {row['Monitor State']}\n"
            f"    apply: {row['URL']}"
        )
    return "\n".join(lines)


def jobs_html_table(df) -> str:
    rows = []
    for _, row in df.iterrows():
        url = row["URL"]
        link = f'<a href="{url}">apply</a>' if isinstance(url, str) and url.startswith("http") else (url or "-")
        rows.append(
            "<tr>"
            f"<td><b>{row['Company']}</b></td>"
            f"<td>{row['Position']}</td>"
            f"<td>{row['Location']}</td>"
            f"<td>{row['Entry Evidence']}</td>"
            f"<td>{row['Monitor State']}</td>"
            f"<td>{link}</td>"
            "</tr>"
        )
    style = (
        "body{font-family:-apple-system,Segoe UI,Arial,sans-serif;font-size:14px;color:#222}"
        "table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ddd;padding:6px 8px;text-align:left;vertical-align:top}"
        "th{background:#f5f5f5}"
    )
    header = "<tr><th>Company</th><th>Position</th><th>Location</th><th>Entry evidence</th><th>State</th><th>Link</th></tr>"
    return f"<html><head><style>{style}</style></head><body><h3>New / reopened roles</h3><table>{header}{''.join(rows)}</table></body></html>"


def health_summary(health_df) -> str:
    counts = health_df["Scan Status"].value_counts().to_dict()
    parts = [f"{status}: {count}" for status, count in sorted(counts.items())]
    problems = health_df[health_df["Scan Status"].isin(["PARTIAL", "TIMEOUT", "UNAVAILABLE", "PARSE_ERROR", "WORKER_ERROR", "NOT_POLLED"])]
    lines = ["Source health — " + " | ".join(parts) if parts else "Source health — empty"]
    for _, row in problems.iterrows():
        detail = str(row["Detail"])[:140]
        lines.append(f"  ! {row['Company']} [{row['Scan Status']}] {detail}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# A/B halves: alternate companies across runs so each GitHub run hammers
# half the boards (~4 h cadence per company — still fine since grad reqs stay
# open for days). Companies skipped in a run keep their baseline untouched:
# reconcile only touches companies it actually scanned.
# ---------------------------------------------------------------------------
def apply_scan_half(module) -> None:
    half = os.environ.get("SCAN_HALF", "all").strip().upper()
    if half not in ("A", "B"):
        return
    names = sorted(t["company"] for t in module.TARGETS)
    mid = (len(names) + 1) // 2
    keep = set(names[:mid] if half == "A" else names[mid:])
    module.TARGETS = [t for t in module.TARGETS if t["company"] in keep]
    print(f"[halves] SCAN_HALF={half}: scanning {len(module.TARGETS)}/{len(names)} companies")


# ---------------------------------------------------------------------------
# Account slices: three accounts (friends) each own a fixed third of the list
# via SLICE=1/2/3, sharing one Supabase DB. Combined with SCAN_HALF above,
# each run scans a sixth (~20 companies, ~4 min). Slices are disjoint, and
# per-company baselines live in the shared DB, so a company moving slices
# after a future add keeps its history. Unset SLICE = whole list (local use).
# ---------------------------------------------------------------------------
def apply_slice(module) -> None:
    slic = os.environ.get("SLICE", "all").strip()
    if slic not in ("1", "2", "3"):
        return
    names = sorted(t["company"] for t in module.TARGETS)
    size = (len(names) + 2) // 3
    keep = set(names[(int(slic) - 1) * size:int(slic) * size])
    module.TARGETS = [t for t in module.TARGETS if t["company"] in keep]
    print(f"[slices] SLICE={slic}/3: keeping {len(module.TARGETS)}/{len(names)} companies")


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------
def selfcheck(module) -> int:
    print(f"module loaded OK: {MODULE_PATH.name}")
    print(f"companies configured: {len(module.TARGETS)}")
    print(f"fast_delta: {getattr(module, 'FAST_DELTA_MODE', False)}")
    print(f"budget_s: {module.SOURCE_SCAN_BUDGET_SECONDS}")
    print(f"db_path: {module.STATE_DB_PATH}")
    half = os.environ.get("SCAN_HALF", "all").strip().upper()
    slic = os.environ.get("SLICE", "all").strip()
    if half in ("A", "B") or slic in ("1", "2", "3"):
        assert set(t["company"] for t in module.TARGETS) <= set(module.EXPECTED_COMPANIES)
        assert 0 < len(module.TARGETS) <= len(module.EXPECTED_COMPANIES)
        print(f"SELFCHECK PASS (slice {slic} half {half})")
    else:
        assert len(module.TARGETS) == module.EXPECTED_COMPANY_COUNT
        print("SELFCHECK PASS")
    return 0


def send_test_email() -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    send_email(
        "[Job Monitor] test email",
        f"This is a test of your job monitor alerting pipeline.\nSent: {stamp}",
        f"<p>This is a <b>test</b> of your job monitor alerting pipeline.<br>Sent: {stamp}</p>",
    )
    print("TEST EMAIL SENT")
    return 0


def run_scan(module) -> int:
    started = datetime.now(timezone.utc)
    print(f"[monitor] scan start {started.isoformat()}")

    df, current_df, health_df = module.run()

    finished = datetime.now(timezone.utc)
    duration_min = (finished - started).total_seconds() / 60.0
    print(f"\n[monitor] scan finished in {duration_min:.1f} min | "
          f"new/reopened: {len(df)} | currently eligible: {len(current_df)}")

    print("\n" + health_summary(health_df))

    if not df.empty:
        stamp = finished.strftime("%Y-%m-%d %H:%M UTC")
        n_new = int((df["Monitor State"] == "NEW").sum())
        n_reopened = int((df["Monitor State"] == "REOPENED").sum())
        slic = os.environ.get("SLICE", "").strip()
        tag = f" slice {slic}/3" if slic in ("1", "2", "3") else ""
        subject = f"[Job Monitor{tag}] {len(df)} new role(s) — {stamp}"
        text_body = (
            f"{n_new} NEW / {n_reopened} REOPENED role(s) since last successful scan.\n\n"
            f"{jobs_text_table(df)}\n\n"
            f"{health_summary(health_df)}\n"
        )
        html_body = (
            jobs_html_table(df)
            + "<hr><pre style='font-size:11px;color:#666'>"
            + health_summary(health_df).replace("<", "&lt;")
            + "</pre>"
        )
        send_email(subject, text_body, html_body)
    else:
        print("[email] no new jobs; no email sent.")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selfcheck", action="store_true", help="verify wiring, no network")
    parser.add_argument("--send-test-email", action="store_true", help="verify SMTP secrets")
    args = parser.parse_args()

    module = load_monitor_module()
    apply_slice(module)
    apply_scan_half(module)

    if args.selfcheck:
        return selfcheck(module)
    if args.send_test_email:
        return send_test_email()
    return run_scan(module)


if __name__ == "__main__":
    raise SystemExit(main())
