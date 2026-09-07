#!/usr/bin/env python3
"""Label NEW/REOPENED jobs by reading their descriptions, then classify.

Runs after run_monitor.py in Actions. Only touches jobs seen in the last
4 hours that lack a label (delta, not full batch — a few board fetches).
Writes an additive desc_label table in job_state.sqlite3; scan baselines
untouched. Never fails the workflow: all errors are logged, exit 0.

Usage: python3 label_new.py [--window-hours 4]
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from classify import classify  # noqa: E402

DB_PATH = HERE / "job_state.sqlite3"
WINDOW_DEFAULT_H = 4
PER_BOARD_SLEEP = 1.0
MAX_JOBS = 500


def get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jd-label-delta"})
    return urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")


def clean(raw: str) -> str:
    t = html.unescape(raw or "")
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def board_of(job_key: str, url: str):
    parts = (job_key or "").split(":")
    if len(parts) >= 3 and parts[0] in ("greenhouse", "gh", "lever", "ashby"):
        prov = "gh" if parts[0] in ("greenhouse", "gh") else parts[0]
        return prov, parts[1].lower(), parts[2]
    m = re.search(r"jobs\.lever\.co/([A-Za-z0-9\-]+)/([A-Za-z0-9\-]+)", url or "")
    if m:
        return "lever", m.group(1).lower(), m.group(2)
    m = re.search(r"jobs\.ashbyhq\.com/([A-Za-z0-9\-]+)/([A-Za-z0-9\-]+)", url or "")
    if m:
        return "ashby", m.group(1).lower(), m.group(2)
    m = re.search(r"greenhouse\.io/([A-Za-z0-9\-]+)", url or "")
    if m:
        jm = re.search(r"gh_jid=(\d+)", url or "")
        return "gh", m.group(1).lower(), jm.group(1) if jm else None
    return None


def fetch_board(kind: str, board: str):
    if kind == "gh":
        jobs = json.loads(get(f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true")).get("jobs", [])
        return {str(j.get("id")): (j.get("title", ""), clean(j.get("content", ""))) for j in jobs}
    if kind == "lever":
        jobs = json.loads(get(f"https://api.lever.co/v0/postings/{board}?mode=json"))
        jobs = jobs if isinstance(jobs, list) else []
        return {str(j.get("id")): (j.get("text", ""), clean(j.get("description", ""))) for j in jobs}
    jobs = json.loads(get(f"https://api.ashbyhq.com/posting-api/job-board/{board}")).get("jobs", [])
    return {str(j.get("id")): (j.get("title", ""), clean(j.get("descriptionHtml", ""))) for j in jobs}


def main() -> int:
    hours = float(sys.argv[sys.argv.index("--window-hours") + 1]) if "--window-hours" in sys.argv else WINDOW_DEFAULT_H
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS desc_label(job_key TEXT PRIMARY KEY, label TEXT, "
                     "conf REAL, reason TEXT, labeled_at TEXT DEFAULT (datetime('now')))")
        rows = conn.execute(
            "SELECT s.job_key, s.company, s.title, s.url FROM job_state s "
            "LEFT JOIN desc_label l ON l.job_key = s.job_key "
            "WHERE s.last_seen_at >= ? AND (l.job_key IS NULL OR l.label = 'CLOSED') "
            "AND (s.url LIKE '%greenhouse%' OR s.url LIKE '%lever.co%' OR s.url LIKE '%ashby%') "
            "LIMIT ?", (cutoff, MAX_JOBS)).fetchall()
    except Exception as e:
        print(f"[label] db read failed: {e}")
        return 0
    print(f"[label] {len(rows)} recent unlabeled fetchable jobs (window {hours}h)")
    groups: dict = {}
    skipped = 0
    for key, comp, title, url in rows:
        b = board_of(key, url)
        if b is None:
            skipped += 1
            continue
        groups.setdefault((b[0], b[1]), []).append((key, comp, title, url, b[2]))
    print(f"[label] {len(groups)} boards, {skipped} unparseable skipped")
    counts = {"ENTRY": 0, "NOT": 0, "CLOSED": 0}
    for (kind, board), recs in sorted(groups.items()):
        try:
            board_jobs = fetch_board(kind, board)
        except Exception as e:
            print(f"[label] {kind}/{board} fetch failed: {str(e)[:100]}")
            continue
        for key, comp, title, url, jid in recs:
            hit = board_jobs.get(str(jid)) if jid else None
            if hit is None:
                hit = next(((t, d) for t, d in board_jobs.values() if t == title), None)
                if hit is None:
                    counts["CLOSED"] += 1
                    conn.execute("INSERT OR REPLACE INTO desc_label(job_key,label,conf,reason) VALUES(?,?,?,?)",
                                 (key, "CLOSED", 1.0, "no longer on board"))
                    continue
            t, d = hit
            label, conf, why = classify(t, d)
            counts[label] += 1
            conn.execute("INSERT OR REPLACE INTO desc_label(job_key,label,conf,reason) VALUES(?,?,?,?)",
                         (key, label, conf, why))
        conn.commit()
        time.sleep(PER_BOARD_SLEEP)
    conn.commit()
    conn.close()
    print(f"[label] done: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
