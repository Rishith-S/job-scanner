#!/usr/bin/env python3
"""Export currently-listed jobs from job_state.sqlite3 to site/jobs.json.

Same honesty rules as the scanner: a job is listed only when its last_seen_at
is at least its company's last_completed_at (seen in the latest complete
snapshot), and eligibility (technical title, entry-level, US-only) is
recomputed with the monitor's own functions so stale labels can't linger.

Usage:
  python3 export_site.py            # writes site/jobs.json + site/meta.json

No network, no secrets, deterministic. Safe to run anywhere.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "job_state.sqlite3"
SITE_DIR = HERE / "site"


def _parse_ts(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> int:
    sys.path.insert(0, str(HERE))
    import run_monitor as _rm

    module = _rm.load_monitor_module()
    conn = sqlite3.connect(str(DB_PATH))
    try:
        completed = {
            row[0]: row[1]
            for row in conn.execute("SELECT company, last_completed_at FROM source_state")
        }
        jobs, per_company = [], {}
        for company, stamp in completed.items():
            baseline = _parse_ts(stamp)
            count = 0
            for (key, title, loc, url, first, last) in conn.execute(
                "SELECT job_key, title, location, url,"
                " first_seen_at, last_seen_at FROM job_state WHERE company = ?",
                (company,),
            ):
                seen = _parse_ts(last)
                if baseline is not None and (seen is None or seen < baseline):
                    continue
                ev = module.entry_evidence(title or "")
                if ev is None or not module.is_us_only_location(loc or ""):
                    continue
                label, _rationale = module.profile_fit(title or "")
                jobs.append({
                    "company": company, "title": title or "",
                    "location": loc or "", "url": url or "",
                    "entry_evidence": ev, "profile_fit": label,
                    "first_seen": (first or "")[:10],
                })
                count += 1
            per_company[company] = {"last_completed": (stamp or "")[:16], "listed": count}
    finally:
        conn.close()

    jobs.sort(key=lambda j: (j["first_seen"], j["company"]), reverse=True)
    SITE_DIR.mkdir(exist_ok=True)
    (SITE_DIR / "jobs.json").write_text(json.dumps(jobs))
    meta = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "companies": len(per_company),
        "listed": len(jobs),
        "per_company": per_company,
    }
    (SITE_DIR / "meta.json").write_text(json.dumps(meta))
    size_kb = (SITE_DIR / "jobs.json").stat().st_size // 1024
    print("[export] companies={0} listed={1} jobs.json={2}KB".format(
        len(per_company), len(jobs), size_kb))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
