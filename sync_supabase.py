#!/usr/bin/env python3
"""Sync this run's results to the shared Supabase DB (the friends' site backend).

Reads run_manifest.json (written by run_monitor.py: who was scanned, status
each) plus job_state.sqlite3 (the persisted snapshots). Upserts companies +
currently-listed jobs; deletes remotely-stored jobs a COMPLETE re-scan no
longer shows (closed roles disappear honestly instead of lingering).

Usage:
  python3 sync_supabase.py            # normal: needs manifest + secrets
  python3 sync_supabase.py --dry-run  # no network: builds payloads from the
                                      # local DB and prints counts + samples
  python3 sync_supabase.py --backfill # one-time: full load without a manifest

Env (Actions secrets, never in repo/chat or logs):
  SUPABASE_URL, SUPABASE_SERVICE_KEY (service_role — bypasses RLS for writes)

A job counts as currently listed only when its last_seen_at is at least its
company's last_completed_at (seen in the latest complete snapshot).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "job_state.sqlite3"
MANIFEST_PATH = HERE / "run_manifest.json"
BATCH = 500


def _client():
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL / SUPABASE_SERVICE_KEY missing; refusing (values never logged).")
    s = requests.Session()
    s.headers.update(
        {"apikey": key, "Authorization": "Bearer {0}".format(key), "Content-Type": "application/json"}
    )
    return url, s


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


def _load_manifest():
    try:
        return json.loads(MANIFEST_PATH.read_text())
    except FileNotFoundError:
        raise SystemExit("run_manifest.json missing — run_monitor.py must run first.")


def _targets_by_company():
    """Fallback portal URLs + ATS names from the monitor config (local, safe)."""
    sys.path.insert(0, str(HERE))
    import run_monitor

    module = run_monitor.load_monitor_module()
    out = {}
    for t in module.TARGETS:
        out[t["company"]] = {
            "ats": t.get("ats", ""),
            "portal": t.get("fallback_url", "") if t.get("kind") == "fallback" else "",
            "fallback": t.get("kind") == "fallback",
        }
    return out


def build_payloads(manifest, conn, targets):
    """Return (company_rows, job_rows, complete_companies) with zero network."""
    source_completed = {
        row[0]: row[1]
        for row in conn.execute("SELECT company, last_completed_at FROM source_state")
    }
    entries = manifest["entries"] if manifest else [
        {"company": c, "ats": "", "status": "COMPLETE", "detail": "",
         "portal": "", "scanned_at": ts or ""}
        for c, ts in source_completed.items()
    ]
    slic, half = (manifest or {}).get("slice", "all"), (manifest or {}).get("half", "all")

    company_rows, complete = [], []
    for e in entries:
        c = e["company"]
        status = e.get("status", "")
        info = targets.get(c, {})
        row = {
            "company": c,
            "ats": e.get("ats") or info.get("ats", ""),
            "scan_status": status,
            "detail": e.get("detail", ""),
            "portal": e.get("portal") or info.get("portal", ""),
            "slice": slic,
            "half": half,
        }
        if status == "COMPLETE" and c in source_completed:
            row["last_completed_at"] = source_completed[c]
            complete.append(c)
        company_rows.append(row)

    # Eligibility is recomputed with the monitor's own rules (same functions
    # the scan uses) so title/location edits since first-seen can't linger.
    sys.path.insert(0, str(HERE))
    import run_monitor as _rm

    _mod = _rm.load_monitor_module()
    _evidence, _us_only, _fit = (_mod.entry_evidence, _mod.is_us_only_location,
                                 _mod.profile_fit)

    job_rows = []
    for c in complete:
        baseline = _parse_ts(source_completed[c])
        for (key, title, loc, url, first, last) in conn.execute(
            "SELECT job_key, title, location, url,"
            " first_seen_at, last_seen_at FROM job_state WHERE company = ?",
            (c,),
        ):
            seen = _parse_ts(last)
            if baseline is not None and (seen is None or seen < baseline):
                continue  # not in the latest complete snapshot = closed, skip
            ev = _evidence(title or "")
            if ev is None or not _us_only(loc or ""):
                continue  # not eligible right now, same rule as the scan
            label, _rationale = _fit(title or "")
            job_rows.append({
                "job_key": key, "company": c, "title": title or "",
                "location": loc or "", "url": url or "",
                "entry_evidence": ev, "profile_fit": label,
                "first_seen_at": first, "last_seen_at": last,
            })
    return company_rows, job_rows, complete


def _upsert(url, s, table, rows, conflict):
    for i in range(0, len(rows), BATCH):
        r = s.post("{0}/rest/v1/{1}".format(url, table),
                   params={"on_conflict": conflict},
                   headers={"Prefer": "resolution=merge-duplicates"},
                   json=rows[i:i + BATCH], timeout=60)
        if r.status_code not in (200, 201, 204):
            raise SystemExit("Supabase upsert {0} failed HTTP {1}: {2}".format(
                table, r.status_code, r.text[:300]))


def _delete_stale(url, s, company_rows_unused, job_rows, complete, dry_run_keys=None):
    """Delete remote jobs of COMPLETE companies that the fresh scan lacks."""
    fresh_by_company = {}
    for j in job_rows:
        fresh_by_company.setdefault(j["company"], set()).add(j["job_key"])
    deleted = 0
    for c in complete:
        r = s.get("{0}/rest/v1/jobs".format(url),
                  params={"company": "eq.{0}".format(c), "select": "job_key"},
                  timeout=60)
        if r.status_code != 200:
            raise SystemExit("Supabase read-back failed HTTP {0}: {1}".format(
                r.status_code, r.text[:200]))
        stale = [row["job_key"] for row in r.json()
                 if row["job_key"] not in fresh_by_company.get(c, set())]
        for key in stale:
            d = s.delete("{0}/rest/v1/jobs".format(url),
                         params={"job_key": "eq.{0}".format(key)}, timeout=60)
            if d.status_code not in (200, 204):
                raise SystemExit("Supabase delete failed HTTP {0}: {1}".format(
                    d.status_code, d.text[:200]))
            deleted += 1
    return deleted


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--backfill", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    try:
        targets = _targets_by_company()
        if args.dry_run or args.backfill:
            manifest = None
        else:
            manifest = _load_manifest()
        company_rows, job_rows, complete = build_payloads(manifest, conn, targets)
    finally:
        conn.close()

    mode = "dry-run" if args.dry_run else ("backfill" if args.backfill else "sync")
    print("[supabase:{0}] companies={1} jobs={2} complete_this_run={3}".format(
        mode, len(company_rows), len(job_rows), len(complete)))
    for j in job_rows[:3]:
        print("  sample: {0} — {1} | {2}".format(j["company"], j["title"][:60], j["location"][:40]))
    if args.dry_run:
        print("[supabase:dry-run] no network touched.")
        return 0

    url, s = _client()
    _upsert(url, s, "companies", company_rows, "company")
    _upsert(url, s, "jobs", job_rows, "job_key")
    deleted = _delete_stale(url, s, company_rows, job_rows, complete)
    print("[supabase:sync] upserted {0} companies + {1} jobs, deleted {2} closed.".format(
        len(company_rows), len(job_rows), deleted))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
