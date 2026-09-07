#!/usr/bin/env python3
"""Headless runner for the entry-level CS job monitor.

Designed for GitHub Actions (or any cron host):
  - Loads entry_level_cs_job_monitor_colab.py without IPython/notebook deps.
  - Persists state to ./job_state.sqlite3 (committed back to the repo).
  - The static site reads an exported jobs.json (see export_site.py).
  - Always logs a source-health summary so failures are visible in Actions.

Usage:
  python3 run_monitor.py                 # normal scheduled run
  python3 run_monitor.py --selfcheck     # verify wiring, no network scan
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import datetime, timezone
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
# Alerts are OFF: the Supabase-backed site is the channel (browse + filters
# for all friends). This runner only scans, syncs, and logs counts.
# ---------------------------------------------------------------------------


def fmt_ts(value) -> str:
    import pandas as pd  # local import; guaranteed present after module load
    if value is None or pd.isna(value):
        return "-"
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


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
# Modes
# ---------------------------------------------------------------------------
def selfcheck(module) -> int:
    print(f"module loaded OK: {MODULE_PATH.name}")
    print(f"companies configured: {len(module.TARGETS)}")
    print(f"fast_delta: {getattr(module, 'FAST_DELTA_MODE', False)}")
    print(f"budget_s: {module.SOURCE_SCAN_BUDGET_SECONDS}")
    print(f"db_path: {module.STATE_DB_PATH}")
    half = os.environ.get("SCAN_HALF", "all").strip().upper()
    if half in ("A", "B"):
        assert set(t["company"] for t in module.TARGETS) <= set(module.EXPECTED_COMPANIES)
        assert 0 < len(module.TARGETS) <= len(module.EXPECTED_COMPANIES)
        print(f"SELFCHECK PASS (half {half})")
    else:
        assert len(module.TARGETS) == module.EXPECTED_COMPANY_COUNT
        print("SELFCHECK PASS")
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
        n_new = int((df["Monitor State"] == "NEW").sum())
        n_reopened = int((df["Monitor State"] == "REOPENED").sum())
        print(f"[site] {len(df)} new/reopened ({n_new} NEW / {n_reopened} REOPENED) — "
              f"see Supabase-backed site; no email sent.")
    else:
        print("[site] no new jobs.")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selfcheck", action="store_true", help="verify wiring, no network")
    args = parser.parse_args()

    module = load_monitor_module()
    apply_scan_half(module)

    if args.selfcheck:
        return selfcheck(module)
    return run_scan(module)


if __name__ == "__main__":
    raise SystemExit(main())
