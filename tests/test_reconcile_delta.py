"""Regression tests for reconcile_complete_scans delta detection.

Bug (2026-09): reconcile required BOTH prior mode == 'full' AND current mode
== 'delta' to arm NEW/REOPENED detection, so a full-then-full run pair always
re-baselined and the alert df stayed empty forever (0 emails despite ~250 new
job rows/day landing in job_state).
"""
import importlib.util
import sqlite3
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
MODULE_PATH = HERE / "entry_level_cs_job_monitor_colab.py"


def load_module(tmp_db: str):
    try:
        import IPython  # noqa: F401
    except ImportError:
        ipy = types.ModuleType("IPython")
        disp = types.ModuleType("IPython.display")
        disp.display = lambda *a, **k: None
        ipy.display = disp
        sys.modules["IPython"] = ipy
        sys.modules["IPython.display"] = disp
    spec = importlib.util.spec_from_file_location("m", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["m"] = mod
    spec.loader.exec_module(mod)
    mod.AUTO_MOUNT_GOOGLE_DRIVE = False
    mod.STATE_DB_PATH = tmp_db
    return mod


def cand(job_key, title="Software Engineer Intern", location="San Jose, CA"):
    base = {
        "job_key": job_key,
        "company": "TestCo",
        "ats": "Greenhouse",
        "title": title,
        "location": location,
        "url": "https://example.com/jobs/1",
        "source_date": None,
        "date_basis": "api_first_published",
        "entry_evidence": "Early Career",
        "profile_rationale": "test",
        "profile_fit": "Good",
    }
    return dict(base)


def source(job_key):
    c = cand(job_key)
    c["profile_rationale"] = "test"
    return c


def scan_full(jobs):
    return {
        "company": "TestCo",
        "complete": True,
        "scan_mode": "full",
        "source_jobs": [source(k) for k in jobs],
        "candidates": [cand(k) for k in jobs],
        "health": None,
    }


def scan_delta(jobs):
    s = scan_full(jobs)
    s["scan_mode"] = "delta"
    return s


def make_env(tmp_path):
    mod = load_module(str(tmp_path / "state.sqlite3"))
    conn = mod.ensure_state_database()
    # Pretend TestCo is one of TARGETS so specs_by_company resolves.
    fake_spec = {"company": "TestCo", "kind": "greenhouse", "board": "testco"}
    mod.TARGETS.append(fake_spec)
    return mod, conn


def run(tmp_path):
    mod, conn = make_env(tmp_path)
    t0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(hours=2)
    t2 = t1 + timedelta(hours=2)

    # Run 1: baseline (no source_state row) -> nothing NEW, persisted.
    new, cur = mod.reconcile_complete_scans(conn, [scan_full(["a", "b"])], t0)
    assert new == [], f"baseline must be silent, got {[r['Monitor State'] for r in new]}"
    assert conn.execute("select count(*) from job_state").fetchone()[0] == 2

    # Run 2: full-then-full with one new job -> MUST alert NEW (old bug: empty).
    new, cur = mod.reconcile_complete_scans(conn, [scan_full(["a", "b", "c"])], t1)
    states = sorted((r["Position"], r["Monitor State"]) for r in new)
    assert states == [("Software Engineer Intern", "NEW")], f"full->full: {new}"
    assert new[0]["First Seen (UTC)"] == t2 or new[0]["First Seen (UTC)"] == t1
    # first_seen must come from job_state (t1 observed_at), not reset to t2:
    js = conn.execute("select first_seen_at from job_state where job_key like '%c'").fetchone()
    assert js and js[0].startswith("2026-09-01T14"), js

    # Run 3: full-then-delta with one new job -> NEW must still arm.
    new, cur = mod.reconcile_complete_scans(conn, [scan_delta(["a", "b", "c", "d"])], t2)
    assert len(new) == 1 and new[0]["Monitor State"] == "NEW", f"full->delta: {new}"

    # Run 4: delta-then-full -> re-baseline (current full sees all; prior delta
    # snapshot is not a full record, but NEW-vs-job_state is still sound: d now
    # known, so a brand-new id 'e' alerts NEW; no false REOPENED for a/b/c).
    new, cur = mod.reconcile_complete_scans(conn, [scan_full(["a", "b", "c", "d", "e"])], t2 + timedelta(hours=2))
    assert len(new) == 1 and new[0]["Monitor State"] == "NEW", f"delta->full: {new}"

    # Run 5: full-then-full, one job closed then reopened -> REOPENED.
    t4 = t2 + timedelta(hours=4)
    new, cur = mod.reconcile_complete_scans(conn, [scan_full(["a", "b", "c", "e"])], t4)  # d closed
    assert new == [], f"closing a job alerts nothing: {new}"
    t5 = t4 + timedelta(hours=2)
    new, cur = mod.reconcile_complete_scans(conn, [scan_full(["a", "b", "c", "d", "e"])], t5)
    assert len(new) == 1 and new[0]["Monitor State"] == "REOPENED", f"reopen: {new}"

    # Run 6: fingerprint mismatch (adapter changed) -> re-baseline, silent.
    conn.execute("update source_state set adapter_fingerprint='bogus'")
    conn.commit()
    new, cur = mod.reconcile_complete_scans(conn, [scan_full(["a", "b", "c", "d", "e"])], t5 + timedelta(hours=2))
    assert new == [], f"fingerprint mismatch must re-baseline: {new}"

    conn.close()
    print("ALL 6 SCENARIOS PASS")


if __name__ == "__main__":
    import tempfile
    from pathlib import Path as P

    with tempfile.TemporaryDirectory() as td:
        run(P(td))
