# Entry-Level CS Job Monitor — GitHub Actions Deployment

Scans 87 companies' career boards every 2 hours, diffs against the previous
successful scan, and emails you only NEW / REOPENED entry-level US roles.

## Setup (one time, ~10 minutes)

### 1. Create a private repo and push this folder

```bash
cd ~/Downloads/job-monitor-actions
git init -b main
git add .
git commit -m "job monitor: initial deployment"
gh repo create job-monitor --private --source=. --push
# (or create a private repo on github.com and push manually)
```

### 2. Add repository secrets

Repo → Settings → Secrets and variables → Actions → "New repository secret"

**Option A — Resend (recommended):**

| Secret | Value |
|---|---|
| `RESEND_API_KEY` | API key from resend.com → API Keys (`re_...`) |
| `ALERT_TO` | your email address |

Note: until you verify a domain at resend.com/domains, Resend only delivers to
the address you signed up with. That's fine for self-alerts. Free tier: 100
emails/day, 3,000/month.

**Option B — Gmail SMTP (fallback):**

| Secret | Value |
|---|---|
| `GMAIL_USER` | your Gmail address |
| `GMAIL_APP_PASSWORD` | 16-character app password (step 3) |
| `ALERT_TO` *(optional)* | where alerts go; defaults to GMAIL_USER |

If both are configured, Resend wins automatically.

### 3. Get a Gmail app password

1. Google Account → Security → enable **2-Step Verification** if not already on.
2. Go to myaccount.google.com/apppasswords
3. Create one named "job-monitor"; copy the 16-char code into the secret above.
   (Regular Gmail passwords do NOT work with SMTP.)

### 4. First run = baseline

Actions tab → **Job Monitor** → **Run workflow**.

The first run establishes the baseline: expect an empty alert set and a log line
like `new/reopened: 0`. That is correct behavior — from the next run onward you
get emailed whenever a matching role appears or reappears.

## What you'll get

- Every 2 hours: scan of all sources (~5–15 min runtime)
- An email ONLY when there are new/reopened roles, containing:
  company, title, location, entry evidence, direct apply link
- Source-health footer in every alert so partial scans are visible
- State persisted in `job_state.sqlite3`, committed back to the repo each run

## Tuning

| Want | Change in `.github/workflows/job-monitor.yml` |
|---|---|
| Hourly checks | `cron: "30 * * * *"` |
| Every 4 hours | `cron: "30 */4 * * *"` |
| Different weekday pattern | standard cron syntax |

| Want | Change in `run_monitor.py` / module |
|---|---|
| Faster big-board scans | lower `*_SHARD_WORKERS` constants in the monitor file |
| Gentler on rate limits | raise delays between requests (see RATE_LIMIT_* constants) |

## Free-tier budget math

GitHub free tier gives private repos 2,000 minutes/month.
At every-2h cadence: ~360 runs × ~8 min ≈ **2,900 min/mo → slightly over.**
Options that keep it comfortably free:

1. **Every 3 hours**: ~240 runs × ~8 min ≈ 1,900 min/mo ✓ fits
2. Keep every 2h but let Fast Delta shrink runs further (already active for
   Amazon/Microsoft/Eightfold/Apple) — most runs land well under 8 min
3. Public repo = unlimited Actions minutes (state DB would be public; fine for
   job listings, but consider privacy)

## Local manual runs (same script)

```bash
python3 run_monitor.py                # one-off scan + email
python3 run_monitor.py --selfcheck    # wiring check, no network
python3 run_monitor.py --send-test-email
```

## Troubleshooting

- **No email but jobs found?** Check secrets exist and app-password is valid;
  run `--send-test-email` to verify SMTP end-to-end.
- **Runs failing at checkout/push?** Ensure the workflow has `contents: write`
  permission (it does) and branch protection isn't blocking bot pushes.
- **Many PARTIAL sources?** Normal occasionally (rate limits); re-run later.
  PARTIAL never produces false alerts — see monitor's honesty contract.
# job-scanner
