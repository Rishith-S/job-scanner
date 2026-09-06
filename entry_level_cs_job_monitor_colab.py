"""Persistent US entry-level technical-job monitor for Google Colab.

This is a replacement for the one-row-per-company sampler.  It has three
separate outputs:

1. ``df``: jobs newly observed since the previous *successful complete* scan.
2. ``current_jobs_df``: every eligible job observed in this scan (optional).
3. ``source_health_df``: every company's scan status; failures are never shown
   as fake "No Active" job listings.

The monitor stores a small SQLite database in Google Drive.  That is necessary
to identify jobs that appeared between runs and does not create CSV or JSON
files.  The first complete scan establishes a baseline, so it correctly emits
zero "new" rows while retaining the current-job table for immediate use.

Important: ``First Seen (UTC)`` means first observed by this monitor, not the
employer's authoritative opening timestamp. Every stable source job ID is
persisted before role filtering. That prevents a pre-existing job whose title
or location changes from being misreported as a new opening. A blocked or
partial scan never advances its baseline and cannot generate a new-job claim.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from threading import BoundedSemaphore
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from IPython.display import display


# ---------------------------------------------------------------------------
# Runtime and persistence controls
# ---------------------------------------------------------------------------
MAX_WORKERS = 20
PER_REQUEST_TIMEOUT_SECONDS = 8.0
# Adapter-specific query fan-out is bounded by this shared semaphore, so
# Amazon/Microsoft shards cannot stampede an ATS beyond the requested 18–20
# concurrent request envelope.
MAX_IN_FLIGHT_REQUESTS = 20
REQUEST_SLOTS = BoundedSemaphore(MAX_IN_FLIGHT_REQUESTS)
# A complete source scan may take longer than ten seconds.  The previous global
# seven-second deadline made the output incomplete by construction.  Large
# Workday/Eightfold boards need several minutes even with sharded pools.
SOURCE_SCAN_BUDGET_SECONDS = 420.0
# Transient-error and rate-limit retry policy.  ATS rate limiters (observed as
# 429s deep into Workday CXS / Eightfold PCSX pagination) require a real pause;
# a half-second blip just burns the retry.
MAX_HTTP_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 0.5
RATE_LIMIT_BACKOFF_SECONDS = 8.0

# ---------------------------------------------------------------------------
# Fast delta mode
# ---------------------------------------------------------------------------
# The user's workflow only cares about jobs opened between runs. For feeds
# whose public search is newest-first (verified live: Amazon ``sort=recent``,
# Apple native date order, Microsoft/Eightfold ``sort_by=timestamp``), each
# shard stops paginating once every row on a page predates the last completed
# scan minus a safety window, instead of downloading the full backlog every
# run. Feeds without a sort contract (Greenhouse/Ashby/Lever single requests,
# Workday CXS, HTML boards) always scan fully. A delta scan is persisted with
# last_scan_mode='delta'; REOPENED detection requires the PRIOR scan to have
# been a full scan, because absence from a delta snapshot means "not fetched,"
# not "posting closed."
FAST_DELTA_MODE = True
DELTA_SAFETY_WINDOW_HOURS = 24.0
# Feeds whose public search is verified newest-first, so stopping early at a
# date cutoff cannot miss newer postings deeper in the result set.
DELTA_SORTED_KINDS = frozenset({"amazon", "microsoft", "eightfold", "apple_html"})
# Workday scans the same ~40 title shards as Amazon/Microsoft/Eightfold, but
# serially in the original version: at a shared 20-slot request semaphore and
# an 8 s per-request timeout that cannot finish inside any sane budget. Give it
# the same bounded inner shard pool as the other sharded adapters.
WORKDAY_SHARD_WORKERS = 3

# Workday CXS and Amazon both cap their broad, unfiltered public searches.
# The monitor therefore exhausts a fixed, title-policy search scope instead of
# claiming that a truncated all-jobs result is a complete source snapshot.
WORKDAY_PAGE_SIZE = 20
WORKDAY_MAX_PAGES_PER_QUERY = 101  # page 101 proves/reveals a 2,000-result CXS cap
WORKDAY_CXS_RESULT_WINDOW = 2_000
AMAZON_PAGE_SIZE = 100
AMAZON_MAX_PAGES_PER_QUERY = 101  # page 101 reveals Amazon's 10,000-result cap
AMAZON_RESULT_WINDOW = 10_000
# Amazon is one source among 20 outer workers. Three independent title shards
# keep the combined request fan-out at roughly 20 while avoiding a one-minute
# serial Amazon scan.
AMAZON_SHARD_WORKERS = 3
MICROSOFT_PAGE_SIZE = 10
MICROSOFT_MAX_PAGES_PER_QUERY = 501
MICROSOFT_SHARD_WORKERS = 3
EIGHTFOLD_PAGE_SIZE = 10
EIGHTFOLD_MAX_PAGES_PER_QUERY = 501
EIGHTFOLD_SHARD_WORKERS = 3

# Every literal accepted by TECHNICAL_RE is represented below.  These are
# *source-query shards*, not profile preferences: general, flat SWE roles are
# deliberately included.  A Workday/Amazon source is complete only when every
# shard reaches a real terminal page without hitting its provider's result cap.
ROLE_POLICY_QUERY_VERSION = "2026-08-24-title-scope-v2"
ROLE_POLICY_QUERY_SHARDS = (
    "Software Engineer", "Software Engineering", "Software Development Engineer", "Software Developer", "SWE", "SDE",
    "Machine Learning Engineer", "ML Engineer", "AI Engineer", "Artificial Intelligence Engineer",
    "Machine Learning AI Engineer", "AI Machine Learning Engineer", "ML AI Engineer", "AI ML Engineer",
    "Data Engineer", "Backend", "Back End", "Frontend", "Front End", "Full Stack", "Fullstack",
    "Infrastructure Engineer", "Platform Engineer", "Cloud Engineer", "System Engineer", "Systems Engineer",
    "Site Reliability Engineer", "SRE", "Production Engineer", "DevOps Engineer",
    "Developer Productivity Engineer", "Developer Experience Engineer", "MLOps Engineer",
    "Embedded", "Firmware", "Research Engineer", "Research Intern", "Computer Science Intern",
    "Computer Science Co-op", "Computer Vision Engineer", "Robotics Engineer", "Robotics Intern",
)
# Public HTML boards are only accepted when their declared pagination can be
# fully traversed.  A changed/unknown pagination shape becomes PARTIAL.
PUBLIC_HTML_MAX_PAGES = 50
PHENOM_MAX_PAGES = 100
GOOGLE_PAGE_SIZE = 20
GOOGLE_MAX_PAGES = 200
APPLE_MAX_PAGES = 300

# A durable state store is required for "jobs opened between runs."  Colab's
# local disk is ephemeral, so the default uses Google Drive and a SQLite file.
AUTO_MOUNT_GOOGLE_DRIVE = True
GOOGLE_DRIVE_MOUNT_POINT = "/content/drive"
STATE_DB_PATH = "/content/drive/MyDrive/entry_level_job_monitor/job_state.sqlite3"

# ``current_jobs_df`` is useful on the first baseline run.  Leave it enabled
# when you do not want to hide currently open eligible jobs.
SHOW_CURRENT_MATCHES = True

# Bump this whenever identity/delta semantics change. Existing state from an
# earlier scraper is then re-baselined once instead of creating false alerts.
STATE_PROTOCOL_VERSION = "2026-08-24-role-scoped-source-state-v2"

UTC = timezone.utc
RUN_STARTED_AT = datetime.now(UTC)
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    # Some Colab/urllib3 combinations advertise zstd but cannot reliably decode
    # Amazon's zstd response.  Request universally supported encodings instead.
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (compatible; EntryLevelJobMonitor/2.0)",
}


# ---------------------------------------------------------------------------
# Target configuration - 87 supplied companies
# ---------------------------------------------------------------------------
def greenhouse(company: str, board: str, fallback_url: str, ats: str = "Greenhouse") -> Dict[str, Any]:
    return {"company": company, "kind": "greenhouse", "ats": ats, "board": board, "fallback_url": fallback_url}


def ashby(company: str, board: str, fallback_url: str, ats: str = "Ashby") -> Dict[str, Any]:
    return {"company": company, "kind": "ashby", "ats": ats, "board": board, "fallback_url": fallback_url}


def lever(company: str, board: str, fallback_url: str, ats: str = "Lever") -> Dict[str, Any]:
    return {"company": company, "kind": "lever", "ats": ats, "board": board, "fallback_url": fallback_url}


def workday(company: str, host: str, tenant: str, site: str, fallback_url: Optional[str] = None) -> Dict[str, Any]:
    return {
        "company": company,
        "kind": "workday",
        "ats": "Workday CXS",
        "host": host,
        "tenant": tenant,
        "site": site,
        "fallback_url": fallback_url or f"https://{host}/en-US/{site}",
    }


def fallback(company: str, fallback_url: str, ats: str = "Enterprise portal / fallback") -> Dict[str, Any]:
    return {"company": company, "kind": "fallback", "ats": ats, "fallback_url": fallback_url}


def amazon(company: str, fallback_url: str) -> Dict[str, Any]:
    return {"company": company, "kind": "amazon", "ats": "Amazon Jobs API", "fallback_url": fallback_url}


def microsoft(company: str, fallback_url: str) -> Dict[str, Any]:
    """Microsoft's current public Eightfold/PCSX job-search contract."""
    return {
        "company": company,
        "kind": "microsoft",
        "ats": "Microsoft public careers API",
        "domain": "microsoft.com",
        "fallback_url": fallback_url,
    }


def eightfold(company: str, host: str, domain: str, fallback_url: str) -> Dict[str, Any]:
    """A verified public Eightfold/PCSX career-search endpoint."""
    return {
        "company": company,
        "kind": "eightfold",
        "ats": "Eightfold public careers API",
        "host": host,
        "domain": domain,
        "fallback_url": fallback_url,
    }


def phenom_html(company: str, fallback_url: str) -> Dict[str, Any]:
    """A Phenom board that exposes its paginated job data in first-party HTML."""
    return {
        "company": company,
        "kind": "phenom_html",
        "ats": "Phenom server-rendered careers board",
        "fallback_url": fallback_url,
    }


def google_html(company: str, fallback_url: str) -> Dict[str, Any]:
    """Google Careers' publicly server-rendered, paginated US result board."""
    return {
        "company": company,
        "kind": "google_html",
        "ats": "Google Careers public board",
        "fallback_url": fallback_url,
    }


def apple_html(company: str, fallback_url: str) -> Dict[str, Any]:
    """Apple Jobs' public server-rendered US search results."""
    return {
        "company": company,
        "kind": "apple_html",
        "ats": "Apple Jobs public board",
        "fallback_url": fallback_url,
    }


def public_html(company: str, kind: str, fallback_url: str, ats: str) -> Dict[str, Any]:
    """Use a company-owned, server-rendered jobs page with a tested parser."""
    return {"company": company, "kind": kind, "ats": ats, "fallback_url": fallback_url}


TARGETS: List[Dict[str, Any]] = [
    greenhouse("Anthropic", "anthropic", "https://www.anthropic.com/careers"),
    greenhouse("Scale AI", "scaleai", "https://scale.com/careers"),
    greenhouse("Stripe", "stripe", "https://stripe.com/jobs/search"),
    greenhouse("Figma", "figma", "https://www.figma.com/careers/"),
    # Verified from Notion's public careers page: jobs.ashbyhq.com/notion
    ashby("Notion", "notion", "https://www.notion.com/careers", "Ashby (current public careers board)"),
    greenhouse("Datadog", "datadog", "https://www.datadoghq.com/careers/"),
    greenhouse("Cloudflare", "cloudflare", "https://www.cloudflare.com/careers/jobs/"),
    # Verified from Applied Intuition's public careers page: jobs.ashbyhq.com/applied
    ashby("Applied Intuition", "applied", "https://www.appliedintuition.com/careers", "Ashby (current public careers board)"),
    greenhouse("Waymo", "waymo", "https://waymo.com/careers/"),
    greenhouse("Rubrik", "rubrik", "https://www.rubrik.com/company/careers"),
    greenhouse("Duolingo", "duolingo", "https://careers.duolingo.com/"),
    greenhouse("Affirm", "affirm", "https://www.affirm.com/careers"),
    greenhouse("Reddit", "reddit", "https://www.redditinc.com/careers"),
    greenhouse("Pinterest", "pinterest", "https://www.pinterestcareers.com/"),
    greenhouse("Roblox", "roblox", "https://careers.roblox.com/"),
    greenhouse("Dropbox", "dropbox", "https://jobs.dropbox.com/"),
    greenhouse("Box", "boxinc", "https://careers.box.com/"),
    greenhouse("Twilio", "twilio", "https://www.twilio.com/en-us/company/jobs"),
    greenhouse("Elastic", "elastic", "https://www.elastic.co/careers"),
    # HubSpot's former Greenhouse board now returns 404. Keep its live,
    # first-party board visible instead of emitting a fabricated empty scan.
    fallback("HubSpot", "https://www.hubspot.com/careers/jobs", "Enterprise portal / public web fallback"),
    greenhouse("Asana", "asana", "https://asana.com/jobs"),
    greenhouse("Samsara", "samsara", "https://www.samsara.com/company/careers"),
    greenhouse("Verkada", "verkada", "https://www.verkada.com/careers/"),
    # Verified from ServiceTitan's public careers page.
    workday("ServiceTitan", "servicetitan.wd1.myworkdayjobs.com", "servicetitan", "ServiceTitan", "https://www.servicetitan.com/careers"),
    # The current public page is browser-rendered/rate-limited to plain HTTP;
    # retain the first-party link rather than pretend it was fully scanned.
    fallback("Confluent", "https://careers.confluent.io/jobs", "Enterprise portal / public web fallback"),
    greenhouse("Pure Storage", "purestorage", "https://www.purestorage.com/company/careers.html"),
    greenhouse("Coinbase", "coinbase", "https://www.coinbase.com/careers/positions"),
    # Aurora's former public Greenhouse board returns 404.
    fallback("Aurora", "https://aurora.tech/careers", "Enterprise portal / public web fallback"),
    greenhouse("Databricks", "databricks", "https://www.databricks.com/company/careers/open-positions"),
    # DoorDash's current WordPress board blocks plain HTTP with a WAF. Its
    # official search URL remains available in health output for direct use.
    fallback("DoorDash", "https://careersatdoordash.com/job-search/", "Enterprise portal / public web fallback"),
    # Plaid's legacy Greenhouse endpoint returns 404; its public Lever board
    # remains the stable API contract (including a truthful zero-posting scan).
    lever("Plaid", "plaid", "https://plaid.com/careers/", "Lever (current public careers board)"),
    greenhouse("Together AI", "togetherai", "https://www.together.ai/careers", "Greenhouse (current public careers board)"),
    lever("Zoox", "zoox", "https://zoox.com/careers/", "Lever (current)"),
    ashby("OpenAI", "openai", "https://openai.com/careers/search/"),
    ashby("Perplexity AI", "perplexity", "https://www.perplexity.ai/hub/careers"),
    ashby("Pinecone", "pinecone", "https://www.pinecone.io/careers/"),
    ashby("Anyscale", "anyscale", "https://www.anyscale.com/careers"),
    ashby("Fireworks AI", "fireworks", "https://fireworks.ai/careers"),
    lever("Palantir", "palantir", "https://jobs.lever.co/palantir"),
    # The prior Lever board was retired.  Its page differs between browser and
    # plain HTTP responses, so it remains an honest first-party fallback.
    fallback("Cohesity", "https://www.cohesity.com/careers/open-positions/", "Enterprise portal / public web fallback"),
    # Verified from Moloco's public careers page: job-boards.greenhouse.io/moloco
    greenhouse("Moloco", "moloco", "https://www.moloco.com/careers", "Greenhouse (current public careers board)"),
    # Snowflake's Phenom page server-renders paginated job data in phApp.ddo.
    # This avoids relying on an undocumented browser-only widget request.
    phenom_html("Snowflake", "https://careers.snowflake.com/us/en/search-results"),
    workday("Nvidia", "nvidia.wd5.myworkdayjobs.com", "nvidia", "nvidiaexternalcareersite"),
    workday("Salesforce", "salesforce.wd12.myworkdayjobs.com", "salesforce", "External_Career_Site"),
    workday("Adobe", "adobe.wd5.myworkdayjobs.com", "adobe", "external_experienced"),
    workday("Autodesk", "autodesk.wd1.myworkdayjobs.com", "autodesk", "Ext"),
    workday("PayPal", "paypal.wd1.myworkdayjobs.com", "paypal", "jobs"),
    # eBay upgraded from fallback to a live-verified server-rendered Phenom adapter.
    workday("Cisco", "cisco.wd5.myworkdayjobs.com", "cisco", "Cisco_Careers"),
    # Intuit's current public board is not the retired Workday endpoint.
    fallback("Intuit", "https://jobs.intuit.com/search-jobs", "Enterprise portal / public web fallback"),
    workday("CrowdStrike", "crowdstrike.wd5.myworkdayjobs.com", "crowdstrike", "crowdstrikecareers"),
    # Verified from the public career site and its public Workday talent-community URL.
    workday("Palo Alto Networks", "paloaltonetworks.wd5.myworkdayjobs.com", "paloaltonetworks", "panwexternalcareers", "https://jobs.paloaltonetworks.com/en/search-jobs"),
    # Verified from Zscaler's public search page: job-boards.greenhouse.io/zscaler
    greenhouse("Zscaler", "zscaler", "https://www.zscaler.com/careers/search", "Greenhouse (current public careers board)"),
    # The public Splunk Workday endpoint currently rejects the required CXS
    # request contract (422); do not treat that response as a zero-job scan.
    fallback("Splunk", "https://careers.cisco.com/global/en/splunk/search-page", "Enterprise portal / public web fallback"),
    workday("Workday", "workday.wd5.myworkdayjobs.com", "workday", "Workday"),
    # Nutanix's prior Workday board was retired; this public board declares
    # its result total and paginated, stable requisition IDs.
    public_html("Nutanix", "nutanix_html", "https://careers.nutanix.com/en/jobs/", "Nutanix public careers board"),
    # NetApp's current board is at careers.netapp.com, not the retired Workday endpoint.
    fallback("NetApp", "https://careers.netapp.com/search-jobs", "Enterprise portal / public web fallback"),
    fallback("HP", "https://jobs.hp.com/", "Workday / enterprise fallback"),
    workday("HPE", "hpe.wd5.myworkdayjobs.com", "hpe", "WFMathpe"),
    workday("Walmart", "walmart.wd504.myworkdayjobs.com", "walmart", "WalmartExternal"),
    workday("Target", "target.wd5.myworkdayjobs.com", "target", "targetcareers"),
    workday("Capital One", "capitalone.wd12.myworkdayjobs.com", "capitalone", "Capital_One"),
    workday("Mastercard", "mastercard.wd1.myworkdayjobs.com", "mastercard", "CorporateCareers"),
    workday("Visa", "visa.wd5.myworkdayjobs.com", "visa", "Visa"),
    # Verified from Roku's public careers page; it exposes a paginated,
    # server-rendered catalog with direct application URLs.
    public_html("Roku", "roku_html", "https://www.weareroku.com/jobs/search", "Roku public careers board"),
    fallback("Wayfair", "https://www.aboutwayfair.com/careers", "Workday / enterprise fallback"),
    workday("Expedia Group", "expedia.wd108.myworkdayjobs.com", "expedia", "search"),
    workday("T-Mobile", "tmobile.wd1.myworkdayjobs.com", "tmobile", "External"),
    workday("Johnson & Johnson", "jj.wd5.myworkdayjobs.com", "jj", "JJ"),
    # The public UHG/Optum board is retained as a direct link until its API
    # contract can be independently verified.
    fallback("Optum", "https://careers.unitedhealthgroup.com/", "Enterprise portal / public web fallback"),
    google_html("Google", "https://www.google.com/about/careers/applications/jobs/results/?location=United%20States"),
    apple_html("Apple", "https://jobs.apple.com/en-us/search?location=united-states-USA"),
    microsoft("Microsoft", "https://apply.careers.microsoft.com/careers?sort_by=timestamp"),
    amazon("Amazon", "https://www.amazon.jobs/en/search?base_query=software+development+engineer+new+grad&loc_query=United+States"),
    fallback("Meta", "https://www.metacareers.com/jobs/?q=software%20engineer%20university%20grad"),
    fallback("Netflix", "https://jobs.netflix.com/search?q=software%20engineer&location=United%20States"),
    fallback("Uber", "https://www.uber.com/global/en/careers/list/?query=software%20engineer&location=United%20States"),
    fallback("Bloomberg", "https://www.bloomberg.com/company/careers/search/?query=software%20engineer&location=United%20States"),
    fallback("Jane Street", "https://www.janestreet.com/join-jane-street/open-roles/?query=software"),
    fallback("ByteDance/TikTok", "https://jobs.bytedance.com/en/position?keywords=software%20engineer&location=United%20States"),
    eightfold("Qualcomm", "qualcomm.eightfold.ai", "qualcomm.com", "https://qualcomm.eightfold.ai/careers"),
    eightfold("Micron", "micron.eightfold.ai", "micron.com", "https://micron.eightfold.ai/careers"),
    eightfold("Applied Materials", "appliedmaterials.eightfold.ai", "appliedmaterials.com", "https://appliedmaterials.eightfold.ai/careers"),
    fallback("Synopsys", "https://careers.synopsys.com/search-jobs?keywords=software%20engineer&location=United%20States"),
    fallback("Morgan Stanley", "https://www.morganstanley.com/careers/career-opportunities-search?keyword=software%20engineer"),
    fallback("Goldman Sachs", "https://higher.gs.com/roles?query=software%20engineer"),
    fallback("JPMorgan", "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions?keyword=software%20engineer"),
    # --- 2026-08-24 expansion: live-verified public ATS contracts ---
    greenhouse("MongoDB", "mongodb", "https://www.mongodb.com/company/careers", "Greenhouse (live-verified)"),
    greenhouse("Robinhood", "robinhood", "https://careers.robinhood.com/", "Greenhouse (live-verified)"),
    greenhouse("Okta", "okta", "https://www.okta.com/company/careers/", "Greenhouse (live-verified)"),
    greenhouse("Block (Square / Cash App)", "block", "https://block.xyz/careers/", "Greenhouse (live-verified)"),
    greenhouse("LinkedIn", "linkedin", "https://www.linkedin.com/jobs/", "Greenhouse (live-verified)"),
    greenhouse(
        "Samsung Research America",
        "samsungresearchamerica",
        "https://sra.samsung.com/careers/",
        "Greenhouse (live-verified)",
    ),
    phenom_html("eBay", "https://jobs.ebayinc.com/us/en/search-results"),
]

EXPECTED_COMPANIES = frozenset(
    {
        "Anthropic", "Scale AI", "Stripe", "Figma", "Notion", "Datadog",
        "Cloudflare", "Applied Intuition", "Waymo", "Zoox", "Rubrik",
        "Duolingo", "Affirm", "Reddit", "Pinterest", "Roblox", "Dropbox",
        "Box", "Twilio", "Elastic", "HubSpot", "Asana", "Samsara",
        "Verkada", "ServiceTitan", "Confluent", "Pure Storage", "Coinbase",
        "Aurora", "Databricks", "DoorDash", "Plaid", "OpenAI",
        "Perplexity AI", "Together AI", "Pinecone", "Anyscale", "Fireworks AI",
        "Palantir", "Cohesity", "Moloco", "Snowflake", "Nvidia", "Salesforce",
        "Adobe", "Autodesk", "PayPal", "eBay", "Cisco", "Intuit",
        "CrowdStrike", "Palo Alto Networks", "Zscaler", "Splunk", "Workday",
        "Nutanix", "NetApp", "HP", "HPE", "Walmart", "Target", "Capital One",
        "Mastercard", "Visa", "Roku", "Wayfair", "Expedia Group", "T-Mobile",
        "Johnson & Johnson", "Optum", "Google", "Apple", "Microsoft", "Amazon",
        "Meta", "Netflix", "Uber", "Bloomberg", "Jane Street", "ByteDance/TikTok",
        "Qualcomm", "Micron", "Applied Materials", "Synopsys", "Morgan Stanley",
        "Goldman Sachs", "JPMorgan",
        "MongoDB", "Robinhood", "Okta", "Block (Square / Cash App)", "LinkedIn",
        "Samsung Research America",
    }
)
EXPECTED_COMPANY_COUNT = len(EXPECTED_COMPANIES)
CONFIGURED_COMPANIES = {target["company"] for target in TARGETS}
assert len(TARGETS) == EXPECTED_COMPANY_COUNT and len(CONFIGURED_COMPANIES) == EXPECTED_COMPANY_COUNT, (
    f"Expected {EXPECTED_COMPANY_COUNT} unique targets; configured {len(TARGETS)} entries / "
    f"{len(CONFIGURED_COMPANIES)} unique companies."
)
assert CONFIGURED_COMPANIES == EXPECTED_COMPANIES, (
    f"Target-list mismatch. Missing={sorted(EXPECTED_COMPANIES - CONFIGURED_COMPANIES)}; "
    f"unexpected={sorted(CONFIGURED_COMPANIES - EXPECTED_COMPANIES)}"
)


# ---------------------------------------------------------------------------
# Entry-level, location, and advisory profile policies
# ---------------------------------------------------------------------------
NON_TECHNICAL_RE = re.compile(
    r"\b(?:account\s+executive|recruit(?:er|ing)|sourc(?:er|ing)|human\s+resources|"
    r"people\s+partner|sales|customer\s+support|marketing|financial\s+analyst|"
    r"legal|compliance|warehouse|supply\s+chain)\b",
    re.IGNORECASE,
)
TECHNICAL_RE = re.compile(
    r"\b(?:software\s+(?:engineer|engineering|developer)|software\s+development\s+engineer|"
    r"\bswe\b|\bsde\b|machine\s+learning\s+engineer|\bml\s+engineer|"
    r"(?:ml|machine\s+learning)\s*(?:/|and)\s*(?:ai|artificial\s+intelligence)\s+engineer|"
    r"(?:ai|artificial\s+intelligence)\s*(?:/|and)\s*(?:ml|machine\s+learning)\s+engineer|"
    r"(?:ai|artificial\s+intelligence)\s+engineer|data\s+engineer|backend|back[-\s]?end|"
    r"front[-\s]?end|full[-\s]?stack|infrastructure\s+engineer|platform\s+engineer|"
    r"cloud\s+engineer|systems?\s+engineer|site\s+reliability\s+engineer|\bsre\b|"
    r"production\s+engineer|devops\s+engineer|developer\s+(?:productivity|experience)\s+engineer|"
    r"mlops\s+engineer|embedded|firmware|research\s+(?:engineer|intern)|"
    r"computer\s+vision\s+engineer|computer\s+science\s+(?:intern|co[-\s]?op)|"
    r"robotics?\s+(?:engineer|intern))\b",
    re.IGNORECASE,
)
EXPERIENCED_RE = re.compile(
    r"\b(?:senior|sr\.?|staff|principal|lead|distinguished|manager|director|"
    r"vice\s+president|vp|experienced|mid[-\s]?level)\b",
    re.IGNORECASE,
)
LEVEL_2_PLUS_RE = re.compile(
    r"\b(?:(?:level\s*)?(?:[2-9]|ii|iii|iv|v|vi|vii|viii|ix|x)|"
    r"(?:swe|sde)\s*(?:[2-9]|ii|iii|iv|v|vi|vii|viii|ix|x)|"
    r"(?:ic|l|e)\s*[2-9]|(?:engineer|developer)\s*(?:[2-9]|ii|iii|iv|v|vi|vii|viii|ix|x))\b",
    re.IGNORECASE,
)
ENTRY_SIGNAL_RE = re.compile(
    r"\b(?:new\s+grad(?:uate)?|university\s+grad(?:uate)?|early\s+career|associate|"
    r"junior|jr\.?|level\s*(?:1|i)|swe\s*(?:1|i)|sde\s*(?:1|i)|ic\s*1|l\s*3|e\s*3|"
    r"20(?:25|26|27)\s*(?:grad|graduate)|intern(?:ship)?|co[-\s]?op)\b",
    re.IGNORECASE,
)

PROFILE_PRIMARY_RE = re.compile(
    # Resume-derived priorities: backend/distributed systems, cloud, reliability,
    # observability, and developer/productivity infrastructure. This is ranking
    # only; profile labels are never used to exclude a technical entry-level job.
    r"\b(?:back[-\s]?end|platform|distributed\s+systems?|microservices?|event[-\s]?driven|"
    r"infrastructure|cloud|observability|site\s+reliability|\bsre\b|"
    r"production(?:\s+engineering)?|devops|developer\s+(?:productivity|experience)|"
    r"server[-\s]?side|api\s+platform)\b",
    re.IGNORECASE,
)
PROFILE_WEB_RE = re.compile(
    r"\b(?:full[-\s]?stack|front[-\s]?end|web\s+(?:engineer|developer|platform)|application\s+engineer)\b",
    re.IGNORECASE,
)
PROFILE_AI_PLATFORM_RE = re.compile(
    r"\b(?:mlops|machine\s+learning\s+platform|ml\s+platform|"
    r"(?:ai|artificial\s+intelligence|ml|machine\s+learning)\s+infrastructure|model\s+platform)\b",
    re.IGNORECASE,
)
PROFILE_ADJACENT_RE = re.compile(
    r"\b(?:data\s+(?:engineer|scientist|analytics)|analytics\s+engineer|business\s+intelligence|"
    r"(?:machine\s+learning|ml|ai|artificial\s+intelligence)\s+(?:engineer|research(?:er)?|scientist)|"
    r"research\s+(?:engineer|scientist)|applied\s+scientist|computer\s+vision|robotics?|embedded|"
    r"firmware|hardware|silicon|verification|validation|compiler)\b",
    re.IGNORECASE,
)
PROFILE_GENERAL_SWE_RE = re.compile(
    r"\b(?:software\s+(?:engineer|developer)|software\s+development\s+engineer|\bswe\b|\bsde\b)\b",
    re.IGNORECASE,
)

US_STATE_NAMES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida",
    "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina", "north dakota", "ohio", "oklahoma",
    "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah",
    "vermont", "virginia", "washington", "west virginia", "wisconsin", "wyoming", "district of columbia",
]
US_STATE_ABBREVS = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
]
US_HUBS = [
    "san francisco", "san jose", "sunnyvale", "mountain view", "palo alto", "menlo park", "redwood city", "cupertino",
    "oakland", "new york city", "new york", "nyc", "seattle", "bellevue", "redmond", "austin", "boston", "cambridge",
    "chicago", "denver", "atlanta", "los angeles", "san diego", "irvine", "portland", "raleigh", "durham", "dallas",
    "houston", "miami", "minneapolis", "salt lake city", "washington, d.c.", "washington dc", "mclean",
]
US_LOCATION_RE = re.compile(
    r"\b(?:united\s+states|u\.?s\.?a?\.?|usa|remote\s*[- ]?\s*(?:us|usa|united\s+states)|"
    + "|".join(map(re.escape, US_STATE_NAMES + US_HUBS))
    + r")\b|(?:,|\()\s*(?:" + "|".join(US_STATE_ABBREVS) + r")\b",
    re.IGNORECASE,
)
INTERNATIONAL_LOCATION_RE = re.compile(
    r"\b(?:india|bangalore|bengaluru|hyderabad|mumbai|pune|london|united\s+kingdom|uk|canada|toronto|"
    r"vancouver|montreal|waterloo|germany|berlin|munich|poland|warsaw|singapore|australia|sydney|melbourne|"
    r"ireland|dublin|netherlands|amsterdam|france|paris|spain|madrid|sweden|stockholm|japan|tokyo|korea|seoul|"
    r"china|shanghai|beijing|taiwan|israel|tel\s+aviv|brazil|sao\s+paulo|mexico|bogota|czech(?:ia|\s+republic)|"
    r"prague|romania|bucharest|philippines|manila)\b",
    re.IGNORECASE,
)


def entry_evidence(title: str) -> Optional[str]:
    """Return title-level evidence; flat technical titles stay visible but unverified."""
    title = " ".join((title or "").split())
    if not title or NON_TECHNICAL_RE.search(title) or not TECHNICAL_RE.search(title):
        return None
    if EXPERIENCED_RE.search(title) or LEVEL_2_PLUS_RE.search(title):
        return None
    return "Explicit title signal" if ENTRY_SIGNAL_RE.search(title) else "Unverified flat technical title"


def profile_fit(title: str) -> Tuple[str, str]:
    """Profile fit is advisory and never removes an otherwise eligible job."""
    title = " ".join((title or "").split())
    if PROFILE_PRIMARY_RE.search(title):
        return "Strong", "Backend / platform / infrastructure / reliability"
    if PROFILE_WEB_RE.search(title):
        return "Good", "Full-stack / frontend / web engineering"
    if PROFILE_AI_PLATFORM_RE.search(title):
        return "Selective", "ML / AI platform or infrastructure"
    if PROFILE_ADJACENT_RE.search(title):
        return "Adjacent", "Technical role outside the primary backend/platform focus"
    if PROFILE_GENERAL_SWE_RE.search(title):
        return "Broad SWE", "General software engineering"
    return "Technical / Adjacent", "Technical entry-level role outside the primary focus"


def is_us_only_location(location: str) -> bool:
    location = " ".join((location or "").split())
    return bool(location and not INTERNATIONAL_LOCATION_RE.search(location) and US_LOCATION_RE.search(location))


# ---------------------------------------------------------------------------
# Normalization and public ATS adapters
# ---------------------------------------------------------------------------
RELATIVE_DATE_RE = re.compile(
    r"(?:posted\s*)?(\d+)\+?\s*(minutes?|hours?|days?|weeks?|months?|years?)\s+ago",
    re.IGNORECASE,
)


@dataclass
class ScanOutcome:
    jobs: List[Dict[str, Any]]
    complete: bool
    detail: str


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, dict):
        preferred = ("name", "location", "label", "city", "state", "country", "value")
        parts = [as_text(value[key]) for key in preferred if value.get(key)]
        if parts:
            return ", ".join(dict.fromkeys(part for part in parts if part))
        return ", ".join(part for part in (as_text(item) for item in value.values()) if part)
    if isinstance(value, (list, tuple, set)):
        return "; ".join(dict.fromkeys(part for part in (as_text(item) for item in value) if part))
    return str(value).strip()


def parse_utc(value: Any) -> Optional[datetime]:
    """Normalize ISO strings, epoch values, and Workday relative date text."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value) / 1000 if abs(float(value)) >= 100_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OSError, OverflowError, ValueError):
            return None

    raw = str(value).strip()
    if raw.isdigit() and len(raw) >= 10:
        return parse_utc(int(raw))
    lower = raw.lower()
    if "today" in lower or "just posted" in lower:
        return RUN_STARTED_AT
    if "yesterday" in lower:
        return RUN_STARTED_AT - timedelta(days=1)
    relative = RELATIVE_DATE_RE.search(lower)
    if relative:
        count = int(relative.group(1))
        unit = relative.group(2).lower()
        if unit.startswith("minute"):
            delta = timedelta(minutes=count)
        elif unit.startswith("hour"):
            delta = timedelta(hours=count)
        elif unit.startswith("day"):
            delta = timedelta(days=count)
        elif unit.startswith("week"):
            delta = timedelta(weeks=count)
        elif unit.startswith("month"):
            # Workday supplies a relative age, not a calendar date. Use a
            # transparent 30-day approximation rather than claiming precision.
            delta = timedelta(days=30 * count)
        else:
            delta = timedelta(days=365 * count)
        return RUN_STARTED_AT - delta
    parsed = pd.to_datetime(raw, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.to_pydatetime(warn=False)


def canonical_job(job_key: Any, title: Any, location: Any, source_date: Any, url: Any, date_basis: str) -> Dict[str, Any]:
    key = as_text(job_key)
    if not key:
        raise ValueError("ATS response did not provide a stable job identity")
    return {
        "job_key": key,
        "title": as_text(title),
        "location": as_text(location),
        "source_date": parse_utc(source_date),
        "url": as_text(url),
        "date_basis": date_basis,
    }


def stable_key(provider: str, board: str, identifier: Any) -> str:
    """Reject missing ATS identifiers instead of silently collapsing jobs together."""
    value = as_text(identifier)
    if not value:
        raise ValueError(f"{provider} response did not provide a stable job identity")
    return f"{provider}:{board}:{value}"


def request_json(
    method: str,
    url: str,
    deadline: float,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Any:
    """Make a bounded JSON request with transient-failure and 429 backoff."""
    for attempt in range(MAX_HTTP_ATTEMPTS):
        remaining = deadline - time.monotonic()
        if remaining <= 0.05:
            raise TimeoutError("source scan budget exhausted")
        slot_wait = min(PER_REQUEST_TIMEOUT_SECONDS, max(0.05, remaining))
        if not REQUEST_SLOTS.acquire(timeout=slot_wait):
            raise TimeoutError("timed out waiting for a bounded request slot")
        retry_delay: Optional[float] = None
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0.05:
                raise TimeoutError("source scan budget exhausted")
            timeout = min(PER_REQUEST_TIMEOUT_SECONDS, max(0.05, remaining))
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=payload,
                headers={**HEADERS, **(extra_headers or {})},
                timeout=(min(2.0, timeout), timeout),
            )
            if response.status_code in {408, 429, 500, 502, 503, 504} and attempt < MAX_HTTP_ATTEMPTS - 1:
                # Honor the server's Retry-After hint when present; otherwise
                # apply a real rate-limit pause that grows per attempt.
                retry_after = as_positive_int(response.headers.get("Retry-After"))
                retry_delay = float(min(30, retry_after)) if retry_after else RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
            else:
                response.raise_for_status()
                return response.json()
        except requests.RequestException:
            last_attempt = attempt >= MAX_HTTP_ATTEMPTS - 1
            if last_attempt or deadline - time.monotonic() <= RETRY_DELAY_SECONDS + 1.0:
                raise
            retry_delay = RETRY_DELAY_SECONDS
        finally:
            REQUEST_SLOTS.release()
        if retry_delay is not None:
            time.sleep(min(retry_delay, max(0.0, deadline - time.monotonic())))
    raise RuntimeError("unreachable request retry state")


def request_text(url: str, deadline: float, *, params: Optional[Dict[str, Any]] = None) -> str:
    """Get a public board page under the same bounded retry policy."""
    for attempt in range(MAX_HTTP_ATTEMPTS):
        remaining = deadline - time.monotonic()
        if remaining <= 0.05:
            raise TimeoutError("source scan budget exhausted")
        slot_wait = min(PER_REQUEST_TIMEOUT_SECONDS, max(0.05, remaining))
        if not REQUEST_SLOTS.acquire(timeout=slot_wait):
            raise TimeoutError("timed out waiting for a bounded request slot")
        retry_delay: Optional[float] = None
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0.05:
                raise TimeoutError("source scan budget exhausted")
            timeout = min(PER_REQUEST_TIMEOUT_SECONDS, max(0.05, remaining))
            response = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=(min(2.0, timeout), timeout),
            )
            if response.status_code in {408, 429, 500, 502, 503, 504} and attempt < MAX_HTTP_ATTEMPTS - 1:
                retry_after = as_positive_int(response.headers.get("Retry-After"))
                retry_delay = float(min(30, retry_after)) if retry_after else RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
            else:
                response.raise_for_status()
                return response.text
        except requests.RequestException:
            last_attempt = attempt >= MAX_HTTP_ATTEMPTS - 1
            if last_attempt or deadline - time.monotonic() <= RETRY_DELAY_SECONDS + 1.0:
                raise
            retry_delay = RETRY_DELAY_SECONDS
        finally:
            REQUEST_SLOTS.release()
        if retry_delay is not None:
            time.sleep(min(retry_delay, max(0.0, deadline - time.monotonic())))
    raise RuntimeError("unreachable request retry state")


def as_positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def response_total(data: Dict[str, Any]) -> Optional[int]:
    """Read common result-total shapes without guessing when none is present."""
    for key in ("total", "totalResults", "totalCount", "count"):
        value = as_positive_int(data.get(key))
        if value is not None:
            return value
    # Amazon exposes ``hits`` as an integer; several other APIs use a nested
    # ``hits`` object. Support both forms without assuming either is exact.
    value = as_positive_int(data.get("hits"))
    if value is not None:
        return value
    hits = data.get("hits")
    if isinstance(hits, dict):
        for key in ("total", "totalResults", "totalCount"):
            value = as_positive_int(hits.get(key))
            if value is not None:
                return value
    return None


# ---------------------------------------------------------------------------
# Tested public-HTML board adapters
# ---------------------------------------------------------------------------
PUBLIC_HTML_DATE_BASIS = "Public board has no employer posting timestamp; monitor first_seen_at"


def clean_html(fragment: str) -> str:
    """Convert a narrow, known card fragment to normalized visible text."""
    return " ".join(unescape(re.sub(r"<[^>]+>", " ", fragment or "")).split())


def card_slices(page: str, start_pattern: re.Pattern[str]) -> List[Tuple[re.Match[str], str]]:
    """Return non-overlapping card slices without relying on a third-party parser."""
    starts = list(start_pattern.finditer(page))
    return [
        (match, page[match.start() : starts[index + 1].start() if index + 1 < len(starts) else len(page)])
        for index, match in enumerate(starts)
    ]


def extract_embedded_json_object(page: str, marker: str) -> Dict[str, Any]:
    """Parse one JSON object assigned in a first-party HTML script safely."""
    source = unescape(page)
    marker_index = source.find(marker)
    if marker_index < 0:
        raise ValueError(f"First-party page did not contain expected marker {marker!r}")
    start = marker_index + len(marker)
    while start < len(source) and source[start].isspace():
        start += 1
    if start >= len(source) or source[start] != "{":
        raise ValueError(f"Expected JSON object after {marker!r}")

    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(source)):
        character = source[index]
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                value = json.loads(source[start : index + 1])
                if not isinstance(value, dict):
                    raise TypeError("Embedded JSON root was not an object")
                return value
    raise ValueError(f"Unterminated JSON object after {marker!r}")


def phenom_page_payload(page: str) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Read Snowflake's server-rendered Phenom search payload from HTML."""
    ddo = extract_embedded_json_object(page, "phApp.ddo =")
    search = ddo.get("eagerLoadRefineSearch")
    if not isinstance(search, dict):
        raise TypeError("Phenom page did not expose eagerLoadRefineSearch")
    data = search.get("data")
    if not isinstance(data, dict):
        raise TypeError("Phenom search payload did not expose data")
    jobs = data.get("jobs")
    if not isinstance(jobs, list):
        raise TypeError("Phenom search payload did not expose jobs")
    return jobs, as_positive_int(search.get("totalHits"))


def canonical_phenom_job(spec: Dict[str, Any], job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a Snowflake/Phenom job while retaining its stable job identity."""
    identifier = job.get("jobId") or job.get("jobSeqNo") or job.get("reqId")
    url = as_text(job.get("applyUrl"))
    if not url:
        url = urljoin(spec["fallback_url"].rstrip("/") + "/", as_text(job.get("jobUrl"))) or spec["fallback_url"]
    return canonical_job(
        stable_key("phenom", spec["company"], identifier),
        job.get("title"),
        job.get("multi_location") or job.get("location") or job.get("cityStateCountry"),
        job.get("postedDate") or job.get("dateCreated"),
        url,
        "Phenom postedDate",
    )


def fetch_phenom_html(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    """Fully traverse a known server-rendered Phenom board with declared totals."""
    endpoint = spec["fallback_url"]
    first_page = request_text(endpoint, deadline)
    first_jobs, expected_total = phenom_page_payload(first_page)
    if expected_total is None:
        raise TypeError("Phenom search payload did not expose totalHits")
    if expected_total == 0:
        return ScanOutcome([], True, "Phenom reported zero open postings.")
    if not first_jobs:
        return ScanOutcome([], False, "Phenom reported jobs but initial page was empty; baseline not advanced.")

    page_size = len(first_jobs)
    expected_pages = (expected_total + page_size - 1) // page_size
    if expected_pages > PHENOM_MAX_PAGES:
        return ScanOutcome([], False, f"Phenom requires {expected_pages} pages beyond safety cap; baseline not advanced.")

    jobs: List[Dict[str, Any]] = []
    seen_keys = set()
    for page_number in range(expected_pages):
        page = first_page if page_number == 0 else request_text(
            endpoint,
            deadline,
            params={"from": page_number * page_size, "s": 1},
        )
        raw_jobs, page_total = phenom_page_payload(page)
        if page_total is not None and page_total != expected_total:
            return ScanOutcome(jobs, False, "Phenom reported a changed total during scan; baseline not advanced.")
        if not raw_jobs:
            return ScanOutcome(jobs, False, f"Phenom page {page_number + 1} was empty before declared total.")

        new_on_page = 0
        for raw_job in raw_jobs:
            job = canonical_phenom_job(spec, raw_job)
            if job["job_key"] in seen_keys:
                continue
            seen_keys.add(job["job_key"])
            new_on_page += 1
            jobs.append(job)
        if new_on_page == 0:
            return ScanOutcome(jobs, False, f"Phenom repeated page {page_number + 1}; baseline not advanced.")

    if len(jobs) != expected_total:
        return ScanOutcome(
            jobs,
            False,
            f"Phenom declared {expected_total} postings but yielded {len(jobs)} unique stable IDs; baseline not advanced.",
        )
    return ScanOutcome(jobs, True, f"Fetched all {len(jobs)} server-rendered Phenom postings across {expected_pages} pages.")


GOOGLE_CARD_START_RE = re.compile(
    r"<li\b[^>]*\bclass=[\"'][^\"']*\blLd3Je\b[^\"']*[\"'][^>]*\bssk=[\"'](?P<identifier>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
GOOGLE_TITLE_RE = re.compile(r"<h3\b[^>]*\bclass=[\"']QJPWVe[\"'][^>]*>(?P<title>.*?)</h3>", re.IGNORECASE | re.DOTALL)
GOOGLE_LOCATION_RE = re.compile(
    r"<span\b[^>]*\bclass=[\"'][^\"']*\br0wTof\b[^\"']*[\"'][^>]*>(?P<location>.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
GOOGLE_URL_RE = re.compile(
    r"<a\b[^>]*\bhref=[\"'](?P<href>[^\"']*jobs/results/[^\"']+)[\"'][^>]*\baria-label=[\"']Learn more",
    re.IGNORECASE | re.DOTALL,
)


def fetch_google_html(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    """Traverse Google Careers' server-rendered US result pages to completion."""
    endpoint = spec["fallback_url"]
    jobs: List[Dict[str, Any]] = []
    seen_keys = set()

    for page_number in range(1, GOOGLE_MAX_PAGES + 1):
        page = request_text(endpoint, deadline, params={"page": page_number} if page_number > 1 else None)
        cards = card_slices(page, GOOGLE_CARD_START_RE)
        if not cards:
            if page_number == 1:
                raise TypeError("Google Careers initial US search page had no recognizable job cards")
            return ScanOutcome(jobs, True, f"Fetched {len(jobs)} Google Careers postings across {page_number - 1} pages.")

        new_on_page = 0
        for start, card in cards:
            title_match = GOOGLE_TITLE_RE.search(card)
            location_match = GOOGLE_LOCATION_RE.search(card)
            url_match = GOOGLE_URL_RE.search(card)
            if not title_match or not location_match or not url_match:
                raise ValueError("Google Careers card lacked title, location, or direct job URL")
            href = unescape(url_match.group("href"))
            job_id_match = re.search(r"jobs/results/(\d+)(?:[-/?]|$)", href)
            # Prefer Google Careers' numeric job ID; fall back to its public
            # card identity only if the URL shape changes.
            identifier = job_id_match.group(1) if job_id_match else unescape(start.group("identifier"))
            key = stable_key("google", "careers", identifier)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            new_on_page += 1
            jobs.append(
                canonical_job(
                    key,
                    clean_html(title_match.group("title")),
                    clean_html(location_match.group("location")),
                    None,
                    urljoin("https://www.google.com/about/careers/applications/", href),
                    "Google Careers list does not expose a posting timestamp; monitor first_seen_at",
                )
            )

        if new_on_page == 0:
            return ScanOutcome(jobs, False, f"Google Careers repeated page {page_number}; baseline not advanced.")
        if len(cards) < GOOGLE_PAGE_SIZE:
            return ScanOutcome(jobs, True, f"Fetched {len(jobs)} Google Careers postings across {page_number} pages.")

    return ScanOutcome(
        jobs,
        False,
        f"Google Careers reached safety cap of {GOOGLE_MAX_PAGES} pages; baseline not advanced.",
    )


APPLE_HYDRATION_MARKER = "window.__staticRouterHydrationData = JSON.parse("


def apple_page_payload(page: str) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Decode Apple Jobs' JSON-string hydration payload from a public page."""
    marker_index = page.find(APPLE_HYDRATION_MARKER)
    if marker_index < 0:
        raise ValueError("Apple Jobs page did not contain its hydration payload")
    json_string_start = marker_index + len(APPLE_HYDRATION_MARKER)
    try:
        encoded_json, _ = json.JSONDecoder().raw_decode(page[json_string_start:])
        hydration = json.loads(encoded_json)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Apple Jobs hydration payload was malformed") from exc
    search = ((hydration.get("loaderData") or {}).get("search")) if isinstance(hydration, dict) else None
    if not isinstance(search, dict):
        raise TypeError("Apple Jobs hydration payload did not expose search data")
    results = search.get("searchResults")
    if not isinstance(results, list):
        raise TypeError("Apple Jobs search data did not expose searchResults")
    return results, as_positive_int(search.get("totalRecords"))


def canonical_apple_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize an Apple Jobs result and construct its stable direct role URL."""
    identifier = job.get("reqId") or job.get("id") or job.get("positionId") or job.get("jobPositionId")
    req_id = as_text(job.get("reqId"))
    slug = as_text(job.get("transformedPostingTitle"))
    if req_id and slug:
        url = f"https://jobs.apple.com/en-us/details/{req_id}/{slug}"
    elif req_id:
        url = f"https://jobs.apple.com/en-us/details/{req_id}"
    else:
        url = "https://jobs.apple.com/en-us/search?location=united-states-USA"
    return canonical_job(
        stable_key("apple", "jobs", identifier),
        job.get("postingTitle") or job.get("title"),
        job.get("locations"),
        job.get("postDateInGMT") or job.get("postingDate"),
        url,
        "Apple postDateInGMT",
    )


def fetch_apple_html(spec: Dict[str, Any], deadline: float, delta_cutoff: Optional[datetime] = None) -> ScanOutcome:
    """Fully paginate Apple's public US board using its declared result count.

    Apple's board is natively newest-first (verified: strictly monotonic
    postDateInGMT across pages). With an active delta cutoff, pagination stops
    once a whole page predates the cutoff; the declared-total cross-check is
    skipped for that truncated snapshot because it no longer applies.
    """
    endpoint = "https://jobs.apple.com/en-us/search"
    base_params = {"location": "united-states-USA"}
    first_page = request_text(endpoint, deadline, params=base_params)
    first_results, expected_total = apple_page_payload(first_page)
    if expected_total is None:
        raise TypeError("Apple Jobs search data did not expose totalRecords")
    if expected_total == 0:
        return ScanOutcome([], True, "Apple Jobs reported zero US postings.")
    if not first_results:
        return ScanOutcome([], False, "Apple Jobs reported postings but initial page was empty; baseline not advanced.")

    page_size = len(first_results)
    expected_pages = (expected_total + page_size - 1) // page_size
    if expected_pages > APPLE_MAX_PAGES:
        return ScanOutcome([], False, f"Apple Jobs requires {expected_pages} pages beyond safety cap; baseline not advanced.")

    jobs: List[Dict[str, Any]] = []
    seen_keys = set()
    stopped_at_cutoff = False
    for page_number in range(1, expected_pages + 1):
        params = base_params if page_number == 1 else {**base_params, "page": page_number}
        page = first_page if page_number == 1 else request_text(endpoint, deadline, params=params)
        raw_jobs, page_total = apple_page_payload(page)
        if page_total is not None and page_total != expected_total and not stopped_at_cutoff:
            return ScanOutcome(jobs, False, "Apple Jobs reported a changed total during scan; baseline not advanced.")
        if not raw_jobs:
            if stopped_at_cutoff:
                break
            return ScanOutcome(jobs, False, f"Apple Jobs page {page_number} was empty before declared total.")

        new_on_page = 0
        for raw_job in raw_jobs:
            job = canonical_apple_job(raw_job)
            if job["job_key"] in seen_keys:
                continue
            seen_keys.add(job["job_key"])
            new_on_page += 1
            jobs.append(job)

        if delta_cutoff is not None:
            ages = [job["source_date"] for job in jobs[-len(raw_jobs) :]]
            if ages and all(age is not None and age < delta_cutoff for age in ages):
                stopped_at_cutoff = True
                return ScanOutcome(
                    jobs,
                    True,
                    f"Apple delta scan stopped at cutoff after {page_number} page(s); "
                    "older postings skipped by design.",
                )
        if new_on_page == 0:
            return ScanOutcome(jobs, False, f"Apple Jobs repeated page {page_number}; baseline not advanced.")

    if len(jobs) != expected_total:
        return ScanOutcome(
            jobs,
            False,
            f"Apple Jobs declared {expected_total} postings but yielded {len(jobs)} unique stable IDs; baseline not advanced.",
        )
    return ScanOutcome(jobs, True, f"Fetched all {len(jobs)} Apple US postings across {expected_pages} pages.")


NUTANIX_CARD_START_RE = re.compile(
    r'<div\b[^>]*\bclass=["\']card\s+card-job\s+job-hover["\'][^>]*'
    r'\bdata-id=["\'](?P<identifier>[^"\']+)["\'][^>]*>',
    re.IGNORECASE,
)
NUTANIX_TITLE_RE = re.compile(
    r'<h2\b[^>]*\bclass=["\']card-title["\'][^>]*>.*?'
    r'<a\b[^>]*\bhref=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
NUTANIX_LOCATION_RE = re.compile(
    r'<p\b[^>]*\bclass=["\'][^"\']*\bjob-meta-location\b[^"\']*["\'][^>]*>(?P<location>.*?)</p>',
    re.IGNORECASE | re.DOTALL,
)
NUTANIX_TOTAL_RE = re.compile(r'\bdata-results=["\'](?P<total>\d+)["\']', re.IGNORECASE)


def fetch_nutanix_html(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    """Fully traverse Nutanix's declared-result, server-rendered job board."""
    endpoint = spec["fallback_url"]
    first_page = request_text(endpoint, deadline)
    total_match = NUTANIX_TOTAL_RE.search(first_page)
    if not total_match:
        raise TypeError("Nutanix board did not expose its declared result total")
    expected_total = int(total_match.group("total"))
    first_cards = card_slices(first_page, NUTANIX_CARD_START_RE)
    if expected_total == 0:
        return ScanOutcome([], True, "Nutanix reported zero open postings.")
    if not first_cards:
        raise TypeError("Nutanix board reported jobs but had no recognizable cards")
    expected_pages = (expected_total + len(first_cards) - 1) // len(first_cards)
    if expected_pages > PUBLIC_HTML_MAX_PAGES:
        return ScanOutcome([], False, f"Nutanix requires {expected_pages} pages beyond the safety cap; baseline not advanced.")

    jobs: List[Dict[str, Any]] = []
    seen_keys = set()
    for page_number in range(1, expected_pages + 1):
        page = first_page if page_number == 1 else request_text(endpoint, deadline, params={"page": page_number})
        cards = card_slices(page, NUTANIX_CARD_START_RE)
        if not cards:
            return ScanOutcome(jobs, False, f"Nutanix page {page_number} had no recognizable cards; baseline not advanced.")

        new_on_page = 0
        for start, card in cards:
            title_match = NUTANIX_TITLE_RE.search(card)
            location_match = NUTANIX_LOCATION_RE.search(card)
            if not title_match or not location_match:
                raise ValueError("Nutanix opening card lacked title or location")
            key = stable_key("nutanix", "careers", unescape(start.group("identifier")))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            new_on_page += 1
            jobs.append(
                canonical_job(
                    key,
                    clean_html(title_match.group("title")),
                    clean_html(location_match.group("location")),
                    None,
                    urljoin("https://careers.nutanix.com", unescape(title_match.group("href"))),
                    PUBLIC_HTML_DATE_BASIS,
                )
            )

        if new_on_page == 0:
            return ScanOutcome(jobs, False, "Nutanix repeated a page; baseline not advanced.")

    if len(jobs) != expected_total:
        return ScanOutcome(
            jobs,
            False,
            f"Nutanix declared {expected_total} postings but returned {len(jobs)} unique cards; baseline not advanced.",
        )
    return ScanOutcome(jobs, True, f"Fetched all {len(jobs)} Nutanix postings across {expected_pages} pages.")


ROKU_ROW_RE = re.compile(
    r'<tr\b[^>]*\bdata-job-url=["\'](?P<url>[^"\']+)["\'][^>]*>(?P<body>.*?)</tr>',
    re.IGNORECASE | re.DOTALL,
)
ROKU_TITLE_RE = re.compile(
    r'<td\b[^>]*\bclass=["\']job-search-results-title["\'][^>]*>.*?<a\b[^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
ROKU_LOCATION_RE = re.compile(
    r'<td\b[^>]*\bclass=["\']job-search-results-location["\'][^>]*>.*?<li\b[^>]*>(?P<location>.*?)</li>',
    re.IGNORECASE | re.DOTALL,
)


def public_html_page_count(page: str) -> int:
    """Return the highest explicit page link exposed by a public board."""
    values = [int(value) for value in re.findall(r"(?:[?&]|&amp;)page=(\d+)", unescape(page), re.IGNORECASE)]
    return max(values, default=1)


def fetch_roku_html(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    """Traverse every explicit page of Roku's company-owned job catalog."""
    endpoint = spec["fallback_url"]
    jobs: List[Dict[str, Any]] = []
    seen_keys = set()
    page_number = 1
    expected_pages = 1

    while page_number <= expected_pages:
        page = request_text(endpoint, deadline, params={"page": page_number} if page_number > 1 else None)
        rows = list(ROKU_ROW_RE.finditer(page))
        if not rows:
            if page_number == 1 and re.search(r"\bno\s+(?:open\s+)?(?:jobs|positions)\b", clean_html(page), re.IGNORECASE):
                return ScanOutcome([], True, "Roku reported no open postings.")
            return ScanOutcome(jobs, False, f"Roku page {page_number} had no recognizable job rows; baseline not advanced.")

        new_on_page = 0
        for row in rows:
            title_match = ROKU_TITLE_RE.search(row.group("body"))
            location_match = ROKU_LOCATION_RE.search(row.group("body"))
            if not title_match or not location_match:
                raise ValueError("Roku job row lacked title or location")
            url = unescape(row.group("url"))
            key = stable_key("roku", "careers", url)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            new_on_page += 1
            jobs.append(
                canonical_job(
                    key,
                    clean_html(title_match.group("title")),
                    clean_html(location_match.group("location")),
                    None,
                    url,
                    PUBLIC_HTML_DATE_BASIS,
                )
            )

        expected_pages = max(expected_pages, public_html_page_count(page))
        if expected_pages > PUBLIC_HTML_MAX_PAGES:
            return ScanOutcome(jobs, False, f"Roku advertised {expected_pages} pages beyond the safety cap; baseline not advanced.")
        if new_on_page == 0:
            return ScanOutcome(jobs, False, "Roku repeated a page; baseline not advanced.")
        page_number += 1

    return ScanOutcome(jobs, True, f"Fetched {len(jobs)} Roku postings across {expected_pages} pages.")


def fetch_greenhouse(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    data = request_json("GET", f"https://boards-api.greenhouse.io/v1/boards/{spec['board']}/jobs", deadline)
    jobs = []
    for job in data.get("jobs") or []:
        identifier = job.get("id") or job.get("absolute_url")
        jobs.append(
            canonical_job(
                stable_key("greenhouse", spec["board"], identifier),
                job.get("title"),
                job.get("location"),
                job.get("updated_at"),
                job.get("absolute_url"),
                "Greenhouse updated_at (not original opening time)",
            )
        )
    return ScanOutcome(jobs, True, f"Fetched {len(jobs)} active Greenhouse postings.")


def fetch_ashby(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    data = request_json("GET", f"https://api.ashbyhq.com/posting-api/job-board/{spec['board']}", deadline)
    jobs = []
    for job in data.get("jobs") or []:
        if job.get("isListed") is False:
            continue
        url = job.get("jobUrl") or job.get("applyUrl")
        identifier = job.get("id") or job.get("jobPostingId") or url
        jobs.append(
            canonical_job(
                stable_key("ashby", spec["board"], identifier),
                job.get("title"),
                job.get("location") or job.get("address") or job.get("secondaryLocations"),
                job.get("publishedAt"),
                url,
                "Ashby publishedAt",
            )
        )
    return ScanOutcome(jobs, True, f"Fetched {len(jobs)} listed Ashby postings.")


def fetch_lever(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    data = request_json(
        "GET",
        f"https://api.lever.co/v0/postings/{spec['board']}",
        deadline,
        params={"mode": "json"},
    )
    jobs = []
    for job in data or []:
        url = job.get("hostedUrl") or job.get("applyUrl")
        identifier = job.get("id") or url
        jobs.append(
            canonical_job(
                stable_key("lever", spec["board"], identifier),
                job.get("text"),
                (job.get("categories") or {}).get("location"),
                job.get("createdAt"),
                url,
                "Lever createdAt",
            )
        )
    return ScanOutcome(jobs, True, f"Fetched {len(jobs)} Lever postings.")


WORKDAY_REQUISITION_RE = re.compile(r"\b(?:JR|REQ|R)[-_]?\d{4,}\b|\b\d{6,}\b", re.IGNORECASE)


def workday_stable_identifier(job: Dict[str, Any]) -> str:
    """Prefer Workday's requisition identity over its mutable display path."""
    posting_id = as_text(job.get("jobPostingId"))
    if posting_id:
        return posting_id

    bullet_fields = job.get("bulletFields")
    values = [as_text(value) for value in bullet_fields] if isinstance(bullet_fields, (list, tuple)) else []
    for value in values:
        match = WORKDAY_REQUISITION_RE.search(value)
        if match:
            return match.group(0)
    if values:
        return values[0]

    external_path = as_text(job.get("externalPath"))
    if external_path:
        return external_path
    raise ValueError("Workday posting has neither requisition identity nor externalPath")


def canonical_workday_job(spec: Dict[str, Any], job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one Workday card using its requisition number as stable state key."""
    host, tenant, site = spec["host"], spec["tenant"], spec["site"]
    path = as_text(job.get("externalPath"))
    key = stable_key("workday", f"{tenant}:{site}", workday_stable_identifier(job))
    url = f"https://{host}/en-US/{site}{path}" if path.startswith("/") else spec["fallback_url"]
    return canonical_job(
        key,
        job.get("title"),
        job.get("locationsText") or job.get("location"),
        job.get("postedOn") or job.get("postedDate"),
        url,
        "Workday relative postedOn (estimated UTC)",
    )


def workday_us_country_facets(data: Dict[str, Any]) -> Dict[str, List[str]]:
    """Return Workday's explicit United-States country facet, when advertised.

    This is an ATS-provided location filter, not a heuristic scraped from card
    text.  It reduces large global boards before pagination while preserving
    the final title/location policy as a second independent check.
    """
    for facet in data.get("facets") or []:
        if not isinstance(facet, dict):
            continue
        parameter = as_text(facet.get("facetParameter"))
        descriptor = as_text(facet.get("descriptor")).lower()
        if not parameter or ("country" not in descriptor and "country" not in parameter.lower()):
            continue
        matching_ids = []
        for value in facet.get("values") or []:
            if not isinstance(value, dict):
                continue
            country = as_text(value.get("descriptor")).lower()
            if re.search(r"\bunited\s+states(?:\s+of\s+america)?\b|\busa\b", country):
                value_id = as_text(value.get("id"))
                if value_id:
                    matching_ids.append(value_id)
        if matching_ids:
            return {parameter: matching_ids}
    return {}


def discover_workday_us_country_facets(endpoint: str, deadline: float) -> Dict[str, List[str]]:
    """Read one CXS response solely to discover its tenant-specific US facet ID."""
    data = request_json(
        "POST",
        endpoint,
        deadline,
        payload={"appliedFacets": {}, "limit": WORKDAY_PAGE_SIZE, "offset": 0, "searchText": ""},
    )
    if not isinstance(data, dict) or not isinstance(data.get("jobPostings"), list):
        raise TypeError("Workday bootstrap response has no jobPostings list")
    return workday_us_country_facets(data)


def fetch_workday_query_shard(
    spec: Dict[str, Any],
    endpoint: str,
    search_text: str,
    applied_facets: Dict[str, List[str]],
    deadline: float,
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """Exhaust one bounded Workday title query without trusting its total blindly."""
    collected: List[Dict[str, Any]] = []
    seen_keys = set()
    offset = 0
    pages = 0
    total_hint: Optional[int] = None

    while pages < WORKDAY_MAX_PAGES_PER_QUERY:
        data = request_json(
            "POST",
            endpoint,
            deadline,
            payload={
                "appliedFacets": applied_facets,
                "limit": WORKDAY_PAGE_SIZE,
                "offset": offset,
                "searchText": search_text,
            },
        )
        page = data.get("jobPostings")
        if not isinstance(page, list):
            raise TypeError(f"Workday response has no jobPostings list for query {search_text!r}")
        if total_hint is None:
            reported = response_total(data)
            # Some Workday tenants return total=0 on later nonempty pages.
            # Preserve only a first-page value that is compatible with data.
            if reported is not None and (reported > 0 or not page):
                total_hint = reported

        # An actual empty page is stronger evidence than Workday's frequently
        # stale/capped total metadata (e.g. Target/Walmart's "2000").
        if not page:
            return collected, True, f"{search_text!r}: terminal empty page after {pages} page(s); total hint={total_hint}."

        new_on_page = 0
        for raw_job in page:
            job = canonical_workday_job(spec, raw_job)
            if job["job_key"] in seen_keys:
                continue
            seen_keys.add(job["job_key"])
            new_on_page += 1
            collected.append(job)

        pages += 1
        offset += len(page)
        if new_on_page == 0:
            return collected, False, f"{search_text!r}: Workday repeated a page at offset {offset - len(page)}."
        if len(page) < WORKDAY_PAGE_SIZE:
            return collected, True, f"{search_text!r}: terminal short page after {pages} page(s); total hint={total_hint}."

        # A non-capped total can be accepted only when every reported row has
        # a unique stable requisition identity. Capped 2,000 totals must be
        # probed: CXS otherwise repeats the first page past its result window.
        if (
            total_hint is not None
            and total_hint < WORKDAY_CXS_RESULT_WINDOW
            and offset >= total_hint
            and len(seen_keys) == total_hint
        ):
            return collected, True, f"{search_text!r}: validated {total_hint} unique requisitions across {pages} page(s)."

    return (
        collected,
        False,
        f"{search_text!r}: reached {WORKDAY_MAX_PAGES_PER_QUERY} pages; "
        "the CXS result window may be truncating this query.",
    )


def fetch_workday(spec: Dict[str, Any], deadline: float) -> ScanOutcome:
    """Complete the monitored title scope on Workday, or safely return PARTIAL.

    A blank CXS query is not usable for large employers: many tenants silently
    cap it at 2,000 rows. Each query below maps to the title policy, so a
    completed scan covers entry-level CS candidates without pretending to have
    downloaded unrelated warehouse/retail/administrative listings.
    """
    host, tenant, site = spec["host"], spec["tenant"], spec["site"]
    endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    unique_jobs: Dict[str, Dict[str, Any]] = {}
    incomplete_details: List[str] = []
    applied_facets = discover_workday_us_country_facets(endpoint, deadline)
    facet_note = "ATS United-States country facet" if applied_facets else "no ATS US country facet"

    # The role-policy shards are independent. Run a small bounded inner pool
    # (mirroring fetch_amazon/fetch_microsoft/fetch_eightfold) so one very
    # large Workday tenant does not serialize ~40 multi-page queries against
    # its source deadline; all shard requests still share REQUEST_SLOTS and
    # the same source deadline.
    with ThreadPoolExecutor(max_workers=min(WORKDAY_SHARD_WORKERS, len(ROLE_POLICY_QUERY_SHARDS))) as executor:
        futures = {
            executor.submit(fetch_workday_query_shard, spec, endpoint, search_text, applied_facets, deadline): search_text
            for search_text in ROLE_POLICY_QUERY_SHARDS
        }
        for future in as_completed(futures):
            search_text = futures[future]
            try:
                shard_jobs, shard_complete, detail = future.result()
            except (TimeoutError, requests.RequestException, TypeError, ValueError) as exc:
                # One failed shard degrades this source to PARTIAL; it must not
                # discard the shards that completed successfully.
                incomplete_details.append(f"{search_text!r}: {type(exc).__name__}: {exc}")
                continue
            for job in shard_jobs:
                unique_jobs[job["job_key"]] = job
            if not shard_complete:
                incomplete_details.append(detail)

    if incomplete_details:
        return ScanOutcome(
            list(unique_jobs.values()),
            False,
            f"{tenant} Workday role-policy scan incomplete; no baseline advanced. "
            + " | ".join(incomplete_details[:3]),
        )

    return ScanOutcome(
        list(unique_jobs.values()),
        True,
        f"Fetched {len(unique_jobs)} unique Workday postings across {len(ROLE_POLICY_QUERY_SHARDS)} "
        f"role-policy query shards using {facet_note}.",
    )


def canonical_amazon_job(job: Dict[str, Any]) -> Dict[str, Any]:
    path = as_text(job.get("job_path"))
    url = f"https://www.amazon.jobs{path}" if path.startswith("/") else as_text(job.get("url_next_step"))
    identifier = job.get("id") or path or url
    return canonical_job(
        stable_key("amazon", "jobs", identifier),
        job.get("title"),
        job.get("location") or job.get("normalized_location"),
        job.get("posted_date") or job.get("updated_time"),
        url,
        "Amazon posted_date",
    )


def fetch_amazon_query_shard(
    endpoint: str, search_text: str, deadline: float, delta_cutoff: Optional[datetime] = None
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """Exhaust one Amazon title query, detecting its documented 10,000-row cap.

    With ``sort=recent`` (verified newest-first) and an active delta cutoff,
    pagination stops as soon as a whole page predates the cutoff: the monitor
    only needs jobs opened between runs, not the full backlog.
    """
    collected: List[Dict[str, Any]] = []
    seen_keys = set()
    offset = 0
    pages = 0
    total_hint: Optional[int] = None

    while pages < AMAZON_MAX_PAGES_PER_QUERY:
        data = request_json(
            "GET",
            endpoint,
            deadline,
            params={
                "base_query": search_text,
                "loc_query": "United States",
                "result_limit": AMAZON_PAGE_SIZE,
                "offset": offset,
                "sort": "recent",
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Amazon response is not an object for query {search_text!r}")
        page = data.get("jobs")
        if not isinstance(page, list):
            api_error = as_text(data.get("error"))
            if api_error:
                return collected, False, f"{search_text!r}: Amazon API error at offset {offset}: {api_error}"
            raise TypeError(f"Amazon response has no jobs list for query {search_text!r}")
        if total_hint is None:
            total_hint = response_total(data)

        if not page:
            return collected, True, f"{search_text!r}: terminal empty page after {pages} page(s); hits hint={total_hint}."

        new_on_page = 0
        for raw_job in page:
            job = canonical_amazon_job(raw_job)
            if job["job_key"] in seen_keys:
                continue
            seen_keys.add(job["job_key"])
            new_on_page += 1
            collected.append(job)

        pages += 1
        offset += len(page)
        if delta_cutoff is not None:
            ages = [job["source_date"] for job in (canonical_amazon_job(j) for j in page)]
            if ages and all(age is not None and age < delta_cutoff for age in ages):
                return (
                    collected,
                    True,
                    f"{search_text!r}: delta scan stopped at cutoff after {pages} page(s); "
                    "older postings skipped by design.",
                )
        if new_on_page == 0:
            return collected, False, f"{search_text!r}: Amazon repeated a page at offset {offset - len(page)}."
        if len(page) < AMAZON_PAGE_SIZE:
            return collected, True, f"{search_text!r}: terminal short page after {pages} page(s); hits hint={total_hint}."
        if (
            total_hint is not None
            and total_hint < AMAZON_RESULT_WINDOW
            and offset >= total_hint
            and len(seen_keys) == total_hint
        ):
            return collected, True, f"{search_text!r}: validated {total_hint} unique postings across {pages} page(s)."

    if total_hint is not None and total_hint >= AMAZON_RESULT_WINDOW:
        # The provider's own result window ends this query; everything
        # obtainable was obtained. This is completeness, not truncation.
        return (
            collected,
            True,
            f"{search_text!r}: fetched {len(seen_keys)} unique postings up to Amazon's "
            f"{AMAZON_RESULT_WINDOW:,}-result search window.",
        )
    return (
        collected,
        False,
        f"{search_text!r}: reached {AMAZON_MAX_PAGES_PER_QUERY} pages; "
        "Amazon's 10,000-result window may be truncating this query.",
    )


def fetch_amazon(spec: Dict[str, Any], deadline: float, delta_cutoff: Optional[datetime] = None) -> ScanOutcome:
    """Complete Amazon's monitored technical-title scope without a 10k false baseline."""
    endpoint = "https://www.amazon.jobs/en/search.json"
    unique_jobs: Dict[str, Dict[str, Any]] = {}
    incomplete_details = []

    # Amazon's search shards are independent. Run a small bounded inner pool
    # so the one very large employer does not monopolize the monitor's wall
    # time; all shard requests still inherit the same source deadline.
    with ThreadPoolExecutor(max_workers=min(AMAZON_SHARD_WORKERS, len(ROLE_POLICY_QUERY_SHARDS))) as executor:
        futures = {
            executor.submit(fetch_amazon_query_shard, endpoint, search_text, deadline, delta_cutoff): search_text
            for search_text in ROLE_POLICY_QUERY_SHARDS
        }
        for future in as_completed(futures):
            search_text = futures[future]
            try:
                shard_jobs, shard_complete, detail = future.result()
            except (TimeoutError, requests.RequestException, TypeError, ValueError) as exc:
                # One failed shard degrades this source to PARTIAL; it must not
                # discard the shards that completed successfully.
                incomplete_details.append(f"{search_text!r}: {type(exc).__name__}: {exc}")
                continue
            for job in shard_jobs:
                unique_jobs[job["job_key"]] = job
            if not shard_complete:
                incomplete_details.append(detail)

    if incomplete_details:
        return ScanOutcome(
            list(unique_jobs.values()),
            False,
            "Amazon role-policy scan incomplete; no baseline advanced. " + " | ".join(incomplete_details[:3]),
        )

    return ScanOutcome(
        list(unique_jobs.values()),
        True,
        f"Fetched {len(unique_jobs)} unique Amazon postings across {len(ROLE_POLICY_QUERY_SHARDS)} role-policy query shards.",
    )


def canonical_microsoft_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one record from Microsoft's public Eightfold/PCSX search."""
    identifier = job.get("id") or job.get("displayJobId") or job.get("atsJobId")
    path = as_text(job.get("positionUrl"))
    url = urljoin("https://apply.careers.microsoft.com", path) if path else "https://apply.careers.microsoft.com/careers"
    return canonical_job(
        stable_key("microsoft", "microsoft.com", identifier),
        job.get("name") or job.get("title"),
        job.get("locations") or job.get("standardizedLocations"),
        job.get("postedTs") or job.get("creationTs"),
        url,
        "Microsoft Eightfold postedTs",
    )


def fetch_microsoft_query_shard(
    spec: Dict[str, Any], search_text: str, deadline: float, delta_cutoff: Optional[datetime] = None
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """Exhaust one official Microsoft careers query using its declared count.

    ``sort_by=timestamp`` (newest-first) plus an active delta cutoff stops
    pagination once a whole page predates the cutoff.
    """
    endpoint = "https://apply.careers.microsoft.com/api/pcsx/search"
    collected: List[Dict[str, Any]] = []
    seen_keys = set()
    start = 0
    pages = 0
    total_hint: Optional[int] = None

    while pages < MICROSOFT_MAX_PAGES_PER_QUERY:
        response = request_json(
            "GET",
            endpoint,
            deadline,
            params={
                "domain": spec.get("domain", "microsoft.com"),
                "query": search_text,
                "location": "United States",
                "start": start,
                "sort_by": "timestamp",
            },
            extra_headers={"Referer": "https://apply.careers.microsoft.com/careers?sort_by=timestamp"},
        )
        payload = response.get("data") if isinstance(response, dict) else None
        if not isinstance(payload, dict):
            raise TypeError(f"Microsoft careers response has no data object for query {search_text!r}")
        page = payload.get("positions")
        if not isinstance(page, list):
            raise TypeError(f"Microsoft careers response has no positions list for query {search_text!r}")
        if total_hint is None:
            total_hint = as_positive_int(payload.get("count"))

        if not page:
            # The direct API's terminal page is authoritative even if its
            # count changed during a live scan.
            return collected, True, f"{search_text!r}: terminal empty page after {pages} page(s); count hint={total_hint}."

        new_on_page = 0
        for raw_job in page:
            job = canonical_microsoft_job(raw_job)
            if job["job_key"] in seen_keys:
                continue
            seen_keys.add(job["job_key"])
            new_on_page += 1
            collected.append(job)

        pages += 1
        start += len(page)
        if delta_cutoff is not None:
            ages = [job["source_date"] for job in collected[-len(page) :]]
            if ages and all(age is not None and age < delta_cutoff for age in ages):
                return (
                    collected,
                    True,
                    f"{search_text!r}: delta scan stopped at cutoff after {pages} page(s); "
                    "older postings skipped by design.",
                )
        if new_on_page == 0:
            return collected, False, f"{search_text!r}: Microsoft careers repeated a page at start {start - len(page)}."
        if len(page) < MICROSOFT_PAGE_SIZE:
            return collected, True, f"{search_text!r}: terminal short page after {pages} page(s); count hint={total_hint}."
        if total_hint is not None and start >= total_hint and len(seen_keys) == total_hint:
            return collected, True, f"{search_text!r}: validated {total_hint} unique postings across {pages} page(s)."

    return (
        collected,
        False,
        f"{search_text!r}: reached {MICROSOFT_MAX_PAGES_PER_QUERY} pages; no baseline advanced.",
    )


def fetch_microsoft(spec: Dict[str, Any], deadline: float, delta_cutoff: Optional[datetime] = None) -> ScanOutcome:
    """Complete Microsoft's US technical-title scope via its current public API."""
    unique_jobs: Dict[str, Dict[str, Any]] = {}
    incomplete_details = []

    with ThreadPoolExecutor(max_workers=min(MICROSOFT_SHARD_WORKERS, len(ROLE_POLICY_QUERY_SHARDS))) as executor:
        futures = {
            executor.submit(fetch_microsoft_query_shard, spec, search_text, deadline, delta_cutoff): search_text
            for search_text in ROLE_POLICY_QUERY_SHARDS
        }
        for future in as_completed(futures):
            search_text = futures[future]
            try:
                shard_jobs, shard_complete, detail = future.result()
            except (TimeoutError, requests.RequestException, TypeError, ValueError) as exc:
                # One failed shard degrades this source to PARTIAL; it must not
                # discard the shards that completed successfully.
                incomplete_details.append(f"{search_text!r}: {type(exc).__name__}: {exc}")
                continue
            for job in shard_jobs:
                unique_jobs[job["job_key"]] = job
            if not shard_complete:
                incomplete_details.append(detail)

    if incomplete_details:
        return ScanOutcome(
            list(unique_jobs.values()),
            False,
            "Microsoft role-policy scan incomplete; no baseline advanced. " + " | ".join(incomplete_details[:3]),
        )
    return ScanOutcome(
        list(unique_jobs.values()),
        True,
        f"Fetched {len(unique_jobs)} unique Microsoft postings across {len(ROLE_POLICY_QUERY_SHARDS)} role-policy query shards.",
    )


def canonical_eightfold_job(spec: Dict[str, Any], job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a record from a company-owned Eightfold/PCSX endpoint."""
    identifier = job.get("id") or job.get("displayJobId") or job.get("atsJobId")
    path = as_text(job.get("positionUrl"))
    url = urljoin(f"https://{spec['host']}", path) if path else spec["fallback_url"]
    return canonical_job(
        stable_key("eightfold", spec["domain"], identifier),
        job.get("name") or job.get("title"),
        job.get("locations") or job.get("standardizedLocations"),
        job.get("postedTs") or job.get("creationTs"),
        url,
        "Eightfold PCSX postedTs",
    )


def fetch_eightfold_query_shard(
    spec: Dict[str, Any], search_text: str, deadline: float, delta_cutoff: Optional[datetime] = None
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """Exhaust one public Eightfold title query and validate its pagination."""
    endpoint = f"https://{spec['host']}/api/pcsx/search"
    collected: List[Dict[str, Any]] = []
    seen_keys = set()
    start = 0
    pages = 0
    total_hint: Optional[int] = None

    while pages < EIGHTFOLD_MAX_PAGES_PER_QUERY:
        response = request_json(
            "GET",
            endpoint,
            deadline,
            params={
                "domain": spec["domain"],
                "query": search_text,
                "location": "United States",
                "start": start,
                "sort_by": "timestamp",
            },
            extra_headers={"Referer": f"https://{spec['host']}/careers"},
        )
        payload = response.get("data") if isinstance(response, dict) else None
        if not isinstance(payload, dict):
            raise TypeError(f"Eightfold response has no data object for query {search_text!r}")
        page = payload.get("positions")
        if not isinstance(page, list):
            raise TypeError(f"Eightfold response has no positions list for query {search_text!r}")
        if total_hint is None:
            total_hint = as_positive_int(payload.get("count"))

        if not page:
            return collected, True, f"{search_text!r}: terminal empty page after {pages} page(s); count hint={total_hint}."

        new_on_page = 0
        for raw_job in page:
            job = canonical_eightfold_job(spec, raw_job)
            if job["job_key"] in seen_keys:
                continue
            seen_keys.add(job["job_key"])
            new_on_page += 1
            collected.append(job)

        pages += 1
        start += len(page)
        if delta_cutoff is not None:
            ages = [job["source_date"] for job in collected[-len(page) :]]
            if ages and all(age is not None and age < delta_cutoff for age in ages):
                return (
                    collected,
                    True,
                    f"{search_text!r}: delta scan stopped at cutoff after {pages} page(s); "
                    "older postings skipped by design.",
                )
        if new_on_page == 0:
            return collected, False, f"{search_text!r}: Eightfold repeated a page at start {start - len(page)}."
        if len(page) < EIGHTFOLD_PAGE_SIZE:
            return collected, True, f"{search_text!r}: terminal short page after {pages} page(s); count hint={total_hint}."
        if total_hint is not None and start >= total_hint and len(seen_keys) == total_hint:
            return collected, True, f"{search_text!r}: validated {total_hint} unique postings across {pages} page(s)."

    return (
        collected,
        False,
        f"{search_text!r}: reached {EIGHTFOLD_MAX_PAGES_PER_QUERY} pages; no baseline advanced.",
    )


def fetch_eightfold(spec: Dict[str, Any], deadline: float, delta_cutoff: Optional[datetime] = None) -> ScanOutcome:
    """Complete a verified Eightfold company's US technical-title scope."""
    unique_jobs: Dict[str, Dict[str, Any]] = {}
    incomplete_details = []

    with ThreadPoolExecutor(max_workers=min(EIGHTFOLD_SHARD_WORKERS, len(ROLE_POLICY_QUERY_SHARDS))) as executor:
        futures = {
            executor.submit(fetch_eightfold_query_shard, spec, search_text, deadline, delta_cutoff): search_text
            for search_text in ROLE_POLICY_QUERY_SHARDS
        }
        for future in as_completed(futures):
            search_text = futures[future]
            try:
                shard_jobs, shard_complete, detail = future.result()
            except (TimeoutError, requests.RequestException, TypeError, ValueError) as exc:
                # One failed shard degrades this source to PARTIAL; it must not
                # discard the shards that completed successfully.
                incomplete_details.append(f"{search_text!r}: {type(exc).__name__}: {exc}")
                continue
            for job in shard_jobs:
                unique_jobs[job["job_key"]] = job
            if not shard_complete:
                incomplete_details.append(detail)

    if incomplete_details:
        return ScanOutcome(
            list(unique_jobs.values()),
            False,
            f"{spec['company']} Eightfold role-policy scan incomplete; no baseline advanced. "
            + " | ".join(incomplete_details[:3]),
        )
    return ScanOutcome(
        list(unique_jobs.values()),
        True,
        f"Fetched {len(unique_jobs)} unique {spec['company']} Eightfold postings across "
        f"{len(ROLE_POLICY_QUERY_SHARDS)} role-policy query shards.",
    )


def fetch_jobs(spec: Dict[str, Any], deadline: float, delta_cutoff: Optional[datetime] = None) -> ScanOutcome:
    if spec["kind"] == "greenhouse":
        return fetch_greenhouse(spec, deadline)
    if spec["kind"] == "ashby":
        return fetch_ashby(spec, deadline)
    if spec["kind"] == "lever":
        return fetch_lever(spec, deadline)
    if spec["kind"] == "workday":
        return fetch_workday(spec, deadline)
    if spec["kind"] == "amazon":
        return fetch_amazon(spec, deadline, delta_cutoff=delta_cutoff)
    if spec["kind"] == "microsoft":
        return fetch_microsoft(spec, deadline, delta_cutoff=delta_cutoff)
    if spec["kind"] == "eightfold":
        return fetch_eightfold(spec, deadline, delta_cutoff=delta_cutoff)
    if spec["kind"] == "apple_html":
        return fetch_apple_html(spec, deadline, delta_cutoff=delta_cutoff)
    if spec["kind"] == "phenom_html":
        return fetch_phenom_html(spec, deadline)
    if spec["kind"] == "google_html":
        return fetch_google_html(spec, deadline)
    if spec["kind"] == "nutanix_html":
        return fetch_nutanix_html(spec, deadline)
    if spec["kind"] == "roku_html":
        return fetch_roku_html(spec, deadline)
    raise ValueError(f"No public adapter for {spec['kind']}")


# ---------------------------------------------------------------------------
# Candidate enrichment, source health, and durable delta state
# ---------------------------------------------------------------------------
JOB_COLUMNS = [
    "Company",
    "ATS",
    "Position",
    "Location",
    "Entry Evidence",
    "Profile Fit",
    "Why It Matches",
    "Source Date (UTC)",
    "Date Basis",
    "First Seen (UTC)",
    "Monitor State",
    "Source Scan",
    "URL",
]
HEALTH_COLUMNS = [
    "Company",
    "ATS",
    "Scan Status",
    "Completeness",
    "Source Jobs",
    "Eligible Jobs",
    "Current Eligible Result",
    "Scanned At (UTC)",
    "Board URL",
    "Detail",
]


def candidate_metadata(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    evidence = entry_evidence(job["title"])
    if evidence is None or not is_us_only_location(job["location"]):
        return None
    profile_label, rationale = profile_fit(job["title"])
    candidate = dict(job)
    candidate["entry_evidence"] = evidence
    candidate["profile_fit"] = profile_label
    candidate["profile_rationale"] = rationale
    return candidate


def health_row(
    spec: Dict[str, Any],
    status: str,
    completeness: str,
    source_jobs: Optional[int],
    eligible_jobs: Optional[int],
    detail: str,
    scanned_at: datetime,
    current_eligible_result: str = "Unverified - source did not complete",
) -> Dict[str, Any]:
    return {
        "Company": spec["company"],
        "ATS": spec["ats"],
        "Scan Status": status,
        "Completeness": completeness,
        "Source Jobs": source_jobs,
        "Eligible Jobs": eligible_jobs,
        "Current Eligible Result": current_eligible_result,
        "Scanned At (UTC)": scanned_at,
        "Board URL": spec["fallback_url"],
        "Detail": detail,
    }


def delta_cutoff_for(spec: Dict[str, Any], conn: sqlite3.Connection) -> Optional[datetime]:
    """Return the newest-first stop-early cutoff for a source, or None.

    A source is eligible for fast-delta scanning only when fast delta mode is
    on, the feed has a verified date-sort contract, and its previous scan
    completed with a matching adapter fingerprint. The safety window absorbs
    clock skew and postings whose employer timestamp updates after opening.
    """
    if not FAST_DELTA_MODE:
        return None
    if spec.get("kind") not in DELTA_SORTED_KINDS:
        return None
    row = conn.execute(
        "SELECT last_completed_at, adapter_fingerprint FROM source_state WHERE company = ?",
        (spec["company"],),
    ).fetchone()
    if not row:
        return None
    fingerprint = source_fingerprint(spec)
    if row[1] != fingerprint:
        return None
    last_completed = parse_utc(row[0])
    if last_completed is None:
        return None
    return last_completed - timedelta(hours=DELTA_SAFETY_WINDOW_HOURS)


def collect_one(spec: Dict[str, Any], delta_cutoff: Optional[datetime] = None) -> Dict[str, Any]:
    """Scan exactly one company and return jobs plus a truthful health record."""
    scanned_at = datetime.now(UTC)
    if spec["kind"] == "fallback":
        return {
            "company": spec["company"],
            "complete": False,
            "source_jobs": [],
            "candidates": [],
            "health": health_row(
                spec,
                "NOT_POLLED",
                "Not scanned",
                None,
                None,
                "No reliable public API configured; use the first-party portal directly.",
                scanned_at,
            ),
        }

    try:
        outcome = fetch_jobs(spec, time.monotonic() + SOURCE_SCAN_BUDGET_SECONDS, delta_cutoff=delta_cutoff)
    except TimeoutError as exc:
        return {
            "company": spec["company"],
            "complete": False,
            "source_jobs": [],
            "candidates": [],
            "health": health_row(spec, "TIMEOUT", "Not scanned", None, None, str(exc), scanned_at),
        }
    except requests.RequestException as exc:
        return {
            "company": spec["company"],
            "complete": False,
            "source_jobs": [],
            "candidates": [],
            "health": health_row(spec, "UNAVAILABLE", "Not scanned", None, None, f"{type(exc).__name__}: {exc}", scanned_at),
        }
    except (TypeError, ValueError, KeyError) as exc:
        return {
            "company": spec["company"],
            "complete": False,
            "source_jobs": [],
            "candidates": [],
            "health": health_row(spec, "PARSE_ERROR", "Not scanned", None, None, f"{type(exc).__name__}: {exc}", scanned_at),
        }

    source_jobs = [
        dict(job, company=spec["company"], ats=spec["ats"])
        for job in outcome.jobs
    ]
    candidates = []
    for job in source_jobs:
        candidate = candidate_metadata(job)
        if candidate is None:
            continue
        candidates.append(candidate)

    status = "COMPLETE" if outcome.complete else "PARTIAL"
    completeness = "Complete" if outcome.complete else "Partial - excluded from delta detection"
    if outcome.complete:
        current_eligible_result = (
            "No Active Entry-Level CS Postings"
            if not candidates
            else f"{len(candidates)} active eligible posting(s)"
        )
    else:
        current_eligible_result = "Partial result - never used for alerts"
    return {
        "company": spec["company"],
        "complete": outcome.complete,
        "source_jobs": source_jobs,
        "candidates": candidates,
        # 'delta' snapshots cover only recent postings; reconcile uses this to
        # decide whether REOPENED detection is valid on the next run.
        "scan_mode": "delta" if delta_cutoff is not None and outcome.complete else "full",
        "health": health_row(
            spec,
            status,
            completeness,
            len(source_jobs),
            len(candidates),
            outcome.detail,
            scanned_at,
            current_eligible_result,
        ),
    }


def source_fingerprint(spec: Dict[str, Any]) -> str:
    """Return the identity contract used to compare source snapshots.

    If an ATS, board slug, or adapter changes, the next successful scan must be
    a re-baseline. Otherwise a provider migration can look like hundreds of
    newly opened roles even though none of them is new.
    """
    identity_fields = ("kind", "board", "host", "tenant", "site", "domain", "fallback_url")
    payload = {
        "protocol": STATE_PROTOCOL_VERSION,
        "source": {key: spec.get(key) for key in identity_fields if key in spec},
    }
    if spec.get("kind") in {"workday", "amazon", "microsoft", "eightfold"}:
        # Changing a source-query shard changes the observable universe. Force
        # one safe re-baseline rather than mislabeling pre-existing jobs NEW.
        payload["role_policy_query_version"] = ROLE_POLICY_QUERY_VERSION
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def ensure_state_database() -> sqlite3.Connection:
    """Mount Drive if needed, then initialize/migrate durable monitor state."""
    drive_root = os.path.join(GOOGLE_DRIVE_MOUNT_POINT, "MyDrive")
    if AUTO_MOUNT_GOOGLE_DRIVE and os.path.abspath(STATE_DB_PATH).startswith(os.path.abspath(GOOGLE_DRIVE_MOUNT_POINT)):
        if not os.path.isdir(drive_root):
            try:
                from google.colab import drive  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "Google Drive is required for durable monitor state. Run in Colab or set STATE_DB_PATH to another durable path."
                ) from exc
            drive.mount(GOOGLE_DRIVE_MOUNT_POINT, force_remount=False)
        if not os.path.isdir(drive_root):
            raise RuntimeError("Google Drive did not mount; refusing to run without durable state.")

    state_dir = os.path.dirname(os.path.abspath(STATE_DB_PATH))
    os.makedirs(state_dir, exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH, timeout=30)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_state (
            company TEXT PRIMARY KEY,
            first_completed_at TEXT NOT NULL,
            last_completed_at TEXT NOT NULL,
            adapter_fingerprint TEXT,
            last_scan_mode TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_state (
            job_key TEXT PRIMARY KEY,
            company TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            title TEXT NOT NULL,
            location TEXT,
            url TEXT,
            source_date TEXT,
            date_basis TEXT,
            entry_evidence TEXT,
            profile_fit TEXT
        )
        """
    )
    # State created by an older filtered-only collector is not trusted for
    # delta detection. A missing fingerprint forces exactly one re-baseline.
    add_column_if_missing(conn, "source_state", "adapter_fingerprint", "TEXT")
    add_column_if_missing(conn, "source_state", "last_scan_mode", "TEXT")
    for column, definition in (
        ("location", "TEXT"),
        ("url", "TEXT"),
        ("source_date", "TEXT"),
        ("date_basis", "TEXT"),
        ("entry_evidence", "TEXT"),
        ("profile_fit", "TEXT"),
    ):
        add_column_if_missing(conn, "job_state", column, definition)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_job_state_company ON job_state(company)")
    conn.commit()
    return conn


def iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.astimezone(UTC).isoformat() if value else None


def display_job_row(
    job: Dict[str, Any],
    first_seen: Optional[datetime],
    monitor_state: str,
    source_scan: str,
) -> Dict[str, Any]:
    return {
        "Company": job["company"],
        "ATS": job["ats"],
        "Position": job["title"],
        "Location": job["location"],
        "Entry Evidence": job["entry_evidence"],
        "Profile Fit": job["profile_fit"],
        "Why It Matches": job["profile_rationale"],
        "Source Date (UTC)": job["source_date"],
        "Date Basis": job["date_basis"],
        "First Seen (UTC)": first_seen,
        "Monitor State": monitor_state,
        "Source Scan": source_scan,
        "URL": job["url"],
    }


def upsert_source_job(
    conn: sqlite3.Connection,
    job: Dict[str, Any],
    first_seen: datetime,
    observed_at: datetime,
    candidate: Optional[Dict[str, Any]],
) -> None:
    """Persist every source identity, including currently ineligible roles."""
    conn.execute(
        """
        INSERT INTO job_state (
            job_key, company, first_seen_at, last_seen_at, title, location,
            url, source_date, date_basis, entry_evidence, profile_fit
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_key) DO UPDATE SET
            company=excluded.company,
            last_seen_at=excluded.last_seen_at,
            title=excluded.title,
            location=excluded.location,
            url=excluded.url,
            source_date=excluded.source_date,
            date_basis=excluded.date_basis,
            entry_evidence=excluded.entry_evidence,
            profile_fit=excluded.profile_fit
        """,
        (
            job["job_key"],
            job["company"],
            iso_or_none(first_seen) or iso_or_none(observed_at),
            iso_or_none(observed_at),
            job["title"],
            job["location"],
            job["url"],
            iso_or_none(job["source_date"]),
            job["date_basis"],
            candidate["entry_evidence"] if candidate else None,
            candidate["profile_fit"] if candidate else None,
        ),
    )


def reconcile_complete_scans(
    conn: sqlite3.Connection,
    scans: List[Dict[str, Any]],
    observed_at: datetime,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Commit complete snapshots and return (new/reopened, current) rows.

    The source snapshot is persisted before eligibility filtering. A role that
    already existed but becomes eligible after a title/location edit is KNOWN,
    not NEW. A previously absent stable ID is labelled REOPENED.
    """
    delta_rows: List[Dict[str, Any]] = []
    current_rows: List[Dict[str, Any]] = []
    specs_by_company = {spec["company"]: spec for spec in TARGETS}

    for scan in scans:
        if not scan["complete"]:
            continue
        company = scan["company"]
        fingerprint = source_fingerprint(specs_by_company[company])
        source_row = conn.execute(
            "SELECT first_completed_at, last_completed_at, adapter_fingerprint, last_scan_mode "
            "FROM source_state WHERE company = ?",
            (company,),
        ).fetchone()
        # REOPENED detection needs the previous snapshot to have observed every
        # posting of the feed: absence must mean "closed," not "not fetched."
        # A delta snapshot only covers recent postings, so a delta-then-delta
        # (or unknown-mode) transition forces a one-run re-baseline instead.
        adapter_matches = bool(source_row and source_row[2] == fingerprint)
        # NEW detection is safe whenever the PREVIOUS completed scan persisted a
        # full snapshot of this feed: a stable ID absent from job_state genuinely
        # was not there before, whether this run is full or delta.
        prior_is_full = bool(adapter_matches and source_row[3] == "full")
        # REOPENED detection additionally needs THIS run to have observed every
        # posting of the feed: absence must mean "closed," not "not fetched."
        # A delta snapshot only covers recent postings, so REOPENED is evaluated
        # only when the current scan itself is full.
        reopened_detection = prior_is_full and scan.get("scan_mode") == "full"
        prior_completed_at = parse_utc(source_row[1]) if reopened_detection else None
        prior_jobs = {}
        if adapter_matches:
            prior_jobs = {
                row[0]: {"first_seen_at": row[1], "last_seen_at": row[2]}
                for row in conn.execute(
                    "SELECT job_key, first_seen_at, last_seen_at FROM job_state WHERE company = ?",
                    (company,),
                )
            }

        candidates_by_key = {job["job_key"]: job for job in scan["candidates"]}
        state_by_key: Dict[str, Tuple[datetime, str]] = {}
        for source_job in scan["source_jobs"]:
            key = source_job["job_key"]
            previous = prior_jobs.get(key)
            if not adapter_matches:
                first_seen = observed_at
                state = "REBASELINE" if source_row else "BASELINE"
            elif previous is None:
                first_seen, state = observed_at, "NEW"
            else:
                first_seen = parse_utc(previous["first_seen_at"]) or observed_at
                last_seen = parse_utc(previous["last_seen_at"])
                was_absent = bool(prior_completed_at and last_seen and last_seen < prior_completed_at)
                state = "REOPENED" if was_absent else "KNOWN"

            candidate = candidates_by_key.get(key)
            upsert_source_job(conn, source_job, first_seen, observed_at, candidate)
            state_by_key[key] = (first_seen, state)

        for candidate in scan["candidates"]:
            first_seen, state = state_by_key[candidate["job_key"]]
            row = display_job_row(candidate, first_seen, state, "COMPLETE")
            current_rows.append(row)
            if state in {"NEW", "REOPENED"}:
                delta_rows.append(dict(row))

        now = iso_or_none(observed_at)
        first_completed_at = source_row[0] if adapter_matches and source_row else now
        conn.execute(
            """
            INSERT INTO source_state (
                company, first_completed_at, last_completed_at, adapter_fingerprint, last_scan_mode
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(company) DO UPDATE SET
                first_completed_at=excluded.first_completed_at,
                last_completed_at=excluded.last_completed_at,
                adapter_fingerprint=excluded.adapter_fingerprint,
                last_scan_mode=excluded.last_scan_mode
            """,
            (company, first_completed_at, now, fingerprint, scan.get("scan_mode", "full")),
        )

    conn.commit()
    return delta_rows, current_rows


def partial_current_rows(scans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Expose partial-source candidates without ever classifying them as new."""
    rows: List[Dict[str, Any]] = []
    for scan in scans:
        if scan["complete"]:
            continue
        status = scan["health"]["Scan Status"]
        for job in scan["candidates"]:
            rows.append(display_job_row(job, None, "PARTIAL - NOT PERSISTED", status))
    return rows


# ---------------------------------------------------------------------------
# Presentation and execution
# ---------------------------------------------------------------------------
def job_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=JOB_COLUMNS)
    for column in ("Source Date (UTC)", "First Seen (UTC)"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame.sort_values(
        ["First Seen (UTC)", "Source Date (UTC)", "Company", "Position"],
        ascending=[False, False, True, True],
        na_position="last",
    ).reset_index(drop=True)


def health_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=HEALTH_COLUMNS)
    frame["Scanned At (UTC)"] = pd.to_datetime(frame["Scanned At (UTC)"], utc=True, errors="coerce")
    return frame.sort_values(["Scan Status", "Company"], ascending=[True, True]).reset_index(drop=True)


def display_timestamp(value: Any) -> str:
    return "-" if pd.isna(value) else pd.Timestamp(value).isoformat()


def print_job_log(label: str, frame: pd.DataFrame) -> None:
    print(f"\n{label}: {len(frame)}")
    if frame.empty:
        return
    for index, row in frame.iterrows():
        print(
            f"[{index + 1:02d}] {row['Company']} ({row['ATS']}) - {row['Position']} | "
            f"{row['Location']} | first seen: {display_timestamp(row['First Seen (UTC)'])} | "
            f"source date: {display_timestamp(row['Source Date (UTC)'])} | "
            f"{row['Entry Evidence']} | {row['Profile Fit']} | {row['URL']}"
        )


def run() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run all sources, persist only complete scans, and render three honest tables."""
    observed_at = datetime.now(UTC)
    conn = ensure_state_database()
    scans: List[Dict[str, Any]] = []

    try:
        # Cutoffs must be read BEFORE workers launch: the previous run's
        # completion timestamps define each source's delta window.
        cutoffs = {spec["company"]: delta_cutoff_for(spec, conn) for spec in TARGETS}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="jobs") as executor:
            futures = {executor.submit(collect_one, spec, cutoffs[spec["company"]]): spec for spec in TARGETS}
            for future in as_completed(futures):
                spec = futures[future]
                try:
                    scans.append(future.result())
                except Exception as exc:  # Defensive guard for unexpected worker defects.
                    scans.append(
                        {
                            "company": spec["company"],
                            "complete": False,
                            "source_jobs": [],
                            "candidates": [],
                            "health": health_row(
                                spec,
                                "WORKER_ERROR",
                                "Not scanned",
                                None,
                                None,
                                f"{type(exc).__name__}: {exc}",
                                datetime.now(UTC),
                            ),
                        }
                    )

        returned = {scan["company"] for scan in scans}
        assert returned == EXPECTED_COMPANIES and len(scans) == EXPECTED_COMPANY_COUNT, (
            f"Coverage failure: missing={sorted(EXPECTED_COMPANIES - returned)}, "
            f"extra={sorted(returned - EXPECTED_COMPANIES)}"
        )

        new_rows, complete_current_rows = reconcile_complete_scans(conn, scans, observed_at)
        all_current_rows = complete_current_rows + partial_current_rows(scans)
    finally:
        conn.close()

    df = job_frame(new_rows)
    current_jobs_df = job_frame(all_current_rows)
    source_health_df = health_frame([scan["health"] for scan in scans])

    pd.set_option("display.max_rows", max(EXPECTED_COMPANY_COUNT + 10, len(current_jobs_df) + 10))
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", None)

    complete_count = int((source_health_df["Scan Status"] == "COMPLETE").sum())
    partial_count = int((source_health_df["Scan Status"] == "PARTIAL").sum())
    failed_count = len(source_health_df) - complete_count - partial_count
    print(
        f"Monitor completed at {observed_at.isoformat()} | complete={complete_count} | "
        f"partial={partial_count} | not-complete={failed_count}"
    )
    if df.empty:
        print("No newly observed or reopened jobs from complete sources. On a first run, this is the expected baseline result.")

    print_job_log("NEW OR REOPENED JOBS SINCE LAST SUCCESSFUL COMPLETE SCAN", df)
    display(df)

    if SHOW_CURRENT_MATCHES:
        print_job_log("CURRENT ELIGIBLE JOBS (COMPLETE + PARTIAL SOURCES)", current_jobs_df)
        display(current_jobs_df)

    print("\nSOURCE HEALTH (not job listings):")
    display(source_health_df)
    return df, current_jobs_df, source_health_df


if __name__ == "__main__":
    df, current_jobs_df, source_health_df = run()
