# fetch.py
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml

from job_agent.core import (
    open_db, load_state, save_state, http_date, RateLimiter, USER_AGENT
)
from job_agent.greenhouse import (
    fetch_board_name as gh_fetch_board_name,
    fetch_jobs as gh_fetch_jobs,
    upsert_job as gh_upsert_job,
)
from job_agent import lever as lever_mod
from job_agent import workday as workday_mod

BASE_DIR = Path(__file__).resolve().parent

def load_registry(path: str = str(BASE_DIR / "registry.yaml")):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

# ----------------- Greenhouse -----------------
def fetch_greenhouse(tokens, rate: float):
    state = load_state()
    limiter = RateLimiter(rate_per_min=rate)
    conn = open_db()
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=20) as client:
        for token in tokens or []:
            token = (token or "").strip()
            if not token:
                continue

            since_hdr = None
            if token in state and "last_fetch" in state[token]:
                try:
                    since_dt = datetime.fromisoformat(state[token]["last_fetch"])
                    since_hdr = http_date(since_dt)
                except Exception:
                    since_hdr = None

            try:
                org_name = gh_fetch_board_name(client, token)
            except Exception as e:
                print(f"[ERROR] board '{token}': {e}")
                continue

            try:
                jobs = gh_fetch_jobs(client, token, since_hdr, limiter)
            except Exception as e:
                print(f"[ERROR] jobs '{token}': {e}")
                continue

            print(f"[INFO] GH {token} ({org_name}): {len(jobs)} jobs fetched")
            with conn:
                for j in jobs:
                    gh_upsert_job(conn, token, org_name, j)

            state.setdefault(token, {})["last_fetch"] = datetime.now(timezone.utc).isoformat()

    save_state(state)
    conn.close()

# ----------------- Lever -----------------
def fetch_lever(companies, rate: float):
    limiter = RateLimiter(rate_per_min=rate)
    conn = open_db()
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=20) as client:
        for company in companies or []:
            company = (company or "").strip()
            if not company:
                continue
            try:
                jobs = lever_mod.fetch_jobs(client, company, limiter)
            except Exception as e:
                print(f"[ERROR] lever '{company}': {e}")
                continue
            print(f"[INFO] LV {company}: {len(jobs)} jobs fetched")
            with conn:
                for j in jobs:
                    lever_mod.upsert_job(conn, company, j)
    conn.close()

# ----------------- Workday -----------------
def fetch_workday(cxs_endpoints, rate: float):
    # (rate kept for parity, not used inside workday_mod.fetch_jobs)
    conn = open_db()
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=20, follow_redirects=True) as client:
        for cxs_url in cxs_endpoints or []:
            cxs_url = (cxs_url or "").strip()
            if not cxs_url:
                continue
            try:
                posts = workday_mod.fetch_jobs(client, cxs_url)
            except Exception as e:
                print(f"[ERROR] wd '{cxs_url}': {e}")
                continue
            print(f"[INFO] WD {cxs_url}: {len(posts)} jobs fetched")
            with conn:
                for rec in posts:
                    workday_mod.upsert_job(conn, cxs_url, rec)
    conn.close()

# ----------------- Entry point -----------------
def main():
    ap = argparse.ArgumentParser(description="Fetch jobs from registry.yaml (Greenhouse + Lever + Workday).")
    ap.add_argument("--rate", type=float, default=30.0, help="Requests per minute (default 30)")
    args = ap.parse_args()

    reg = load_registry()
    gh_tokens    = (reg.get("greenhouse", {}) or {}).get("tokens", [])
    lv_companies = (reg.get("lever", {}) or {}).get("companies", [])
    wd_endpoints = (reg.get("workday", {}) or {}).get("cxs_endpoints", [])

    if gh_tokens:
        fetch_greenhouse(gh_tokens, args.rate)
    if lv_companies:
        fetch_lever(lv_companies, args.rate)
    if wd_endpoints:
        fetch_workday(wd_endpoints, args.rate)

    if not any([gh_tokens, lv_companies, wd_endpoints]):
        print("No sources found in registry.yaml (greenhouse.tokens / lever.companies / workday.cxs_endpoints).")

if __name__ == "__main__":
    main()
