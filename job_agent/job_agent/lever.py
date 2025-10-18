# job_agent/lever.py
from __future__ import annotations
import hashlib, sys, sqlite3
from typing import List, Dict, Optional
from datetime import datetime, timezone
import httpx

from .core import request_with_backoff, USER_AGENT

BASE = "https://api.lever.co/v0/postings/{company}"

def fetch_jobs(client: httpx.Client, company: str, limiter) -> List[Dict]:
    """
    Fetch public Lever postings for a company slug.
    Example URL: https://api.lever.co/v0/postings/<company>?mode=json
    """
    headers = {"User-Agent": USER_AGENT}
    params = {"mode": "json"}
    url = BASE.format(company=company)
    r = request_with_backoff(client, "GET", url, headers=headers, params=params, limiter=limiter)
    if r.status_code == 404:
        raise ValueError(f"Lever company not found: {company}")
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []

def _first_location(candidate) -> Optional[str]:
    # Lever can return a string or a list/dict – normalize to a single string
    if not candidate:
        return None
    if isinstance(candidate, str):
        return candidate
    if isinstance(candidate, dict):
        return candidate.get("name") or candidate.get("location") or None
    if isinstance(candidate, list):
        for item in candidate:
            if isinstance(item, dict) and (item.get("name") or item.get("location")):
                return item.get("name") or item.get("location")
            if isinstance(item, str):
                return item
    return None

def upsert_job(conn: sqlite3.Connection, company: str, j: dict) -> None:
    """
    Map Lever JSON payload to our 'jobs' table.
    """
    url = (j.get("hostedUrl") or j.get("applyUrl") or "").strip()
    if not url:
        return

    # Title
    title = (j.get("text") or j.get("title") or "").strip()

    # Org/team
    org_name = company
    cats = j.get("categories") or {}
    if isinstance(cats, dict):
        org_name = (cats.get("team") or cats.get("department") or org_name) or company

    # Location (try new + old fields)
    loc = (
        _first_location(j.get("workplaceLocations"))
        or cats.get("location")
        or j.get("country")
        or j.get("location")
    )

    # Description
    desc = ""
    if isinstance(j.get("lists"), list):
        desc = "\n".join([str(x.get("text", "")).strip() for x in j["lists"] if isinstance(x, dict)])
    desc = desc or (j.get("descriptionPlain") or j.get("description") or "")

    # Posted time (ms epoch in createdAt/updatedAt)
    posted_at = None
    ts = j.get("createdAt") or j.get("updatedAt")
    if isinstance(ts, (int, float)):
        posted_at = datetime.fromtimestamp(ts/1000.0, tz=timezone.utc).isoformat()

    canonical_key = hashlib.md5(url.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc).isoformat()

    try:
        conn.execute(
            """
            INSERT INTO jobs (board_token, org_name, title, description, location,
                              post_url, posted_at, first_seen_at, last_seen_at, canonical_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_key) DO UPDATE SET
              last_seen_at=excluded.last_seen_at,
              title=excluded.title,
              description=excluded.description,
              location=excluded.location,
              posted_at=COALESCE(excluded.posted_at, jobs.posted_at)
            """,
            (f"lever:{company}", org_name, title, (desc or "").strip(), (loc or None),
             url, posted_at, now, now, canonical_key),
        )
    except sqlite3.DatabaseError as e:
        print(f"[WARN] upsert failed for {url}: {e}", file=sys.stderr)
