from __future__ import annotations
import hashlib, sys, sqlite3
from typing import List, Optional, Dict
from datetime import datetime, timezone
import httpx

from .core import request_with_backoff, _parse_iso, _iso, USER_AGENT

BASE = "https://boards-api.greenhouse.io/v1/boards/{token}"

def fetch_board_name(client: httpx.Client, token: str) -> str:
    url = BASE.format(token=token)
    r = request_with_backoff(client, "GET", url, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        raise ValueError(f"Board token not found: {token}")
    r.raise_for_status()
    try:
        return r.json().get("name") or token
    except Exception:
        return token

def fetch_jobs(client: httpx.Client, token: str, since_http_date: Optional[str], limiter) -> List[Dict[str, str]]:
    url = BASE.format(token=token) + "/jobs"
    headers = {"User-Agent": USER_AGENT}
    params = {"content": "true"}
    if since_http_date:
        headers["If-Modified-Since"] = since_http_date
    r = request_with_backoff(client, "GET", url, headers=headers, params=params, limiter=limiter)
    if r.status_code == 304:
        return []
    r.raise_for_status()
    data = r.json()
    return data.get("jobs", [])

def upsert_job(conn: sqlite3.Connection, board_token: str, org_name: str, j: dict) -> None:
    url = (j.get("absolute_url") or "").strip()
    if not url:
        return
    title = " ".join((j.get("title") or "").split())
    loc = ((j.get("location") or {}).get("name") or "").strip()
    desc = (j.get("content") or "").strip()
    posted = _parse_iso(j.get("updated_at") or j.get("created_at"))
    canonical_key = hashlib.md5(url.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc)

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
            (board_token, org_name, title, desc, loc or None, url,
             _parse_iso(j.get("updated_at") or j.get("created_at")).isoformat() if posted else None,
             now.isoformat(), now.isoformat(), canonical_key),
        )
    except sqlite3.DatabaseError as e:
        print(f"[WARN] upsert failed for {url}: {e}", file=sys.stderr)
