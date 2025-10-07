#!/usr/bin/env python3
"""
Greenhouse fetcher + query (rate-limited, no Docker, SQLite + FTS5)

Usage examples:
  # 1) Fetch from one or more boards (tokens), then run a query
  python greenhouse_fetch_and_query.py --boards acme charitywater --fetch
  python greenhouse_fetch_and_query.py --query "engineer" --limit 10

  # 2) One-shot: fetch and query
  python greenhouse_fetch_and_query.py --boards acme --fetch --query "nurse"

Notes:
- "board token" is the last part of https://boards.greenhouse.io/<TOKEN>
- Public API endpoint fetched: https://boards-api.greenhouse.io/v1/boards/<TOKEN>/jobs?content=true
- We use a polite rate limiter.
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import httpx


DB_PATH = "gh_jobs.db"
STATE_PATH = "state.json"  # stores per-board last fetch time (for If-Modified-Since)
USER_AGENT = "JobAgent/1.0 (+https://example.org) Python-HTTPX"  # be a good citizen
BASE = "https://boards-api.greenhouse.io/v1/boards/{token}"
REQUEST_TIMEOUT = 15.0

# ---------- Rate limiter (spreads requests so we don't hammer the API) ----------
class RateLimiter:
    """
    Simple time-based limiter: ensures a minimum gap between requests.
    Example: rate_per_min=30 -> ~2.0s gap between calls.
    """
    def __init__(self, rate_per_min: float = 30.0):
        self.min_gap = 60.0 / float(rate_per_min)
        self._next_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if now < self._next_at:
            time.sleep(self._next_at - now)
        self._next_at = max(self._next_at, now) + self.min_gap


limiter = RateLimiter(rate_per_min=30)  # ~1 request every 2s (tweak as needed)


# ---------- Tiny persistence for conditional fetch ----------
def load_state() -> Dict[str, Dict[str, str]]:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: Dict[str, Dict[str, str]]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_PATH)


# ---------- SQLite schema + FTS ----------
DDL = """
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY,
  board_token TEXT NOT NULL,
  org_name TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  location TEXT,
  post_url TEXT NOT NULL,
  posted_at TEXT,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  canonical_key TEXT NOT NULL UNIQUE
);
"""

FTS_DDL = """
CREATE VIRTUAL TABLE IF NOT EXISTS job_fts
USING fts5(title, description, org_name, location, content='jobs', content_rowid='id');

CREATE TRIGGER IF NOT EXISTS jobs_ai AFTER INSERT ON jobs BEGIN
  INSERT INTO job_fts(rowid, title, description, org_name, location)
  VALUES (new.id, new.title, new.description, new.org_name, COALESCE(new.location,''));
END;

CREATE TRIGGER IF NOT EXISTS jobs_ad AFTER DELETE ON jobs BEGIN
  DELETE FROM job_fts WHERE rowid=old.id;
END;

CREATE TRIGGER IF NOT EXISTS jobs_au AFTER UPDATE ON jobs BEGIN
  UPDATE job_fts SET
    title=new.title,
    description=new.description,
    org_name=new.org_name,
    location=COALESCE(new.location,'')
  WHERE rowid=new.id;
END;
"""


def open_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute(DDL)

    # Try to enable FTS; if FTS5 isn't available in your Python build,
    # queries will fall back to LIKE.
    try:
        for stmt in FTS_DDL.strip().split(";\n\n"):
            if stmt.strip():
                conn.execute(stmt)
        conn.commit()
    except sqlite3.OperationalError:
        # FTS not available; it's okay (we'll use LIKE searching).
        pass
    return conn


# ---------- Greenhouse client (polite, with backoff) ----------
def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def request_with_backoff(client: httpx.Client, method: str, url: str, *, headers=None, params=None) -> httpx.Response:
    """Minimal exponential backoff on 429/5xx, plus global rate limiter."""
    backoff = 1.0
    max_backoff = 16.0
    while True:
        limiter.wait()
        try:
            r = client.request(method, url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        except httpx.RequestError as e:
            # network hiccup -> backoff and retry
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
            continue

        if r.status_code in (429, 500, 502, 503, 504):
            # server asked us to slow down / temporary failure
            retry_after = r.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                time.sleep(max(float(retry_after), backoff))
            else:
                time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
            continue

        return r


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


def fetch_jobs(client: httpx.Client, token: str, since_http_date: Optional[str]) -> List[Dict[str, str]]:
    """Fetch jobs JSON for a board. since_http_date (RFC 7231) is optional; GH may ignore it."""
    url = BASE.format(token=token) + "/jobs"
    headers = {"User-Agent": USER_AGENT}
    params = {"content": "true"}  # include HTML job description

    if since_http_date:
        headers["If-Modified-Since"] = since_http_date  # GH may or may not honor this

    r = request_with_backoff(client, "GET", url, headers=headers, params=params)
    if r.status_code == 304:
        return []
    r.raise_for_status()
    data = r.json()
    return data.get("jobs", [])


# ---------- Upsert into DB ----------
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
            (
                board_token,
                org_name,
                title,
                desc,
                loc or None,
                url,
                _iso(posted),
                _iso(now),
                _iso(now),
                canonical_key,
            ),
        )
        conn.commit()
    except sqlite3.DatabaseError as e:
        # If something odd in the payload, skip but continue
        print(f"[WARN] upsert failed for {url}: {e}", file=sys.stderr)


# ---------- Query helpers ----------
def fts_available(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT 1 FROM job_fts LIMIT 1;")
        return True
    except sqlite3.OperationalError:
        return False


def search(conn: sqlite3.Connection, q: str, loc: Optional[str], limit: int = 20) -> List[Dict[str, str]]:
    if fts_available(conn) and q.strip():
        # FTS5 search
        sql = """
        SELECT jobs.*
        FROM job_fts
        JOIN jobs ON jobs.id=job_fts.rowid
        WHERE job_fts MATCH :q
        """
        params = {"q": q}
        if loc:
            sql += " AND jobs.location = :loc"
            params["loc"] = loc
        sql += " ORDER BY jobs.posted_at DESC NULLS LAST, jobs.first_seen_at DESC LIMIT :limit"
        params["limit"] = limit
        rows = conn.execute(sql, params).fetchall()
    else:
        # Fallback LIKE search
        like = f"%{q}%"
        if loc:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE (title LIKE ? OR description LIKE ?) AND location=?
                ORDER BY posted_at DESC NULLS LAST, first_seen_at DESC
                LIMIT ?
                """,
                (like, like, loc, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE (title LIKE ? OR description LIKE ?)
                ORDER BY posted_at DESC NULLS LAST, first_seen_at DESC
                LIMIT ?
                """,
                (like, like, limit),
            ).fetchall()

    cols = [c[0] for c in conn.execute("PRAGMA table_info(jobs);").fetchall()]
    # ^ Actually returns rows; safer:
    cols = [d[0] for d in conn.execute("SELECT * FROM jobs LIMIT 1").description] if rows else [
        "id", "board_token", "org_name", "title", "description", "location", "post_url",
        "posted_at", "first_seen_at", "last_seen_at", "canonical_key"
    ]
    out = []
    for r in rows:
        rec = dict(zip(cols, r))
        out.append({
            "title": rec["title"],
            "org": rec["org_name"],
            "location": rec["location"],
            "url": rec["post_url"],
            "posted_at": rec["posted_at"],
        })
    return out


# ---------- CLI ----------
def http_date(dt: datetime) -> str:
    # RFC 7231 IMF-fixdate (e.g., Tue, 15 Nov 1994 08:12:31 GMT)
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def do_fetch(board_tokens: List[str]) -> None:
    if not board_tokens:
        print("No boards provided. Use --boards <token> [<token> ...]")
        return

    state = load_state()
    conn = open_db()

    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        for token in board_tokens:
            token = token.strip()
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
                org_name = fetch_board_name(client, token)
            except Exception as e:
                print(f"[ERROR] board '{token}': {e}", file=sys.stderr)
                continue

            try:
                jobs = fetch_jobs(client, token, since_hdr)
            except Exception as e:
                print(f"[ERROR] jobs '{token}': {e}", file=sys.stderr)
                continue

            print(f"[INFO] {token} ({org_name}): {len(jobs)} jobs fetched")
            for j in jobs:
                upsert_job(conn, token, org_name, j)

            # Record last fetch time
            state.setdefault(token, {})["last_fetch"] = datetime.now(timezone.utc).isoformat()

    save_state(state)
    conn.close()


def do_query(q: str, loc: Optional[str], limit: int) -> None:
    conn = open_db()
    results = search(conn, q, loc, limit)
    conn.close()
    if not results:
        print("(no results)")
        return
    for i, r in enumerate(results, 1):
        print(f"{i:>2}. {r['title']}  —  {r['org']}")
        if r.get("location"):
            print(f"    📍 {r['location']}")
        if r.get("posted_at"):
            print(f"    🗓  {r['posted_at']}")
        print(f"    🔗 {r['url']}\n")


def main():
    ap = argparse.ArgumentParser(description="Fetch + query Greenhouse jobs (rate-limited).")
    ap.add_argument("--boards", nargs="*", default=[], help="One or more Greenhouse board tokens, e.g. acme charitywater")
    ap.add_argument("--fetch", action="store_true", help="Fetch/update jobs for the given boards")
    ap.add_argument("--query", type=str, default="", help="Search text (title/description/org/location)")
    ap.add_argument("--loc", type=str, default=None, help="Optional exact location filter (matches normalized text from Greenhouse)")
    ap.add_argument("--limit", type=int, default=20, help="Max results to print")
    ap.add_argument("--rate", type=float, default=30.0, help="Requests per minute (default 30)")
    args = ap.parse_args()

    # update rate limit
    global limiter
    limiter = RateLimiter(rate_per_min=args.rate)

    if args.fetch and not args.boards:
        ap.error("--fetch requires at least one --boards token")

    if args.fetch:
        do_fetch(args.boards)

    if args.query:
        do_query(args.query, args.loc, args.limit)
    elif not args.fetch:
        ap.print_help()


if __name__ == "__main__":
    main()
