# job_agent/workday.py
from __future__ import annotations
import hashlib, sqlite3, sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

import httpx

from .core import USER_AGENT  # reuse your UA
# (We won't use request_with_backoff for POST-with-body; some helpers don't pass json.)

def _guess_referer(cxs_url: str) -> str:
    """
    /wday/cxs/<TENANT>/<SITE>/jobs -> Referer: https://host/en-US/<SITE>
    """
    parts = urlsplit(cxs_url)
    segs = parts.path.strip("/").split("/")
    site = segs[3] if len(segs) >= 5 else ""
    referer_path = f"/en-US/{site}" if site else "/"
    return urlunsplit((parts.scheme, parts.netloc, referer_path, "", ""))

def _base_root(cxs_url: str) -> str:
    p = urlsplit(cxs_url)
    return urlunsplit((p.scheme, p.netloc, "", "", ""))

def _extract_posts(payload: dict):
    # Workday has 2 common shapes
    posts = payload.get("jobPostings")
    if posts is None and isinstance(payload.get("data"), dict):
        posts = payload["data"].get("jobPostings")
    return posts or []

def fetch_jobs(
    client: httpx.Client,
    cxs_jobs_url: str,
    search_text: str = "",
    page_limit: int = 50,
    max_pages: int = 40,
) -> List[Dict]:
    """
    Pull all pages from a Workday cxs endpoint.
    1) Try POST with JSON body (most tenants).
    2) If that fails, try GET with params.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": _guess_referer(cxs_jobs_url),
        "Content-Type": "application/json",
    }

    all_posts: List[Dict] = []
    offset = 0

    for _ in range(max_pages):
        body = {
            "appliedFacets": {},
            "limit": page_limit,
            "offset": offset,
            "searchText": search_text or "",
        }

        # POST with JSON
        r = client.post(cxs_jobs_url, headers=headers, json=body, timeout=20)
        if r.status_code in (400, 405):
            # fallback to GET with params
            r = client.get(
                cxs_jobs_url,
                headers=headers,
                params={"limit": page_limit, "offset": offset, "searchText": search_text or ""},
                timeout=20,
            )
        r.raise_for_status()

        data = r.json()
        posts = _extract_posts(data)
        if not posts:
            break

        all_posts.extend(posts)
        if len(posts) < page_limit:
            break
        offset += page_limit

    return all_posts

def _normalize_url(u: str) -> str:
    from urllib.parse import urlsplit, urlunsplit
    p = urlsplit(u)
    return urlunsplit((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", ""))

def upsert_job(conn: sqlite3.Connection, endpoint_label: str, rec: Dict) -> None:
    """
    Map a Workday jobPosting dict to the common jobs schema.
    """
    title = (rec.get("title") or "").strip()
    if not title:
        return

    subtitles = rec.get("subtitles") if isinstance(rec.get("subtitles"), list) else []
    org_name = (subtitles[0].get("title").strip() if subtitles and isinstance(subtitles[0], dict) else "") or endpoint_label

    location = (rec.get("locationsText") or rec.get("location") or "").strip()
    posted_at = rec.get("postedOn") or rec.get("postedDate") or rec.get("createdOn")

    # Build a public URL from externalPath on the same host
    external_path = rec.get("externalPath") or rec.get("externalUrlPath") or ""
    if not external_path:
        return
    url = _base_root(endpoint_label) + external_path
    url = _normalize_url(url)

    now_iso = datetime.now(timezone.utc).isoformat()
    canonical_key = hashlib.md5(url.encode("utf-8")).hexdigest()

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
            (f"workday:{endpoint_label}", org_name, title, "", location or None,
             url, posted_at, now_iso, now_iso, canonical_key),
        )
        # optional normalization (if you created normalize.py)
        try:
            from .normalize import normalize_fields
            norm = normalize_fields(title, "", location)
            conn.execute(
                """UPDATE jobs
                   SET role_level=?, work_type=?, employment_type=?, city=?, state=?, country=?, remote=COALESCE(remote, ?)
                   WHERE canonical_key=?""",
                (norm["role_level"], norm["work_type"], norm["employment_type"],
                 norm["city"], norm["state"], norm["country"], norm["remote"], canonical_key),
            )
        except Exception:
            pass
    except sqlite3.DatabaseError as e:
        print(f"[WARN] upsert failed for {url}: {e}", file=sys.stderr)
