from __future__ import annotations
import json, os, sqlite3, time
from typing import Dict, Optional
from datetime import datetime, timezone
import httpx

DB_PATH = os.getenv("DB_PATH", "gh_jobs.db")
STATE_PATH = os.getenv("STATE_PATH", "state.json")
USER_AGENT = os.getenv(
    "USER_AGENT",
    "JobAgent/1.0 (+https://github.com/your/repo) Python-HTTPX"
)
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15.0"))

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

class RateLimiter:
    def __init__(self, rate_per_min: float = 30.0):
        self.min_gap = 60.0 / float(rate_per_min)
        self._next_at = 0.0
    def wait(self) -> None:
        now = time.monotonic()
        if now < self._next_at:
            time.sleep(self._next_at - now)
        self._next_at = max(self._next_at, now) + self.min_gap

def open_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute(DDL)
    try:
        conn.executescript(FTS_DDL)
        conn.commit()
    except sqlite3.OperationalError:
        pass
    return conn

def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try: return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except: return None

def http_date(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def request_with_backoff(client: httpx.Client, method: str, url: str, *, headers=None, params=None, limiter: RateLimiter=None) -> httpx.Response:
    backoff = 1.0; max_backoff = 16.0
    while True:
        if limiter: limiter.wait()
        try:
            r = client.request(method, url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        except httpx.RequestError:
            time.sleep(backoff); backoff = min(max_backoff, backoff*2); continue
        if r.status_code in (429, 500, 502, 503, 504):
            ra = r.headers.get("Retry-After")
            if ra and ra.isdigit(): time.sleep(max(float(ra), backoff))
            else: time.sleep(backoff)
            backoff = min(max_backoff, backoff*2); continue
        return r

def load_state() -> Dict[str, Dict[str, str]]:
    if not os.path.exists(STATE_PATH): return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def save_state(state: Dict[str, Dict[str, str]]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(state, f, indent=2)
    os.replace(tmp, STATE_PATH)
