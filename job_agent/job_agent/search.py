# --- at the top ---
from __future__ import annotations
import sqlite3
from typing import List, Optional, Dict

def fts_available(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT 1 FROM job_fts LIMIT 1;")
        return True
    except sqlite3.OperationalError:
        return False

def search_jobs(conn: sqlite3.Connection, q: str, loc: Optional[str], limit: int = 20) -> List[Dict[str, str]]:
    if fts_available(conn) and q.strip():
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
        like = f"%{q}%"
        if loc:
            rows = conn.execute(
                """SELECT * FROM jobs
                   WHERE (title LIKE ? OR description LIKE ? OR org_name LIKE ? OR location LIKE ?)
                     AND location = ?
                   ORDER BY posted_at DESC NULLS LAST, first_seen_at DESC
                   LIMIT ?""",
                (like, like, like, like, loc, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM jobs
                   WHERE (title LIKE ? OR description LIKE ? OR org_name LIKE ? OR location LIKE ?)
                   ORDER BY posted_at DESC NULLS LAST, first_seen_at DESC
                   LIMIT ?""",
                (like, like, like, like, limit)
            ).fetchall()

    out = []
    for r in rows:
        rec = dict(r)
        out.append({
            "title": rec["title"],
            "org": rec["org_name"],
            "location": rec["location"],
            "url": rec["post_url"],
            "posted_at": rec["posted_at"],
        })
    return out
