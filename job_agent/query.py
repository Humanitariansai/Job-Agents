from __future__ import annotations
import argparse
from job_agent.core import open_db
from job_agent.search import search_jobs   # <-- updated

def main():
    ap = argparse.ArgumentParser(description="Query local jobs DB.")
    ap.add_argument("--query", type=str, required=True, help="Search text")
    ap.add_argument("--loc", type=str, default=None, help="Exact location filter")
    ap.add_argument("--limit", type=int, default=20, help="Max results")
    args = ap.parse_args()

    conn = open_db()
    results = search_jobs(conn, args.query, args.loc, args.limit)  # <-- updated
    conn.close()

    if not results:
        print("(no results)"); return
    for i, r in enumerate(results, 1):
        print(f"{i:>2}. {r['title']} — {r['org']}")
        if r.get("location"): print(f"    📍 {r['location']}")
        if r.get("posted_at"): print(f"    🗓  {r['posted_at']}")
        print(f"    🔗 {r['url']}\n")

if __name__ == "__main__":
    main()
