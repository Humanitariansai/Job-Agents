# Job Agent — Greenhouse Ingestion (No Scraping)

**What it is:** A **rate-limited API ingester** for the **Greenhouse Job Board API**.  
It polls public endpoints (no login), normalizes/dedupes results, and stores them in **SQLite** with **full-text search**.  
Use it to fetch multiple companies (boards) and run keyword queries locally.

## Why this is compliant
This uses Greenhouse’s **public Job Board API** (read-only, documented).  
No scraping, no logins. We identify with a User-Agent and respect rate limits.

## Quickstart
```bash
# create venv (optional)
python -m venv .venv && . .venv/Scripts/activate  # (Windows PowerShell: .\ .venv\Scripts\Activate)

pip install "httpx>=0.27.0"

# 1) fetch from one or more boards (replace tokens)
python greenhouse_fetch_and_query.py --boards pathai ginkgobioworks datavant2 foliahealth acadianassetmanagementllc clearviewhealthcarepartners putnamassociatesllc --fetch --rate 12

# 2) query locally
python greenhouse_fetch_and_query.py --query "data analyst" --limit 10
