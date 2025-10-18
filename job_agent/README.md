# Job Agent

**What it is:** A **rate-limited API ingester** for the **Greenhouse Job Board API**.  
A small, modular pipeline that collects job postings from common ATS providers and lets you search them locally with meaningful filters (city, level, remote). Focus: healthcare / data / ML roles in the Boston area.

## Status: 
Work in progress. Greenhouse & Lever are working; Workday is integrated but will require tweaks.

## Project structure
job_agent/
  __init__.py
  core.py          # DB, FTS, rate limiter, helpers
  greenhouse.py    # Greenhouse fetch + upsert
  lever.py         # Lever fetch + upsert
  workday.py       # Workday (cxs) fetch + upsert (experimental)
  normalize.py     # derive role_level/work_type/city/... (optional)
fetch.py           # read registry.yaml; run GH + Lever + WD ingesters
query.py           # CLI search with filters
registry.example.yaml
requirements.txt



## Quickstart
# 1) Python env
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate
# macOS/Linux:
# source .venv/bin/activate

# 2) Install deps
pip install -r requirements.txt

# 3) Configure sources
copy registry.example.yaml registry.yaml   # Windows
# cp registry.example.yaml registry.yaml  # macOS/Linux

# 4) Fetch (polite rate limit)
python fetch.py --rate 12

# 5) Query
python query.py --query "data OR analytics OR machine learning" --limit 25

