# Library for making HTTP requests (used to call the Greenhouse API)
import requests

# Library for reading and writing CSV files
import csv

# Library for working with the file system (checking if files exist, paths, etc.)
import os

# Library to decode HTML entities like &nbsp;, &lt;, &amp;
import html

# Library for regular expressions (used to remove HTML tags and clean text)
import re

# Library for working with dates and times (used to filter recent jobs)
from datetime import datetime, timedelta, timezone


# ---------- CONFIG ----------
# Output CSV file where all jobs will be stored
ALL_JOBS_CSV = "Greenhouse_Jobs.csv"

# Text file that contains a list of company board names (one per line)
COMPANIES_FILE = "AI_Apply/Companies.txt"


# ---------- LOAD COMPANIES ----------
# This function reads company names from Companies.txt
# Each line should contain one Greenhouse board name (for example: discord, stripe, airbnb, etc.)
def load_companies(filename):
    companies = []

    # Open the companies file for reading
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            name = line.strip()

            # Ignore empty lines and comment lines (lines starting with "#")
            if name and not name.startswith("#"):
                companies.append(name)

    return companies


# Load the company list into memory
COMPANIES = load_companies(COMPANIES_FILE)

# Show how many companies were loaded
print(f"🏢 Loaded {len(COMPANIES)} companies:", COMPANIES)


# ---------- CLEANER ----------
# This function cleans HTML job descriptions and converts them into readable plain text
def clean_content(raw_html):
    # If the description is empty or missing, return an empty string
    if not raw_html:
        return ""

    # Decode HTML entities (&lt; &gt; &amp; &nbsp; etc.)
    text = html.unescape(raw_html)

    # Replace non-breaking spaces with normal spaces
    text = text.replace("\xa0", " ")
    text = text.replace("&nbsp;", " ")

    # Remove all HTML tags using a regular expression
    # Example: <p>Text</p> → Text
    text = re.sub(r"<[^>]+>", " ", text)

    # Normalize whitespace:
    # - remove extra spaces
    # - collapse multiple newlines / tabs into a single space
    text = re.sub(r"\s+", " ", text).strip()

    return text


# ---------- LOAD EXISTING JOBS (GLOBAL DEDUP) ----------
# This set will store "company:job_id" keys so we don’t save duplicate jobs
existing = set()

# If the CSV already exists, read it and collect existing job IDs
if os.path.exists(ALL_JOBS_CSV):
    with open(ALL_JOBS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # For each saved row, build a unique key (company + job_id)
        for row in reader:
            key = f"{row['company']}:{row['job_id']}"
            existing.add(key)

# Check whether the CSV file already existed before this run
file_exists = os.path.exists(ALL_JOBS_CSV)

# Counters for reporting statistics at the end
new_count = 0
total_found = 0


# ---------- OPEN CSV ONCE ----------
# Open the output CSV file in append mode (so new jobs are added to the end)
with open(ALL_JOBS_CSV, "a", newline="", encoding="utf-8") as f:

    # These are the column names for the CSV file
    fieldnames = [
        "job_id",
        "company",
        "title",
        "location",
        "department",
        "published_at",
        "url",
        "description"
    ]

    # Create a CSV writer that writes dictionaries into rows
    writer = csv.DictWriter(f, fieldnames=fieldnames)

    # If this is a brand-new file, write the header row first
    if not file_exists:
        writer.writeheader()


    # ---------- LOOP OVER COMPANIES ----------
    # For each company in the list, fetch its Greenhouse job board
    for company in COMPANIES:
        print(f"\n🔍 Fetching jobs from {company}...")

        # Build the Greenhouse API URL for this company
        # content=true tells the API to include full job descriptions
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true"

        try:
            # Send an HTTP GET request to the API
            resp = requests.get(url, timeout=30)

            # Raise an error if the response status is not 200 (OK)
            resp.raise_for_status()

            # Convert the JSON response into a Python dictionary
            data = resp.json()

        except Exception as e:
            # If anything goes wrong (network error, invalid JSON, etc.), skip this company
            print(f"❌ Failed to fetch {company}: {e}")
            continue


        # Only keep jobs published within the last N days
        cutoff = datetime.now(timezone.utc) - timedelta(days=2)  # look back 3 days
        recent_jobs = []

        # Loop through all jobs returned by the API
        for job in data.get("jobs", []):

            # Get the job’s publish date
            published_raw = job.get("first_published")

            # Skip jobs without a publish date
            if not published_raw:
                continue

            try:
                # Convert ISO timestamp string into a datetime object
                published = datetime.fromisoformat(
                    published_raw.replace("Z", "+00:00")
                )
            except Exception:
                # Skip malformed dates
                continue

            # Keep only jobs newer than the cutoff date
            if published >= cutoff:
                recent_jobs.append(job)

        print(f"   → Recent jobs found: {len(recent_jobs)}")
        total_found += len(recent_jobs)


        # ---------- SAVE EACH JOB ----------
        # Loop through all recent jobs and write new ones into the CSV
        for job in recent_jobs:

            # Build a unique key using company name + job ID
            key = f"{company}:{job.get('id')}"

            # Skip this job if it already exists in the CSV
            if key in existing:
                continue

            # Clean the HTML job description
            cleaned = clean_content(job.get("content"))

            # Write one row into the CSV file
            writer.writerow({
                "job_id": job.get("id"),
                "company": company,
                "title": job.get("title"),
                "location": job.get("location", {}).get("name"),
                "department": job.get("departments")[0]["name"] if job.get("departments") else "",
                "published_at": job.get("first_published"),
                "url": job.get("absolute_url"),
                "description": cleaned
            })

            # Add this job to the "existing" set so it won't be duplicated later
            existing.add(key)

            # Increase counter of newly added jobs
            new_count += 1


# ---------- DONE ----------
# Print final summary after all companies have been processed
print("\n🎯 Finished crawling Greenhouse boards.")
print(f"Total recent jobs scanned: {total_found}")
print(f"✅ New jobs added to {ALL_JOBS_CSV}: {new_count}")