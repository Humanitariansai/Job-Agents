"""
Data Storage Utilities

Handles saving and loading job data in JSON and CSV formats.
"""

import json
import pandas as pd
from pathlib import Path
from typing import List, Dict
from datetime import datetime


DATA_DIR = Path("data/jobs")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_jobs(jobs: List[Dict], company_name: str) -> tuple:
    """
    Save jobs to both JSON and CSV formats
    
    Args:
        jobs: List of job dictionaries
        company_name: Name for the output files
    
    Returns:
        (json_path, csv_path): Paths to saved files
    """
    if not jobs:
        print("⚠️  No jobs to save")
        return None, None
    
    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"{company_name}_{timestamp}"
    
    # Save as JSON
    json_path = DATA_DIR / f"{filename}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    print(f"💾 JSON saved: {json_path}")
    
    # Save as CSV
    df = pd.DataFrame(jobs)
    csv_path = DATA_DIR / f"{filename}.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"📊 CSV saved: {csv_path}")
    
    return json_path, csv_path

def merge_jobs(old_jobs: List[Dict], new_jobs: List[Dict]) -> List[Dict]:
    """
    Merge new jobs with existing, removing duplicates by job_id
    
    Args:
        old_jobs: Previously saved jobs
        new_jobs: Newly scraped jobs
    
    Returns:
        Merged list without duplicates (new jobs overwrite old)
    """
    merged = {}
    
    # Add old jobs first
    for job in old_jobs:
        job_id = job.get("job_id")
        if job_id:
            merged[job_id] = job
    
    # Add/update with new jobs
    for job in new_jobs:
        job_id = job.get("job_id")
        if job_id:
            merged[job_id] = job
    
    print(f"📊 Merged: {len(old_jobs)} old + {len(new_jobs)} new = {len(merged)} unique jobs")
    return list(merged.values())

def get_existing_job_ids(company_name: str) -> set:
    """Get set of job IDs we already have"""
    jobs = load_jobs(company_name)
    return {job.get("job_id") for job in jobs if job.get("job_id")}

def load_jobs(company_name: str) -> List[Dict]:
    """
    Load most recent jobs for a company
    
    Args:
        company_name: Name of company
    
    Returns:
        List of job dictionaries
    """
    pattern = f"{company_name}_*.json"
    files = sorted(DATA_DIR.glob(pattern))
    
    if not files:
        print(f"📭 No saved jobs found for {company_name}")
        return []
    
    latest_file = files[-1]
    with open(latest_file, 'r', encoding='utf-8') as f:
        jobs = json.load(f)
    
    print(f"📂 Loaded {len(jobs)} jobs from {latest_file.name}")
    return jobs