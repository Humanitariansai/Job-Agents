"""
Workday Jobs Agent - Scraper

Usage:
    python examples/scrape_jobs.py --company cvs --limit 10
    python examples/scrape_jobs.py --company centene
    python examples/scrape_jobs.py --company all
"""

import asyncio
import argparse
import sys
from pathlib import Path
from utils.storage import save_jobs, load_jobs, get_existing_job_ids

# Add parent directory to path FIRST
sys.path.append(str(Path(__file__).parent.parent))

from config.companies import get_company, list_companies, COMPANY_NAMES
from scrapers.workday import WorkdayScraper
from utils.storage import save_jobs, load_jobs

# Try to import merge_jobs (optional feature)
try:
    from utils.storage import merge_jobs
except ImportError:
    merge_jobs = None


async def scrape_company(company_name: str, limit: int = None):
    """Scrape jobs from one company"""
    print(f"\n{'='*60}")
    print(f"🏢 Scraping {company_name.upper()}")
    print(f"{'='*60}")
    
    config = get_company(company_name)
    scraper = WorkdayScraper(config)
    
    job_summaries = scraper.get_job_ids()
    
    if not job_summaries:
        print("❌ No jobs found")
        return []
    
    # NEW: Filter out jobs we already have
    existing_ids = get_existing_job_ids(company_name)
    if existing_ids:
        new_summaries = [j for j in job_summaries if scraper._extract_job_id(j) not in existing_ids]
        print(f"⏭️  Skipping {len(job_summaries) - len(new_summaries)} existing jobs")
        job_summaries = new_summaries
    
    if limit:
        job_summaries = job_summaries[:limit]
        print(f"🔢 Limited to {limit} jobs for testing\n")
    
    jobs = await scraper.get_job_details(job_summaries)
    
    if jobs:
        # Only merge if merge_jobs function is available
        if merge_jobs:
            old_jobs = load_jobs(company_name)
            if old_jobs:
                jobs = merge_jobs(old_jobs, jobs)
        
        save_jobs(jobs, company_name)
    
    return jobs


async def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="Workday Jobs Agent - Scrape jobs from Workday career sites"
    )
    
    parser.add_argument(
        "--company",
        choices=COMPANY_NAMES + ["all"],
        help="Company to scrape (or 'all')"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of jobs (for testing)"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available Workday companies"
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_companies()
        return
    
    if not args.company:
        parser.print_help()
        return
    
    if args.company == "all":
        companies_to_scrape = COMPANY_NAMES
    else:
        companies_to_scrape = [args.company]
    
    all_jobs = []
    for company in companies_to_scrape:
        jobs = await scrape_company(company, args.limit)
        all_jobs.extend(jobs)
    
    print(f"\n{'='*60}")
    print(f"✅ WORKDAY JOBS AGENT - COMPLETE!")
    print(f"{'='*60}")
    print(f"Total jobs scraped: {len(all_jobs)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())