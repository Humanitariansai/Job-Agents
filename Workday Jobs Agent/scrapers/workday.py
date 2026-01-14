"""
Workday Job Scraper

Simple, ethical scraper for Workday career sites.
Teaches: HTTP requests, JSON parsing, async operations
"""

import asyncio
import aiohttp
import requests
import time
from typing import List, Dict


class WorkdayScraper:
    """
    Scrapes jobs from Workday career sites
    
    Two-step process:
    1. Get list of job IDs (fast)
    2. Fetch full details (slow, rate-limited)
    """
    
    def __init__(self, company_config: Dict):
        """Initialize with company configuration"""
        self.config = company_config
        self.base_url = company_config["base_url"]
        self.job_list_url = f"{self.base_url}/jobs"
        
        # Standard headers for all requests
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Educational Bot)"
        }
    
    def get_job_ids(self) -> List[Dict]:
        """
        Step 1: Get list of job IDs with basic info
        Returns: List of job summaries
        """
        print(f"\n🔍 Finding jobs at {self.config['name']}...")
        
        # First request: Get available categories
        response = requests.post(
            self.job_list_url,
            headers=self.headers,
            json={"limit": 1, "offset": 0},
            timeout=30
        )
        
        facets = response.json().get("facets", [])
        
        # Find our target category IDs
        category_ids = self._find_category_ids(facets)
        
        if not category_ids:
            print("❌ No matching categories found")
            return []
        
        # Fetch all jobs with pagination
        return self._fetch_all_jobs(category_ids)
    
    def _find_category_ids(self, facets: List[Dict]) -> List[str]:
        """Find IDs for our target categories"""
        target_categories = self.config["categories"]
        category_ids = []
        
        for facet in facets:
            if facet.get("facetParameter") == "jobFamilyGroup":
                for category in facet.get("values", []):
                    if category["descriptor"] in target_categories:
                        category_ids.append(category["id"])
                        print(f"  ✓ {category['descriptor']}: {category['count']} jobs")
        
        return category_ids
    
    def _fetch_all_jobs(self, category_ids: List[str]) -> List[Dict]:
        """Fetch all jobs with pagination"""
        all_jobs = []
        offset = 0
        page_size = 20
        total_jobs = None  # Add this line

        while True:
            payload = {
                "appliedFacets": {"jobFamilyGroup": category_ids},
                "limit": page_size,
                "offset": offset,
                "searchText": ""
            }

            response = requests.post(
                self.job_list_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )

            data = response.json()
            jobs = data.get("jobPostings", [])

            # Get total count from first response
            if total_jobs is None:
                total_jobs = data.get("total", 0)
                print(f"  📊 Total available: {total_jobs} jobs")

            if not jobs:
                break

            all_jobs.extend(jobs)
            print(f"  📄 Fetched {len(all_jobs)}/{total_jobs} job listings...")

            # Stop if we've fetched all jobs
            if len(all_jobs) >= total_jobs:
                break

            offset += page_size
            time.sleep(1)

        print(f"✅ Found {len(all_jobs)} total jobs\n")
        return all_jobs
    
    async def get_job_details(self, job_summaries: List[Dict]) -> List[Dict]:
        """
        Step 2: Fetch full details for each job
        Uses async for speed, rate limiting for ethics
        """
        print(f"📥 Fetching details for {len(job_summaries)} jobs...")
        
        concurrent = self.config["rate_limit"]["concurrent"]
        semaphore = asyncio.Semaphore(concurrent)
        
        # SSL workaround for some systems
        connector = aiohttp.TCPConnector(ssl=False)
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector
        ) as session:
            tasks = [
                self._fetch_one_job(session, job, semaphore, idx, len(job_summaries))
                for idx, job in enumerate(job_summaries, 1)
            ]
            
            results = await asyncio.gather(*tasks)
        
        # Filter out failed requests
        successful = [r for r in results if r is not None]
        print(f"\n✅ Successfully fetched {len(successful)}/{len(job_summaries)} jobs")
        
        return successful
    
    async def _fetch_one_job(
        self, 
        session: aiohttp.ClientSession, 
        job: Dict, 
        semaphore: asyncio.Semaphore,
        index: int,
        total: int
    ) -> Dict:
        """Fetch and parse a single job's full details"""
        async with semaphore:
            try:
                job_path = job.get("externalPath", "")
                detail_url = f"{self.base_url}{job_path}"
                
                job_title = job.get("title", "Unknown")[:50]
                print(f"  [{index}/{total}] {job_title}...")
                
                async with session.get(detail_url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_job_data(data)
                    else:
                        print(f"    ❌ Failed: HTTP {response.status}")
                        return None
            
            except Exception as error:
                print(f"    ❌ Error: {error}")
                return None
    
    def _parse_job_data(self, json_data: Dict) -> Dict:
        """Extract relevant fields from job detail JSON"""
        job_info = json_data.get("jobPostingInfo", {})
        
        return {
            "company": self.config["name"],
            "job_id": job_info.get("jobReqId"),
            "title": job_info.get("title"),
            "location": job_info.get("location"),
            "remote_type": job_info.get("remoteType"),
            "job_type": job_info.get("timeType"),
            "posted_date": job_info.get("postedOn"),
            "description": job_info.get("jobDescription"),
            "apply_url": job_info.get("externalUrl")
        }