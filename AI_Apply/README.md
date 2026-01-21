# 🤖 AI Auto Apply – Job Fetching, Matching, and Application Automation

## Overview

AI Auto Apply is an end-to-end automation system designed to streamline the job application process by combining job data ingestion, intelligent matching, and browser-based application automation.

The system simulates a real job seeker workflow — from collecting job listings, filtering relevant roles, and automatically submitting applications — enabling scalable job applications with minimal manual effort.

---

## Project Structure

Auto_Apply.py → Main automation & application engine
Jobs_Fetch.py → Job ingestion from APIs
Jobs_Matcher.py → Job filtering & ranking logic
Helpers.py → Shared browser automation utilities

---

## Architecture Overview

## Data Flow

Raw Job Listings (APIs)
        ↓
Normalized Job Dataset (CSV)
        ↓
Filtered & Ranked Jobs
        ↓
Automated Form Submissions
        ↓
Application Results & Logs


---

## Module Breakdown

### 1. Jobs_Fetch.py — Job Data Ingestion

**Purpose**  
Fetches recent job postings from external job APIs and stores them in a structured CSV format for downstream processing.

**Key Features**
- Connects to job listing APIs using HTTP requests  
- Filters jobs based on recency (e.g., last X days)  
- Normalizes raw JSON into structured fields  
- Saves clean job data into CSV files  

**Outputs**
- Job title  
- Company  
- Location  
- Posting date  
- Job URL  
- Job description  

This module acts as the **data collection layer** of the pipeline.

---

### 2. Jobs_Matcher.py — Intelligent Job Matching

**Purpose**  
Filters and ranks jobs based on candidate preferences and suitability.

**Key Features**
- Reads job postings from CSV  
- Applies keyword and skill-based filtering  
- Matches jobs against:
  - Desired roles  
  - Locations  
  - Keywords  
  - Experience level  
- Produces a shortlist of high-quality job opportunities  

**Output**
- Refined CSV containing only relevant, high-fit jobs  

This module serves as the **decision and ranking layer**.

---

### 3. Helpers.py — Automation Utilities

**Purpose**  
Provides reusable Playwright automation helpers used across the application pipeline.

**Key Features**
- Label-based and placeholder-based element selection  
- Dynamic form field detection  
- Timeout handling and retry logic  
- Safe scrolling and interaction utilities  

This module ensures:
- Stability across different job portals  
- Reduced failures from dynamic or slow-loading forms  

It acts as the **automation reliability layer**.

---

### 4. Auto_Apply.py — Automated Job Application Engine

**Purpose**  
Drives the full browser-based job application workflow.

**Key Features**
- Reads shortlisted jobs from the matcher output  
- Launches Playwright browser sessions  
- Navigates to job application pages  
- Automatically fills:
  - Personal details  
  - Work history  
  - Education  
  - Resume uploads  
  - Custom questions  
- Handles multi-step application forms  
- Tracks success and failure states  

**Capabilities**
- Multi-job batch application  
- Conditional logic for repeated employment blocks  
- Dynamic question handling  

This module is the **execution and automation layer**.

---

## Technologies Used

- Python  
- Playwright (browser automation)  
- HTTP APIs  
- CSV-based data pipelines  
- Async browser workflows  

---

## Current Capabilities

- Automated job ingestion from APIs  
- Intelligent job filtering and ranking  
- Browser-based application automation  
- Dynamic form handling  
- Scalable batch applications  

---

## Challenges & Limitations

- Anti-bot protections and CAPTCHAs on job portals  
- Inconsistent form structures across companies  
- File upload reliability (resume / attachments)  
- API rate limits and quotas  
- Session and authentication persistence  

---

## Roadmap / Next Steps

- Add LLM-based resume and cover letter customization  
- Integrate Greenhouse, Lever, and Workday APIs directly  
- Build an application tracking dashboard  
- Implement CAPTCHA-aware or human-in-the-loop fallback  
- Centralize reporting in Snowflake or Postgres  
- Add scheduling and throttling for safe large-scale execution  

---

## Summary (For Interviews / Resume Use)

Built an end-to-end automated job application system that ingests job postings from APIs, filters and ranks roles based on candidate preferences, and automatically applies using browser automation. The pipeline handles dynamic multi-step forms, resume uploads, and conditional employment blocks using Playwright, enabling scalable and intelligent job applications with minimal manual effort.

---

## License

MIT License


