# Library for reading and writing CSV files
import csv

# Library for working with file paths and checking if files exist
import os

# OpenAI Python client (used to call GPT models)
from openai import OpenAI

# Library for reading PDF files
from pypdf import PdfReader

# Library for loading environment variables from a .env file
from dotenv import load_dotenv


# Load variables from .env into environment (ex: OPENAI_API_KEY)
load_dotenv()


# ---------- CONFIG ----------
# Input CSV containing raw jobs from the Greenhouse crawler
INPUT_CSV = "Greenhouse_Jobs.csv"

# Output CSV that will store AI-filtered and scored jobs
OUTPUT_CSV = "Personal_Jobs.csv"

# Path to the resume PDF that will be compared against job descriptions
RESUME_PDF = "Resume/Data_Scientist_Resume_Sample.pdf"


# Create an OpenAI client using the API key from environment variables
client = OpenAI()


# ---------- READ RESUME FROM PDF ----------
# This function reads all text from a resume PDF and returns it as a single string
def load_resume_text(pdf_path):
    # Open the PDF file
    reader = PdfReader(pdf_path)

    text = ""

    # Loop through every page in the PDF
    for page in reader.pages:

        # Extract text from the current page
        page_text = page.extract_text()

        # Only add non-empty pages
        if page_text:
            text += page_text + "\n"

    # Remove leading/trailing whitespace and return full resume text
    return text.strip()


# Load resume once at startup
print("📄 Reading resume PDF...")
RESUME_TEXT = load_resume_text(RESUME_PDF)
print("✅ Resume loaded.")


# ---------- LOAD EXISTING PERSONAL JOBS (DEDUP) ----------
# This set keeps track of jobs already processed so we don’t analyze them again
existing = set()

# If the output CSV already exists, load previously analyzed jobs
if os.path.exists(OUTPUT_CSV):
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Build a unique key (company + job_id) for each existing row
        for row in reader:
            key = f"{row['company']}:{row['job_id']}"
            existing.add(key)

# Check whether the output file already existed before this run
file_exists = os.path.exists(OUTPUT_CSV)

# Counter for how many new jobs are analyzed in this run
new_count = 0


# ---------- AI ANALYSIS FUNCTION (MATCH + VISA IN ONE CALL) ----------
# This function sends the resume + job description to GPT
# and asks it to return:
#   - Fit score (0–100)
#   - Visa sponsorship (Yes / No / Maybe)
#   - One-line verdict explanation
def analyze_job(job_title, job_description):

    # Build a long instruction prompt for the AI recruiter
    prompt = f"""
You are an experienced technical recruiter screening jobs for an international data / AI professional.

Evaluate how suitable this job is for the candidate by comparing the resume holistically to the job description, and report visa sponsorship separately.

Location rule (VERY IMPORTANT):
- First determine whether this role is based in the United States.
- If the job is NOT US-based (outside the US or explicitly non-US), set Fit score = 0.
- If the location is unclear or global/remote without country, assume it is US-eligible and continue scoring normally.

Fit scoring rules:
- Target junior, associate, or mid-level individual contributor roles only.
- Strongly penalize manager, lead, architect, staff, principal, or people-management roles.
- Moderately penalize “senior” roles unless scope clearly matches mid-level (1–5 years, hands-on IC).
- Score fit on overall alignment with the JD: role scope, responsibilities, required skills, and experience.
- Ignore “preferred / nice-to-have” sections.
- Do not hardcode or prioritize specific skills.
- Treat projects, volunteer roles, and automation systems as valid experience.

Visa rules (do not affect fit score):
- Visa: Yes / No / Maybe based only on the JD.
- If not mentioned, mark “Maybe”.

Resume:
{RESUME_TEXT}

Job Title:
{job_title}

Job Description:
{job_description}

Output (strict):
- Fit score (0–100)
- Visa sponsorship: Yes / No / Maybe
- One-line verdict: one line explaining why this role is or isn’t a good fit
"""

    # Send the prompt to OpenAI Chat Completions API
    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2   # Low temperature = more stable, less random answers
    )

    # Extract the raw text response from the model
    text = resp.choices[0].message.content.strip()

    # Default fallback values
    score = "0"
    visa = "maybe"
    verdict = ""

    # Parse the model’s output line by line
    for line in text.splitlines():
        lower = line.lower()

        # Look for the "Fit score" line
        if "fit score" in lower:
            score = line.split(":", 1)[1].strip()

        # Look for the visa sponsorship line
        elif "visa" in lower:
            visa = line.split(":", 1)[1].strip().lower()

        # Look for the verdict line
        elif "verdict" in lower or "one-line" in lower:
            verdict = line.split(":", 1)[1].strip()

    # ---------- SAFETY CLEANUP ----------
    # Make sure the score is numeric
    if not score.isdigit():
        score = "0"

    # Make sure visa value is one of the allowed options
    if visa not in ["yes", "no", "maybe"]:
        visa = "maybe"

    # Return structured results
    return score, visa, verdict


# ---------- PROCESS JOBS ----------
# Open the input job list and the output personal job list at the same time
with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
     open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as outfile:

    # Reader for the input jobs
    reader = csv.DictReader(infile)

    # Define output CSV columns
    fieldnames = [
        "job_id",
        "company",
        "title",
        "location",
        "url",
        "fit_score",
        "visa_sponsor",
        "verdict",
        "status"
    ]

    # Writer for the output file
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)

    # Write header only if this is a brand-new file
    if not file_exists:
        writer.writeheader()


    # Loop through every job from the input CSV
    for job in reader:

        # Build unique key to detect duplicates
        key = f"{job['company']}:{job['job_id']}"

        # Skip jobs that were already analyzed before
        if key in existing:
            continue   # already processed before


        # Extract job title and description
        title = job["title"]
        description = job.get("description", "")

        print(f"\n🧠 Analyzing job: {job['company']} | {title}")

        try:
            # Call the AI analysis function
            score, visa, verdict = analyze_job(title, description)

        except Exception as e:
            # If OpenAI fails (rate limit, timeout, etc.), skip this job
            print("❌ AI failed:", e)
            continue


        # Write analyzed job into the output CSV
        writer.writerow({
            "job_id": job["job_id"],
            "company": job["company"],
            "title": title,
            "location": job["location"],
            "url": job["url"],
            "fit_score": score,
            "visa_sponsor": visa,
            "verdict": verdict,
            "status": "new"   # mark newly added jobs
        })

        # Increase counter and show result
        new_count += 1
        print(f"   ✅ Fit: {score} | Visa: {visa} | {verdict}")


# ---------- DONE ----------
# Final summary
print("\n🎯 Done.")
print(f"✅ New jobs analyzed and added to {OUTPUT_CSV}: {new_count}")