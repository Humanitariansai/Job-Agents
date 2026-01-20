# Standard library import for working with file paths (used for resume upload)
import os

# Import Playwright’s synchronous API
from playwright.sync_api import sync_playwright

# 🔹 IMPORT HELPERS
# These are helper functions you created in another file (Helpers.py)
# They handle common tasks like filling fields, selecting dropdowns, etc.
from Helpers import (
    clear_and_fill,
    select_dropdown,
    select_location_city,
    click_add_another_employment_if_exists,
    fill_employment_block,
    get_employment_block_count
)

# ---------- CONFIG ----------
# URL of the job application form we want to automate
JOB_URL = "https://job-boards.greenhouse.io/discord/jobs/8371252002"

# Path to the resume file that will be uploaded
RESUME_PATH = "Resume/Data_Scientist_Resume_Sample.pdf"


# ---------- MAIN ----------
# Start Playwright and open a browser session
with sync_playwright() as p:

    # Launch a Chromium browser window
    # headless=False → shows the browser on screen (good for debugging)
    # slow_mo=10 → slows down actions slightly so you can see them happen
    browser = p.chromium.launch(headless=False, slow_mo=10)

    # Open a new browser tab (page)
    page = browser.new_page()

    # Navigate to the job application page
    page.goto(JOB_URL)


    # ---------- BASIC INFO ----------
    # Fill in personal information fields
    clear_and_fill(page, "First Name", "Alex")
    clear_and_fill(page, "Last Name", "Johnson")
    clear_and_fill(page, "Email", "alex.johnson@email.com")
    clear_and_fill(page, "Phone", "(555) 123-4567")
    clear_and_fill(page, "How did you hear about this job?", "LinkedIn")
    clear_and_fill(page, "Preferred First", "Alex")


    # ---------- ADDRESS ----------
    # Fill in address-related fields (some forms use different label names)
    clear_and_fill(page, "Street Address", "123 Main Street")
    clear_and_fill(page, "Address", "123 Main Street")
    clear_and_fill(page, "State", "MA")
    clear_and_fill(page, "ZIP", "02115")
    clear_and_fill(page, "Postal", "02115")
    clear_and_fill(page, "Country", "United States")


    # ---------- LOCATION ----------
    # Handle special city autocomplete fields
    select_location_city(page, "Boston, MA")

    # Some forms ask location in multiple ways — fill all matching fields
    clear_and_fill(page, "Where are you located", "Boston, MA")
    clear_and_fill(page, "Where are you currently located", "Boston, MA")


    # ---------- RESUME UPLOAD ----------
    # Wait until a file input element appears on the page
    page.wait_for_selector('input[type="file"]')

    # Upload the resume file into the file input
    page.set_input_files('input[type="file"]', RESUME_PATH)


    # ---------- LINKS ----------
    # Fill in profile and portfolio links
    clear_and_fill(page, "LinkedIn", "https://linkedin.com/in/alexjohnson")
    clear_and_fill(page, "Website", "https://portfolio.example.com")
    clear_and_fill(page, "GitHub", "https://github.com/alexjohnson")
    clear_and_fill(page, "Portfolio", "https://portfolio.example.com")
    clear_and_fill(page, "salary expectations", "$110,000")


    # ---------- WORK AUTHORIZATION ----------
    # Answer work authorization and legal questions using dropdowns
    select_dropdown(page, "Are you legally authorized to work", "Yes")
    select_dropdown(page, "Are you currently located in the US", "Yes")
    select_dropdown(page, "Will you now require immigration sponsorship", "No")
    select_dropdown(page, "Will you in the future require immigration", "No")
    select_dropdown(page, "can you provide documentation", "Yes")
    select_dropdown(page, "Are you at least 18", "Yes")


    # ---------- HYBRID / AVAILABILITY ----------
    # Answer availability and relocation questions
    select_dropdown(page, "Are you willing to relocate", "Yes")
    select_dropdown(page, "Are you able and willing to report", "Yes")
    select_dropdown(page, "hybrid", "Yes")
    clear_and_fill(page, "How soon are you able to start", "Within 2 weeks")


    # ---------- EMPLOYMENT HISTORY ----------
    # Fill total years of experience
    clear_and_fill(page, "Years of Industry Experience", "4")

    # Some forms use dropdowns for experience instead of text fields
    select_dropdown(page, "Years of Industry Experience", "4|4+|More than 3")

    # Click the "Add another" button to create multiple job history blocks
    click_add_another_employment_if_exists(page, times=3)

    # Count how many employment blocks exist now
    blocks = get_employment_block_count(page)
    print(f"ℹ️ Detected {blocks} employment blocks")

    # Fill first employment block if it exists
    if blocks >= 1:
        fill_employment_block(
            page,
            index=0,
            company="Google",
            title="Data Scientist",
            location="Boston, MA, USA",
            start="01/2023",
            current=True   # This is the current job
        )

    # Fill second employment block if it exists
    if blocks >= 2:
        fill_employment_block(
            page,
            index=1,
            company="Microsoft",
            title="Data Scientist (Education Analytics Team)",
            location="Boston, MA, USA",
            start="01/2021",
            end="12/2022"   # Past job, so we provide an end date
        )


    # ---------- EDUCATION ----------
    # Select the school from an autocomplete field
    try:
        school_field = page.get_by_label("School").first
        school_field.wait_for(timeout=5000)
        school_field.click()
        school_field.fill("")
        school_field.type("Northeastern University", delay=100)

        # Wait for suggestion list and select the correct option
        page.wait_for_timeout(1000)
        option = page.get_by_role("option", name="Northeastern University")
        option.first.click()
    except:
        # If the field does not exist on this form, silently skip it
        pass


    # Select degree from a dropdown
    try:
        degree_field = page.get_by_label("Degree").first
        degree_field.wait_for(timeout=5000)
        degree_field.click()

        # Try multiple possible text matches
        for text in ["Master's Degree", "Master of Science"]:
            opt = page.get_by_role("option", name=text)
            if opt.count() > 0:
                opt.first.click()
                break
    except:
        pass


    # Fill discipline / major field
    try:
        discipline_field = page.get_by_label("Discipline").first
        discipline_field.wait_for(timeout=5000)
        discipline_field.fill("")
        discipline_field.type("Data Science", delay=80)

        page.wait_for_timeout(800)
        option = page.get_by_role("option", name="Data Science")
        if option.count() > 0:
            option.first.click()
    except:
        pass


    # ---------- PRIOR EMPLOYMENT / REFERRALS ----------
    select_dropdown(page, "Have you worked at", "No")
    select_dropdown(page, "previously been employed", "No")
    select_dropdown(page, "Were you referred", "No")


    # ---------- GOVERNMENT / CONFLICT ----------
    # Conflict of interest and compliance questions
    select_dropdown(page, "government official", "No")
    select_dropdown(page, "close relative", "No")
    select_dropdown(page, "conflict", "No")
    select_dropdown(page, "financial interest", "No")
    select_dropdown(page, "institutional client", "No")


    # ---------- ROLE QUESTIONS ----------
    # Job-specific screening questions
    select_dropdown(page, "Do you have SQL experience", "Yes")
    select_dropdown(page, "Are you available to work this schedule", "Yes")
    select_dropdown(page, "years of experience", "Yes")


    # ---------- DEMOGRAPHICS / EEO ----------
    # Voluntary demographic information (EEO section)
    select_dropdown(page, "Gender Identity", "Male|Man")
    select_dropdown(page, "Gender", "Male|Man")
    select_dropdown(page, "Gender*", "Male|Man")
    select_dropdown(page, "Hispanic", "No")
    select_dropdown(page, "Race", "Asian")
    select_dropdown(page, "race*", "Asian")
    select_dropdown(page, "Veteran Status", "Not")
    select_dropdown(page, "Veteran Status*", "Not")
    select_dropdown(page, "Military Veteran", "Not")
    select_dropdown(page, "Disability Status", "No")
    select_dropdown(page, "Disability Status*", "No")
    select_dropdown(page, "LGBTQ", "No")
    select_dropdown(page, "Do you identify as transgender?", "No")


    # ---------- PRIVACY / ACKNOWLEDGEMENT ----------
    # Accept privacy and acknowledgement agreements
    select_dropdown(page, "Privacy", "Agree|Yes")
    select_dropdown(page, "acknowledge", "Yes")


    # ---------- COMMUNICATION PREFERENCES ----------
    # SMS and text message consent
    select_dropdown(page, "Would you like to receive communications via SMS", "No")
    select_dropdown(page, "consent to receive text messages", "No")


    # ---------- FINAL STEP ----------
    # Inform the user that essay/custom questions were skipped
    print("⏸ Essay / custom questions skipped.")
    print("👉 Review once and submit manually.")

    # Pause the script and open Playwright Inspector so the user can review the form
    page.pause()

    # Close the browser after finishing
    browser.close()
