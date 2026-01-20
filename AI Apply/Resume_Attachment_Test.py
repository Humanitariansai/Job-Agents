from playwright.sync_api import sync_playwright
import os

JOB_URL = "https://job-boards.greenhouse.io/discord/jobs/8371252002"

RESUME_PATH = "Resume/Data_Scientist_Resume_Sample.pdf"

print("Resume exists:", os.path.exists(RESUME_PATH))


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=300)
    page = browser.new_page()
    page.goto(JOB_URL)

    # ✅ WAIT for the actual file input to exist
    page.wait_for_selector('input[type="file"]')

    # ✅ Attach resume directly to input
    page.set_input_files(
        'input[type="file"]',
        RESUME_PATH
    )

    print("✅ Resume attached successfully")

    page.pause()
    browser.close() 
