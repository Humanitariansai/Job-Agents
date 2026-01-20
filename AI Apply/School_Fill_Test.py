from playwright.sync_api import sync_playwright
import os

JOB_URL = "https://job-boards.greenhouse.io/doordashusa/jobs/7537124"

RESUME_PATH = os.path.expanduser(
    "Resume/Data_Scientist_Resume_Sample.pdf"
)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=500)
    page = browser.new_page()
    page.goto(JOB_URL)

    # ✅ WAIT for the actual file input to exist
    page.wait_for_selector('input[type="file"]')

    # ✅ Attach resume directly to input
    page.set_input_files(
        'input[type="file"]',
        RESUME_PATH
    )

    # ---------- EDUCATION ----------

    # 🔴 Wait for Education section to render (Greenhouse loads this late)
    try:
        page.locator("text=Education").first.wait_for(timeout=5000)
        print("✅ Education section loaded")
    except:
        print("⚠️ Education section not found yet")

    # 🔴 Wait explicitly for School field to appear
    try:
        school_field = page.get_by_label("School").first
        school_field.wait_for(timeout=5000)
        print("✅ School field is visible")
    except:
        print("⚠️ School field not visible — skipping education")
    else:
        # Click and type slowly (important for Greenhouse autocomplete)
        school_field.click()
        school_field.fill("")
        school_field.type("Northeastern University", delay=100)

        # 🔴 Wait for dropdown options to load
        try:
            page.wait_for_timeout(1000)  # give backend time
            option = page.get_by_role("option", name="Northeastern University")
            option.wait_for(timeout=5000)
            option.first.click()
            print("✅ Selected School: Northeastern University")
        except:
            print("⚠️ School not found in dropdown — skipping selection")

    # Degree (dropdown only, must wait)
    try:
        degree_field = page.get_by_label("Degree").first
        degree_field.wait_for(timeout=5000)
        degree_field.click()

        for text in ["Master's Degree", "Master of Science"]:
            opt = page.get_by_role("option", name=text)
            if opt.count() > 0:
                opt.first.click()
                print(f"✅ Selected Degree: {text}")
                break
    except:
        print("⚠️ Degree field not available — skipping")

    # Discipline
    try:
        discipline_field = page.get_by_label("Discipline").first
        discipline_field.wait_for(timeout=5000)
        discipline_field.fill("")
        discipline_field.type("Data Science", delay=80)

        page.wait_for_timeout(800)
        option = page.get_by_role("option", name="Data Science")
        if option.count() > 0:
            option.first.click()
            print("✅ Selected Discipline: Data Science")
    except:
        print("⚠️ Discipline field not available — skipping")


    print("✅ Resume attached successfully")

    page.pause()
    browser.close()
