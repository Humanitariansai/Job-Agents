# Import Playwright's timeout error so we can catch it specifically
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError


# ---------- HELPERS ----------
# This function finds an input field by its visible label,
# clears anything inside it, and types a new value.
def clear_and_fill(page, label, value, timeout=100):
    try:
        # Find the first field that matches the given label text
        field = page.get_by_label(label).first

        # Wait until the field appears (up to "timeout" milliseconds)
        field.wait_for(timeout=timeout)

        # Click the field to focus it
        field.click()

        # Clear any existing text
        field.fill("")

        # Type the new value into the field
        field.fill(value)

        # Success message in the terminal
        print(f"✅ Filled: {label}")

    # If the element does not appear in time, this error is raised
    except PlaywrightTimeoutError:
        print(f"⚠️ Skipped (not found in time): {label}")

    # Catch any other unexpected error and show it
    except Exception as e:
        print(f"⚠️ Skipped {label}: {e}")


# This function opens a dropdown (select box) and chooses an option by visible text
def select_dropdown(page, label, option_text=None, timeout=100):
    try:
        # Find the dropdown by its label
        dropdown = page.get_by_label(label).first

        # Wait until it appears
        dropdown.wait_for(timeout=timeout)

        # Click to open the dropdown menu
        dropdown.click()

        # If the user provided specific option text to select
        if option_text:
            # Allow multiple possible option names separated by "|"
            for text in option_text.split("|"):
                # Look for an option with that visible name
                option = page.get_by_role("option", name=text.strip())

                # If the option exists, click it and stop searching
                if option.count() > 0:
                    option.first.click()
                    print(f"✅ Selected: {label} -> {text.strip()}")
                    return

            # If no option matched, raise an error
            raise Exception(f"No matching option for {option_text}")

        # If no specific option was provided, just pick the first one
        else:
            page.get_by_role("option").first.click()
            print(f"✅ Selected: {label}")

    except PlaywrightTimeoutError:
        print(f"⚠️ Skipped dropdown (not found in time): {label}")

    except Exception as e:
        print(f"⚠️ Skipped dropdown {label}: {e}")


# This function handles special city/location fields that show suggestions while typing
def select_location_city(page, city, timeout=1000):
    try:
        # Find the "Location" input field
        field = page.get_by_label("Location").first

        # Wait for it to appear
        field.wait_for(timeout=timeout)

        # Click into the field
        field.click()

        # Type the city name slowly (helps suggestion lists appear)
        field.type(city, delay=100)

        # Wait until at least one suggestion option appears
        page.get_by_role("option").first.wait_for(timeout=timeout)

        # Click the first suggested option
        page.get_by_role("option").first.click()

        print("✅ Location selected")

    except PlaywrightTimeoutError:
        print("⚠️ Skipped location (not found in time)")

    except Exception as e:
        print(f"⚠️ Skipped location: {e}")


# This function clicks "Add another" buttons in the Employment section
# to create more job experience blocks
def click_add_another_employment_if_exists(page, times=1):
    try:
        employment_section = None

        # Try to find the Employment section using different possible titles
        for text in ["Employment", "Work Experience", "Professional Experience"]:
            sec = page.locator(f"text={text}")
            if sec.count() > 0:
                employment_section = sec.first
                break

        # If no employment section was found, stop here
        if not employment_section:
            print("ℹ️ No Employment section found — skipping experience expansion")
            return

        # Move up the DOM tree to get the container that holds the buttons
        container = employment_section.locator("..").locator("..")

        clicked = 0

        # Try clicking "Add another" up to "times" times
        for i in range(times):
            # Find the "Add another" button inside the employment container
            add_btn = container.get_by_role("button", name="Add another")

            # If no button exists or it is not visible, stop
            if add_btn.count() == 0 or not add_btn.first.is_visible():
                print("ℹ️ No more employment Add another buttons found — stopping expansion")
                break

            # Click the button to add a new employment block
            add_btn.first.click()

            # Small pause to allow the UI to create the new block
            page.wait_for_timeout(700)

            clicked += 1
            print(f"➕ Clicked Add another (employment) {clicked}")

        print(f"✅ Employment blocks expanded by {clicked}")

    except Exception as e:
        print(f"⚠️ Could not click employment Add another: {e}")


# This function fills one employment block (company, title, dates, etc.)
def fill_employment_block(page, index, company, title, location, start, end=None, current=False):
    try:
        # Find all fieldsets that look like employment blocks (contain "Company")
        blocks = (
            page.locator("text=Employment")
            .locator("..")
            .locator("fieldset")
            .filter(has_text="Company")
        )

        # Select the block by index (0 = first job, 1 = second job, etc.)
        block = blocks.nth(index)

        # Fill each field inside this employment block
        block.get_by_label("Company").fill(company)
        block.get_by_label("Title").fill(title)
        block.get_by_label("Location").fill(location)
        block.get_by_label("Start Date").fill(start)

        # If this is the current job, tick the "Current" checkbox
        if current:
            try:
                block.get_by_label("Current").check()
                print("☑️ Marked current role")
            except:
                # Some forms may not have a "Current" checkbox
                pass
        else:
            # Otherwise, fill the End Date if provided
            if end:
                block.get_by_label("End Date").fill(end)

        print(f"✅ Filled employment block {index+1}: {company}")

    except Exception as e:
        print(f"⚠️ Failed filling employment block {index+1}: {e}")


# This function counts how many employment blocks currently exist on the page
def get_employment_block_count(page):
    try:
        # Count all fieldsets that contain the word "Company"
        return page.locator("fieldset").filter(has_text="Company").count()
    except:
        # If something goes wrong, return 0 instead of crashing
        return 0
