import pandas as pd
import sqlite3
from datetime import datetime
import os
import logging
import re
import time
import random # Added for randomized delays
from urllib.parse import urljoin, unquote, urlparse
from dateutil.parser import parse as parse_date # For flexible date parsing if needed
from bs4 import BeautifulSoup

# Selenium and undetected-chromedriver imports
# Ensure you have undetected-chromedriver installed: pip install undetected-chromedriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# --- Configuration ---
DATASET_URL = "https://data.london.gov.uk/dataset/recorded_crime_summary"
# Use regex patterns for flexibility
FILENAME_PATTERNS = {
    "borough": re.compile(r"MPS Borough Level Crime.*\(Historical\).*\.csv", re.IGNORECASE), # Specifically target historical
    "lsoa": re.compile(r"MPS LSOA Level Crime.*\.csv", re.IGNORECASE),
    "ward": re.compile(r"MPS Ward Level Crime.*\.csv", re.IGNORECASE),
}
DB_NAME = "data/london_crime_data.db"
DOWNLOAD_DIR = os.path.abspath("data") # Use absolute path for consistency
MAX_RETRIES = 3 # Number of retries for page loads/downloads
BASE_RETRY_DELAY = 5 # Base delay in seconds for retries (will increase)

# --- Logging Setup ---
# Ensure the logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')
log_file_path = os.path.join('logs', f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path), # Log to a file
        logging.StreamHandler() # Also log to console
    ]
)
# --- Helper Functions ---

def setup_selenium_driver(download_dir=None):
    """
    Set up an undetected-chromedriver instance with robust options,
    attempting to auto-detect the Chrome version.

    Args:
        download_dir (str, optional): Directory to save downloaded files.

    Returns:
        uc.Chrome: Configured undetected-chromedriver instance or None on failure.
    """
    options = uc.ChromeOptions()
    # Headless mode can sometimes be detected, try running with a head first if issues persist
    options.add_argument("--headless") # Enable headless mode for deployment
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # Use a common, realistic user agent
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36") # Example, uc might override

    # Options to make Selenium harder to detect
    options.add_argument('--disable-blink-features=AutomationControlled')

    # Configure download behavior if download_dir is provided
    prefs = {}
    if download_dir:
        logging.info(f"Setting download directory to: {download_dir}")
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True, # Keep safe browsing enabled
            "plugins.always_open_pdf_externally": True
        }
        options.add_experimental_option("prefs", prefs)

    driver = None
    try:
        logging.info("Initializing undetected-chromedriver (attempting auto-version detection)...")
        
        # Try to find Chrome binary in common locations
        chrome_binary = None
        possible_paths = [
            '/usr/bin/google-chrome',
            '/usr/bin/google-chrome-stable',
            '/usr/bin/chromium',
            '/usr/bin/chromium-browser',
            '/snap/bin/chromium',
            '/usr/bin/chromium-browser-stable',
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',  # macOS
            '/opt/google/chrome/chrome'  # Another common Linux location
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                chrome_binary = path
                logging.info(f"Found Chrome binary at: {chrome_binary}")
                break
        
        # Only set binary_location if we actually found a valid path
        # This avoids setting it to None which can cause the "Binary Location Must be a String" error
        if chrome_binary and isinstance(chrome_binary, str):
            logging.info(f"Setting binary_location to: {chrome_binary}")
            options.binary_location = chrome_binary
        else:
            logging.warning("No Chrome binary found in common locations. Letting undetected_chromedriver auto-detect.")
        
        try:
            # Use undetected_chromedriver with explicit options
            driver = uc.Chrome(options=options)
        except Exception as e:
            if "Binary Location Must be a String" in str(e):
                logging.warning("Binary location error. Trying with minimal options...")
                # Try with minimal options as a fallback
                simple_options = uc.ChromeOptions()
                simple_options.add_argument("--headless")
                simple_options.add_argument("--no-sandbox")
                
                # Don't set binary_location at all in the fallback options
                # This lets undetected_chromedriver try to find Chrome on its own
                try:
                    driver = uc.Chrome(options=simple_options)
                    logging.info("Successfully created driver with minimal options")
                except Exception as nested_e:
                    logging.error(f"Fallback also failed: {nested_e}")
                    # Try one more approach with no options at all
                    try:
                        logging.warning("Attempting with no options at all...")
                        driver = uc.Chrome()
                        logging.info("Successfully created driver with no options")
                    except Exception as final_e:
                        logging.error(f"Final attempt failed: {final_e}")
                        raise
            else:
                # Re-raise if it's not the binary location error
                raise

        # Further attempt to hide webdriver status
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Set timeouts
        driver.set_page_load_timeout(60) # Increased timeout
        driver.implicitly_wait(10) # Implicit wait for elements

        logging.info("WebDriver initialized successfully.")
        # Log detected/used versions if possible (uc might not expose this easily)
        # cap = driver.capabilities
        # logging.info(f"Browser Version: {cap.get('browserVersion')}")
        # logging.info(f"ChromeDriver Version: {cap.get('chrome', {}).get('chromedriverVersion')}")
        return driver

    except WebDriverException as e:
        logging.error(f"Failed to initialize WebDriver: {e}")
        logging.error("Check if Chrome is installed and accessible. If the error persists, try updating Chrome or manually specifying the correct version_main in the script.")
        if driver:
            driver.quit()
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during WebDriver setup: {e}")
        if driver:
            driver.quit()
        return None

def robust_get_url(driver, url, retries=MAX_RETRIES, delay=BASE_RETRY_DELAY):
    """Attempts to load a URL with retries and exponential backoff."""
    for attempt in range(retries):
        try:
            logging.info(f"Attempt {attempt + 1}/{retries}: Navigating to {url}")
            driver.get(url)
            
            # Try to click the cookie consent button if it appears
            try:
                cookie_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".ccc-notify-button.ccc-link.ccc-tabbable.ccc-accept-button"))
                )
                logging.info("Cookie consent button found, clicking it")
                cookie_button.click()
                time.sleep(1)  # Short delay after clicking
            except (TimeoutException, NoSuchElementException):
                logging.info("No cookie consent button found or it couldn't be clicked")
            
            # Basic check for Cloudflare challenge (may need refinement)
            # Give Cloudflare checks a bit more time if detected
            time.sleep(random.uniform(1, 3)) # Short random delay after page load
            page_title = driver.title.lower()
            page_source_lower = driver.page_source.lower()

            if "checking if the site connection is secure" in page_source_lower or "cloudflare" in page_title or "just a moment..." in page_title:
                logging.warning(f"Potential Cloudflare challenge detected on attempt {attempt + 1}. Waiting longer...")
                time.sleep(delay + random.uniform(3, 7)) # Wait longer if challenged
                # Re-check title/source after waiting
                page_title = driver.title.lower()
                page_source_lower = driver.page_source.lower()
                if "checking if the site connection is secure" in page_source_lower or "cloudflare" in page_title or "just a moment..." in page_title:
                     logging.warning(f"Cloudflare challenge may still be present after waiting.")
                else:
                     logging.info("Cloudflare challenge likely passed after waiting.")
                     return True # Assume passed if keywords disappear

            else:
                logging.info(f"Successfully loaded URL: {url}")
                return True # Success
        except TimeoutException:
            logging.warning(f"Timeout loading {url} on attempt {attempt + 1}/{retries}.")
        except WebDriverException as e:
            # Handle specific common WebDriver exceptions if needed
            logging.warning(f"WebDriverException on attempt {attempt + 1}/{retries} for {url}: {e}")
            if "net::ERR_CONNECTION_REFUSED" in str(e):
                 logging.error("Connection refused. Is the webdriver process running or blocked?")
            # Add more specific handlers if certain errors recur
        except Exception as e:
            logging.error(f"Unexpected error loading {url} on attempt {attempt + 1}/{retries}: {e}", exc_info=True)

        # Exponential backoff retry logic
        if attempt < retries - 1:
            wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
            logging.info(f"Waiting {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time)
        else:
            logging.error(f"Failed to load URL {url} after {retries} attempts.")
            return False # Failure after all retries
    return False # Should not be reached, but safety return

def find_latest_files(url, patterns):
    """
    Scrapes the page using undetected-chromedriver to find the most recent download links.

    Args:
        url (str): The URL of the dataset page.
        patterns (dict): Regex patterns for filenames.

    Returns:
        dict: {key: (download_url, last_updated_date, filename)} or None on failure.
    """
    latest_files = {key: None for key in patterns.keys()}
    max_dates = {key: datetime.min for key in patterns.keys()}
    driver = None

    try:
        logging.info(f"Attempting to find files on: {url}")
        driver = setup_selenium_driver()
        if not driver:
            logging.error("WebDriver setup failed in find_latest_files.")
            return None # Driver setup failed

        # Navigate to the dataset page
        if not robust_get_url(driver, url):
            logging.error("Failed to load the dataset page after retries.")
            return None

        # Wait for a key element of the resource list to be present
        wait_time = 30 # seconds
        logging.info(f"Waiting up to {wait_time}s for resource list container...")
        resource_list_locator = (By.CSS_SELECTOR, "div.dp-dataset__resources") # Try common list tags
        try:
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located(resource_list_locator)
            )
            logging.info("Resource list container found.")
        except TimeoutException:
            logging.error(f"Resource list container ({resource_list_locator}) not found after {wait_time}s.")
            # Save page source for debugging
            try:
                debug_filename = f"debug_page_source_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logging.info(f"Saved page source to {debug_filename}")
            except Exception as e_save:
                logging.error(f"Could not save page source: {e_save}")
            return None # Cannot proceed without the list

        # Let dynamic content load
        time.sleep(random.uniform(3, 6))

        # Get page source and parse
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find resource items within the list
        # Use the locator that was successful in the wait
        resource_list = soup.select_one(resource_list_locator[1]) # Get the selector string
        if not resource_list:
             logging.error("Could not find the resource list element even after waiting.")
             return None # Cannot proceed

        resource_items = resource_list.find_all('li', class_='resource-item') # Common structure for CKAN
        if not resource_items:
             # Fallback if 'li' items aren't direct children or have different structure
             resource_items = resource_list.select('div.resource-item') # Another possible structure
             if not resource_items:
                logging.warning("No 'li.resource-item' or 'div.resource-item' found within the resource list.")
                # Maybe the structure is simpler? Look for any links inside the list
                resource_items = resource_list.find_all('a', href=True)
                if not resource_items:
                    logging.error("Could not find any resource items (li, div, or a) within the list.")
                    return None


        logging.info(f"Found {len(resource_items)} potential resource items/links.")

        for item in resource_items:
            # Adapt finding logic based on whether item is 'li', 'div', or 'a'
            link_tag = None
            title_text = None
            file_url = None

            if item.name == 'a': # If we found direct links
                link_tag = item
                title_text = item.get_text(strip=True)
            else: # If we found li or div containers
                # Try common patterns for links and titles within the container
                link_tag = item.find('a', class_='resource-url-analytics') or \
                           item.find('a', {"data-format": True}) or \
                           item.find('a', href=True) # General fallback link
                title_tag = item.find('a', class_='heading') or \
                            item.find('div', class_='title') or \
                            item.find('h3') # Common title elements
                title_text = title_tag.get_text(strip=True) if title_tag else None

            if not link_tag:
                logging.debug(f"Skipping item, no suitable download link found in: {item.prettify()[:200]}...") # Log snippet
                continue

            file_url = link_tag.get('href')
            if not file_url or file_url.startswith('#'): # Ignore placeholder links
                 logging.debug(f"Skipping item, link tag has no valid href: {file_url}")
                 continue

            # Ensure the URL is absolute
            if not file_url.startswith(('http://', 'https://')):
                base_url = urlparse(url)
                file_url = urljoin(f"{base_url.scheme}://{base_url.netloc}", file_url)

            # Extract filename from URL or title
            filename_from_url = None
            try:
                parsed_url = urlparse(file_url)
                filename_from_url = unquote(os.path.basename(parsed_url.path)).strip()
                # Basic sanity check on filename from URL
                if not re.search(r'\.[a-zA-Z0-9]+$', filename_from_url): # Check if it has an extension
                     logging.debug(f"Filename from URL '{filename_from_url}' lacks extension, might be incorrect.")
                     # filename_from_url = None # Optionally discard if no extension
            except Exception as e:
                logging.warning(f"Could not parse filename from URL {file_url}: {e}")

            filename_from_title = title_text if title_text else None

            # Prefer filename from URL if it ends with .csv
            filename = None
            if filename_from_url and filename_from_url.lower().endswith('.csv'):
                filename = filename_from_url
            elif filename_from_title and filename_from_title.lower().endswith('.csv'):
                 filename = filename_from_title
            # Fallback if no .csv found, prioritize URL then title
            elif filename_from_url and '.' in filename_from_url: # Prefer URL if it has any extension
                 filename = filename_from_url
            elif filename_from_title:
                 filename = filename_from_title
            elif filename_from_url: # Last resort URL filename
                 filename = filename_from_url
            else:
                logging.warning(f"Could not determine a suitable filename for URL: {file_url}. Link text: {title_text}")
                continue # Skip if no usable filename

            # Normalize filename slightly (replace multiple spaces)
            filename = re.sub(r'\s+', ' ', filename).strip()

            logging.debug(f"Processing file: '{filename}' from URL: {file_url}")

            # --- Date Parsing ---
            parsed_date = datetime.min # Default to minimum date
            # Try finding date within specific elements first (more reliable)
            date_tag = item.find('span', class_='last-updated-text') or \
                       item.find('span', class_='date') # Add other potential date classes/tags
            if date_tag:
                date_text = date_tag.get_text(strip=True)
                try:
                    parsed_date = parse_date(date_text)
                    logging.debug(f"Parsed date '{parsed_date}' from element text: '{date_text}'")
                except (ValueError, TypeError):
                    logging.warning(f"Could not parse date from specific element text: '{date_text}'")

            # Fallback: Look for dates in the item's general text if specific tag failed
            if parsed_date == datetime.min:
                item_text = item.get_text(" ", strip=True)
                # Regex for YYYY-MM-DD, DD/MM/YYYY, DD Month YYYY etc. (flexible)
                # This regex is broad, might need refinement
                date_match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})|(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})|(\d{1,2}\s+\w+\s+\d{4})', item_text)
                if date_match:
                    try:
                        # Use fuzzy parsing for flexibility
                        parsed_date = parse_date(date_match.group(0), fuzzy=True)
                        logging.debug(f"Parsed date '{parsed_date}' using regex fallback from item text: '{date_match.group(0)}'")
                    except (ValueError, TypeError):
                         logging.warning(f"Could not parse date from regex match: '{date_match.group(0)}'")

            # If no date found, use current time as a fallback
            if parsed_date == datetime.min:
                logging.warning(f"Could not find modification date for '{filename}'. Using current time as fallback.")
                parsed_date = datetime.now()

            current_date = parsed_date

            # Check against target patterns
            for key, pattern in patterns.items():
                # Use the determined filename for matching
                if pattern.match(filename):
                    logging.info(f"Found match for '{key}': '{filename}' (Date: {current_date.strftime('%Y-%m-%d')})")
                    # Prioritize historical borough file if found
                    # Make historical check more robust
                    is_historical = "historical" in filename.lower()

                    if "borough" in key and is_historical:
                         logging.info(f"Prioritizing historical file for '{key}': {filename}")
                         # Check if we already found one, only update if this one is newer (unlikely but possible)
                         if latest_files[key] is None or current_date > latest_files[key][1]:
                              latest_files[key] = (file_url, current_date, filename)
                              max_dates[key] = current_date # Update max date as well
                         break # Stop checking patterns for this item

                    # Otherwise, check if this file is newer than the current best for this key
                    # AND ensure we don't overwrite a prioritized historical file with a non-historical one
                    elif current_date > max_dates[key] and not ("borough" in key and latest_files[key] and "historical" in latest_files[key][2].lower()):
                        logging.info(f"Updating latest file for '{key}' to: {filename}")
                        max_dates[key] = current_date
                        latest_files[key] = (file_url, current_date, filename)
                    elif latest_files[key] is None: # If no file found yet for this key
                         logging.info(f"Setting initial file for '{key}' to: {filename}")
                         max_dates[key] = current_date
                         latest_files[key] = (file_url, current_date, filename)
                    else:
                         logging.info(f"File '{filename}' is not newer or doesn't qualify to replace existing for '{key}'.")
                    break # Move to next item once matched to a pattern


    except TimeoutException:
        logging.error("Timeout occurred during file finding process.")
        return None
    except WebDriverException as e:
         logging.error(f"WebDriverException during file finding: {e}", exc_info=True)
         return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during file finding: {e}", exc_info=True) # Log traceback
        return None
    finally:
        if driver:
            logging.info("Closing WebDriver for file finding.")
            driver.quit()

    # Log final selections
    logging.info("--- Final File Selections ---")
    found_any = False
    for key, value in latest_files.items():
        if value:
            logging.info(f"-> {key}: {value[2]} (Date: {value[1].strftime('%Y-%m-%d')}, URL: {value[0]})")
            found_any = True
        else:
            logging.warning(f"-> {key}: No file found matching pattern.")
    if not found_any:
         logging.error("No files matching any pattern were found on the page.")


    return latest_files


def download_file(url, local_path, retries=MAX_RETRIES, delay=BASE_RETRY_DELAY):
    """
    Downloads a file using undetected-chromedriver, handling potential blocks.

    Args:
        url (str): The URL of the file to download.
        local_path (str): The local path where the file should be saved.
        retries (int): Number of download attempts.
        delay (int): Base delay between retries.

    Returns:
        bool: True if download was successful, False otherwise.
    """
    driver = None
    download_dir = os.path.dirname(local_path)
    expected_filename = os.path.basename(local_path) # The name we want the file to have
    temp_download_path = None # Path where Chrome might initially download

    # Ensure download directory exists
    os.makedirs(download_dir, exist_ok=True)

    for attempt in range(retries):
        actual_downloaded_file = None # Reset for each attempt
        download_complete = False    # Reset for each attempt
        try:
            logging.info(f"Attempt {attempt + 1}/{retries}: Downloading file from {url} to {download_dir}")
            driver = setup_selenium_driver(download_dir=download_dir)
            if not driver:
                logging.error("Failed to setup driver for download.")
                # No point retrying if driver setup fails repeatedly
                if attempt < retries - 1:
                    wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
                    logging.info(f"Waiting {wait_time:.2f} seconds before retrying driver setup...")
                    time.sleep(wait_time)
                    continue # Retry the loop
                else:
                    return False # Failed after retries

            # Navigate to the URL to trigger the download
            if not robust_get_url(driver, url):
                 logging.error(f"Failed to navigate to download URL {url} on attempt {attempt + 1}")
                 # Close driver before retrying
                 if driver: driver.quit()
                 driver = None # Ensure driver is None so finally block doesn't try to quit again
                 if attempt < retries - 1:
                    wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
                    logging.info(f"Waiting {wait_time:.2f} seconds before retrying download...")
                    time.sleep(wait_time)
                    continue # Retry the loop
                 else:
                    return False # Failed after retries

            # --- Wait for download to complete ---
            download_wait_time = 180 # Increased wait time again for potentially very large files
            start_time = time.time()


            logging.info(f"Waiting up to {download_wait_time}s for download to appear and complete in {download_dir}...")

            while time.time() - start_time < download_wait_time:
                # Check for .crdownload files (indicates download in progress)
                in_progress = any(f.endswith('.crdownload') for f in os.listdir(download_dir))

                # List completed files (no .crdownload extension)
                completed_files = [f for f in os.listdir(download_dir) if not f.endswith('.crdownload') and os.path.isfile(os.path.join(download_dir, f))]

                potential_match = None
                for f in completed_files:
                    f_path = os.path.join(download_dir, f)
                    try:
                        # Check modification time relative to download start
                        # Use a generous window (e.g., last 5 mins) to catch files
                        mod_time = os.path.getmtime(f_path)
                        if (time.time() - mod_time) < 300 and mod_time > start_time - 10: # Modified recently and after we started
                             # Check if filename is somewhat related (optional, can be fragile)
                             # Or just check if it's the right type (e.g., csv)
                             if f.lower().endswith('.csv'):
                                  logging.debug(f"Potential download match found: {f} (Modified: {datetime.fromtimestamp(mod_time)})")
                                  # Check size - ensure it's not empty or tiny
                                  if os.path.getsize(f_path) > 100: # Min size check
                                      potential_match = f_path
                                      break # Found a likely candidate
                                  else:
                                      logging.warning(f"Detected file {f} is very small ({os.path.getsize(f_path)} bytes). May be incomplete or incorrect.")
                    except FileNotFoundError:
                        continue # File might have been moved/deleted
                    except Exception as e_check:
                         logging.warning(f"Error checking file {f_path}: {e_check}")

                # If we found a potential match AND no downloads are in progress
                if potential_match and not in_progress:
                     logging.info(f"Download likely complete. Found candidate: {os.path.basename(potential_match)}")
                     actual_downloaded_file = potential_match
                     download_complete = True
                     break
                elif not potential_match and not in_progress and (time.time() - start_time) > 30:
                     # If > 30s passed, nothing is downloading, and no match found, assume failure for this attempt
                     logging.warning("No download in progress and no suitable completed file found after 30s.")
                     # break # Or continue waiting the full duration? Let's continue waiting.

                # Log progress
                if in_progress:
                     logging.debug(f"Download in progress (.crdownload file exists)... Wait: {int(time.time() - start_time)}s")
                else:
                     logging.debug(f"No download in progress. Checking completed files... Wait: {int(time.time() - start_time)}s")


                time.sleep(3) # Check every 3 seconds

            # --- Post-Wait Processing ---
            if not download_complete or not actual_downloaded_file:
                logging.error(f"Download did not complete or file not found/verified within {download_wait_time} seconds on attempt {attempt + 1}.")
                # Retry logic handled by the main loop
                if attempt < retries - 1:
                     # Clean up potential partial files before retrying
                     for f in os.listdir(download_dir):
                         if f.endswith(".crdownload"):
                             try:
                                 os.remove(os.path.join(download_dir, f))
                                 logging.info(f"Removed partial download file: {f}")
                             except OSError as e_rem:
                                 logging.warning(f"Could not remove partial file {f}: {e_rem}")
                     # Wait before next attempt
                     wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
                     logging.info(f"Waiting {wait_time:.2f} seconds before retrying download...")
                     time.sleep(wait_time)
                     continue # Go to next attempt
                else:
                     logging.error(f"Download failed after {retries} attempts.")
                     return False # Failed all retries

            # --- Rename the downloaded file ---
            downloaded_filename = os.path.basename(actual_downloaded_file)
            final_path = os.path.join(download_dir, expected_filename)

            if actual_downloaded_file != final_path:
                 logging.info(f"Renaming downloaded file '{downloaded_filename}' to '{expected_filename}'")
                 try:
                     # If the target file already exists, remove it first (safer)
                     if os.path.exists(final_path):
                         logging.warning(f"Target file {final_path} already exists. Replacing.")
                         os.remove(final_path)
                     os.rename(actual_downloaded_file, final_path)
                     logging.info(f"Successfully renamed to {final_path}")
                 except OSError as e:
                     logging.error(f"Failed to rename '{actual_downloaded_file}' to '{final_path}': {e}")
                     # Consider this a failure for this attempt
                     if attempt < retries - 1:
                         wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
                         logging.info(f"Waiting {wait_time:.2f} seconds before retrying download (due to rename error)...")
                         time.sleep(wait_time)
                         continue # Retry the loop
                     else:
                         return False # Failed after all retries

            # Final verification
            if os.path.exists(final_path) and os.path.getsize(final_path) > 100: # Check size again
                logging.info(f"Successfully downloaded and verified: {final_path} (Size: {os.path.getsize(final_path)} bytes)")
                return True # Success!
            else:
                logging.error(f"Verification failed. File {final_path} not found or is too small after download and rename.")
                # Retry logic handled by the main loop if attempts remain
                if attempt < retries - 1:
                     wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
                     logging.info(f"Waiting {wait_time:.2f} seconds before retrying download (due to verification failure)...")
                     time.sleep(wait_time)
                     continue # Retry the loop
                else:
                     return False # Failed after all retries

        except TimeoutException:
            logging.error(f"Timeout occurred during download process on attempt {attempt + 1}.")
        except WebDriverException as e:
             logging.error(f"WebDriverException during download on attempt {attempt + 1}: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"An unexpected error occurred during download attempt {attempt + 1}: {e}", exc_info=True)

        # --- Cleanup and Retry Logic for Exceptions ---
        finally:
            # Always try to close the driver if it exists
            if driver:
                logging.info(f"Closing WebDriver for download attempt {attempt + 1}.")
                driver.quit()
                driver = None # Ensure it's reset

        # If an exception occurred or verification failed, and attempts remain:
        if attempt < retries - 1:
            wait_time = delay * (2 ** attempt) + random.uniform(0, delay / 2)
            logging.info(f"Waiting {wait_time:.2f} seconds before retrying download due to error/failure...")
            time.sleep(wait_time)
        else:
            logging.error(f"Download failed after {retries} attempts.")
            return False # Failed after all retries

    return False # Should not be reached, but safety return

def csv_to_sqlite(csv_path, db_name, table_name):
    """Loads data from a CSV file into a SQLite table, replacing the table if it exists."""
    if not os.path.exists(csv_path):
         logging.error(f"CSV file not found, cannot load to SQLite: {csv_path}")
         return False

    try:
        logging.info(f"Reading CSV: '{os.path.basename(csv_path)}'")
        # Try common encodings
        df = None
        try:
            df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False) # low_memory=False can help with mixed types
        except UnicodeDecodeError:
            logging.warning("UTF-8 decoding failed, trying latin-1...")
            try:
                df = pd.read_csv(csv_path, encoding='latin-1', low_memory=False)
            except Exception as e_enc:
                 logging.error(f"Failed to read CSV {csv_path} with both UTF-8 and latin-1: {e_enc}")
                 return False
        except pd.errors.ParserError as e_parse:
             logging.error(f"Pandas ParserError reading {csv_path}: {e_parse}. Check CSV format.")
             # Try with a different separator if suspecting delimiter issues (less likely for official data)
             # try:
             #     df = pd.read_csv(csv_path, encoding='latin-1', low_memory=False, sep=';') # Example: try semicolon
             # except: pass # If still fails, proceed to return False
             return False # Exit if parsing fails

        if df is None: # Should not happen if exceptions are caught, but safety check
             logging.error(f"DataFrame is None after attempting to read {csv_path}")
             return False

        logging.info(f"Read {len(df)} rows and {len(df.columns)} columns from CSV.")
        if df.empty:
             logging.warning(f"CSV file {csv_path} is empty. Skipping database load.")
             return True # Treat as success (nothing to load)

        # --- Data Cleaning ---
        # 1. Store original columns for reference
        original_columns = df.columns.tolist()
        # 2. Clean column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace(r'[^a-z0-9_]', '', regex=True)
        cleaned_columns = df.columns.tolist()
        column_mapping = dict(zip(original_columns, cleaned_columns))
        logging.info(f"Cleaned column names. Mapping: {column_mapping}")

        # 3. Remove potentially empty columns (e.g., 'unnamed:_XX')
        cols_before = df.shape[1]
        df = df.loc[:, ~df.columns.str.match('^unnamed')]
        cols_after = df.shape[1]
        if cols_before > cols_after:
             logging.info(f"Removed {cols_before - cols_after} 'unnamed' columns.")

        # 4. Optional: Trim whitespace from all string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()

        # --- Database Loading ---
        conn = None # Initialize conn
        try:
            conn = sqlite3.connect(db_name)
            logging.info(f"Loading data into SQLite table '{table_name}' (if_exists='replace')...")
            # Use chunking for potentially large files to manage memory
            chunksize = 10000 # Adjust chunk size based on memory constraints
            df.to_sql(table_name, conn, if_exists='replace', index=False, chunksize=chunksize, method='multi') # Use 'multi' method for efficiency
            conn.commit() # Explicitly commit changes
            logging.info(f"Successfully loaded data into table '{table_name}'.")
            return True
        except sqlite3.Error as e:
            logging.error(f"SQLite error writing to table {table_name}: {e}")
            if conn: conn.rollback() # Rollback partial changes on error
            return False
        finally:
            if conn:
                conn.close()
                logging.debug(f"Closed SQLite connection for {table_name}.")

    except pd.errors.EmptyDataError:
        logging.warning(f"CSV file is empty (pandas check): {csv_path}")
        return True # File exists but is empty, consider it processed.
    except FileNotFoundError:
         logging.error(f"CSV file not found at path: {csv_path}") # Should be caught earlier, but safety
         return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV to SQLite conversion for {csv_path}: {e}", exc_info=True)
        return False


def create_combined_borough_table(db_name):
    """
    Combines historical borough and ward data into a single long-format table
    aggregated by borough, crime type, and month.
    """
    logging.info("--- Creating combined borough crime table ---")
    conn = None # Initialize conn
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # --- Check for Source Tables ---
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crime_borough_historical';")
        hist_exists = cursor.fetchone()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crime_ward';")
        ward_exists = cursor.fetchone()

        if not hist_exists:
            logging.error("Source table 'crime_borough_historical' not found. Cannot create combined table.")
            return False # Essential table missing

        process_ward_data = bool(ward_exists) # Flag to indicate if ward data should be processed
        if not ward_exists:
            logging.warning("Source table 'crime_ward' not found. Combined table will only include historical data.")

        # --- Read Historical Data ---
        logging.info("Reading crime_borough_historical...")
        try:
            df_hist = pd.read_sql_query("SELECT * FROM crime_borough_historical", conn)
            logging.info(f"Read {len(df_hist)} rows from crime_borough_historical.")
            if df_hist.empty:
                 logging.warning("Historical borough data table is empty.")
                 # Decide if this is fatal or continue with potentially only ward data
                 # If historical is essential, return False here. Let's assume it's essential.
                 logging.error("Historical data is empty, cannot proceed with combination.")
                 return False
        except pd.io.sql.DatabaseError as e:
             logging.error(f"Error reading crime_borough_historical table: {e}. Check if table structure is valid.")
             return False


        # Identify key columns robustly
        major_col_hist = next((col for col in ['major_category', 'majortext'] if col in df_hist.columns), None)
        minor_col_hist = next((col for col in ['minor_category', 'minortext'] if col in df_hist.columns), None)
        borough_col_hist = next((col for col in ['borough', 'boroughname', 'lookup_boroughname'] if col in df_hist.columns), None)

        if not all([major_col_hist, minor_col_hist, borough_col_hist]):
            logging.error(f"Could not find required columns (major, minor, borough) in historical data. Found: {df_hist.columns.tolist()}")
            return False

        logging.info(f"Using columns: Major='{major_col_hist}', Minor='{minor_col_hist}', Borough='{borough_col_hist}' for historical data.")
        id_vars_hist = [major_col_hist, minor_col_hist, borough_col_hist]

        # Identify date columns (YYYYMM format)
        date_cols_hist = sorted([col for col in df_hist.columns if re.match(r'^\d{6}$', col)])
        if not date_cols_hist:
            logging.error("No date columns (YYYYMM format) found in historical data.")
            return False

        logging.info(f"Found {len(date_cols_hist)} date columns in historical data (Range: {date_cols_hist[0]} to {date_cols_hist[-1]}).")

        df_hist_long = pd.melt(df_hist,
                               id_vars=id_vars_hist,
                               value_vars=date_cols_hist,
                               var_name='month_str',
                               value_name='count')

        # Convert month_str to datetime and handle potential errors
        df_hist_long['date'] = pd.to_datetime(df_hist_long['month_str'], format='%Y%m', errors='coerce')
        rows_before_dropna = len(df_hist_long)
        df_hist_long.dropna(subset=['date'], inplace=True) # Drop rows where date conversion failed
        if len(df_hist_long) < rows_before_dropna:
             logging.warning(f"Dropped {rows_before_dropna - len(df_hist_long)} rows from historical data due to invalid date format in 'month_str'.")
        df_hist_long.drop(columns=['month_str'], inplace=True)
        # Convert count, handling non-numeric values by coercing to NaN then filling with 0
        df_hist_long['count'] = pd.to_numeric(df_hist_long['count'], errors='coerce').fillna(0).astype(int)
        # Standardize column names for merging
        df_hist_long.rename(columns={major_col_hist: 'major_category', minor_col_hist: 'minor_category', borough_col_hist: 'borough'}, inplace=True)
        logging.info(f"Transformed historical data: {len(df_hist_long)} rows.")


        # --- Read and Transform Ward Data (if exists and flagged for processing) ---
        df_ward_agg = pd.DataFrame() # Initialize empty DataFrame
        if process_ward_data:
            logging.info("Reading crime_ward...")
            try:
                df_ward = pd.read_sql_query("SELECT * FROM crime_ward", conn)
                logging.info(f"Read {len(df_ward)} rows from crime_ward.")
            except pd.io.sql.DatabaseError as e:
                 logging.error(f"Error reading crime_ward table: {e}. Skipping ward data processing.")
                 process_ward_data = False # Turn off flag if read fails

            if process_ward_data and not df_ward.empty:
                # Identify key columns in ward data
                major_col_ward = next((col for col in ['major_category', 'majortext'] if col in df_ward.columns), None)
                minor_col_ward = next((col for col in ['minor_category', 'minortext'] if col in df_ward.columns), None)
                borough_col_ward = next((col for col in ['borough', 'lookup_boroughname', 'boroughname'] if col in df_ward.columns), None)
                ward_cols = [col for col in ['wardname', 'wardcode'] if col in df_ward.columns] # Keep ward details temporarily

                if not all([major_col_ward, minor_col_ward, borough_col_ward]):
                    logging.error(f"Could not find required columns (major, minor, borough) in ward data. Found: {df_ward.columns.tolist()}. Skipping ward processing.")
                    process_ward_data = False # Turn off flag
                else:
                    logging.info(f"Using columns: Major='{major_col_ward}', Minor='{minor_col_ward}', Borough='{borough_col_ward}' for ward data.")
                    id_vars_ward = [major_col_ward, minor_col_ward, borough_col_ward] + ward_cols

                    date_cols_ward = sorted([col for col in df_ward.columns if re.match(r'^\d{6}$', col)])
                    if not date_cols_ward:
                        logging.warning("No date columns (YYYYMM format) found in ward data. Skipping ward transformation.")
                        process_ward_data = False # Turn off flag
                    else:
                        logging.info(f"Found {len(date_cols_ward)} date columns in ward data (Range: {date_cols_ward[0]} to {date_cols_ward[-1]}).")

                        df_ward_long = pd.melt(df_ward,
                                               id_vars=id_vars_ward,
                                               value_vars=date_cols_ward,
                                               var_name='month_str',
                                               value_name='count')

                        df_ward_long['date'] = pd.to_datetime(df_ward_long['month_str'], format='%Y%m', errors='coerce')
                        rows_before_dropna_ward = len(df_ward_long)
                        df_ward_long.dropna(subset=['date'], inplace=True)
                        if len(df_ward_long) < rows_before_dropna_ward:
                             logging.warning(f"Dropped {rows_before_dropna_ward - len(df_ward_long)} rows from ward data due to invalid date format in 'month_str'.")
                        df_ward_long.drop(columns=['month_str'], inplace=True)
                        df_ward_long['count'] = pd.to_numeric(df_ward_long['count'], errors='coerce').fillna(0).astype(int)

                        # Rename columns before aggregation
                        df_ward_long.rename(columns={major_col_ward: 'major_category', minor_col_ward: 'minor_category', borough_col_ward: 'borough'}, inplace=True)

                        # Aggregate ward data to borough level
                        agg_cols = ['borough', 'major_category', 'minor_category', 'date']
                        logging.info(f"Aggregating ward data by: {agg_cols}")
                        df_ward_agg = df_ward_long.groupby(agg_cols, as_index=False)['count'].sum()
                        logging.info(f"Aggregated ward data to {len(df_ward_agg)} borough-level rows.")
            elif process_ward_data and df_ward.empty:
                 logging.warning("Ward data table exists but is empty.")
                 process_ward_data = False # No ward data to process


        # --- Combine Data ---
        # Ensure columns match before concatenating
        cols_to_keep = ['borough', 'major_category', 'minor_category', 'date', 'count']
        df_hist_long = df_hist_long[cols_to_keep] # Select only needed columns

        if process_ward_data and not df_ward_agg.empty:
            df_ward_agg = df_ward_agg[cols_to_keep] # Ensure ward agg has same columns
            df_combined = pd.concat([df_hist_long, df_ward_agg], ignore_index=True)
            logging.info(f"Combined historical and aggregated ward data ({len(df_combined)} rows before final aggregation).")
        else:
            df_combined = df_hist_long # Use only historical if ward processing skipped/failed/empty
            logging.info("Proceeding with only historical data for combination.")

        if df_combined.empty:
             logging.error("Combined DataFrame is empty before final processing. Cannot proceed.")
             return False

        # --- Standardize Text Fields ---
        logging.info("Standardizing text fields (stripping, title casing)...")
        for col in ['borough', 'major_category', 'minor_category']:
             # Convert to string first to handle potential non-string data, then apply string methods
             df_combined[col] = df_combined[col].astype(str).str.strip().str.title()
             # Replace multiple spaces with single space
             df_combined[col] = df_combined[col].str.replace(r'\s+', ' ', regex=True)
             # Handle 'Nan' strings resulting from NaNs being converted
             df_combined[col] = df_combined[col].replace('Nan', 'Unknown')


        # --- Standardize Crime Categories (Refine Mapping) ---
        # Create a more comprehensive mapping based on expected/observed values
        # This mapping is crucial for accurate aggregation and analysis.
        # It likely needs ongoing refinement as data changes.
        minor_crime_mapping = {
            # Theft & Handling
            'Theft From Person': 'Theft From Person',
            'Theft From Motor Vehicle': 'Theft From Motor Vehicle',
            'Theft Or Taking Of Motor Vehicle': 'Theft Or Taking Of Motor Vehicle',
            'Other Theft': 'Other Theft',
            'Handling Stolen Goods': 'Handling Stolen Goods',
            'Shoplifting': 'Shoplifting',
            'Bicycle Theft': 'Bicycle Theft',
            'Making Off Without Payment': 'Making Off Without Payment',

            # Burglary
            'Burglary In Dwelling': 'Burglary - Residential', # Standardize to Residential/Business
            'Burglary In A Dwelling': 'Burglary - Residential',
            'Burglary - Residential': 'Burglary - Residential',
            'Burglary Other Than Dwelling': 'Burglary - Business And Community',
            'Burglary Non-Dwelling': 'Burglary - Business And Community',
            'Burglary Business And Community': 'Burglary - Business And Community',

            # Criminal Damage
            'Criminal Damage To Dwelling': 'Criminal Damage To Dwelling',
            'Criminal Damage To Motor Vehicle': 'Criminal Damage To Motor Vehicle',
            'Criminal Damage To Other Building': 'Criminal Damage To Other Building',
            'Other Criminal Damage': 'Other Criminal Damage',
            'Arson': 'Arson', # Often grouped under Criminal Damage major cat

            # Violence Against the Person
            'Common Assault': 'Common Assault',
            'Assault With Injury': 'Assault With Injury',
            'Wounding/Gbh': 'Wounding/GBH',
            'Harassment': 'Harassment',
            'Offensive Weapon': 'Possession Of Weapons', # Standardize weapon offences
            'Possession Of Weapon': 'Possession Of Weapons',
            'Possession Of Weapons': 'Possession Of Weapons',
            'Other Violence': 'Other Violence',
            'Murder': 'Homicide', # Standardize Homicide
            'Homicide': 'Homicide',
            'Violence With Injury': 'Violence With Injury', # Broader category if used
            'Violence Without Injury': 'Violence Without Injury', # Broader category if used
            'Racially Or Religiously Aggravated Offences': 'Racially/Religiously Aggravated Offences', # Simplify if possible

            # Sexual Offences
            'Rape': 'Rape',
            'Other Sexual Offences': 'Other Sexual Offences',

            # Robbery
            'Personal Robbery': 'Robbery - Personal Property', # Standardize
            'Robbery Of Personal Property': 'Robbery - Personal Property',
            'Business Robbery': 'Robbery - Business Property',
            'Robbery Of Business Property': 'Robbery - Business Property',

            # Drugs
            'Drug Trafficking': 'Drug Trafficking',
            'Possession Of Drugs': 'Drug Possession',
            'Other Drug Offences': 'Other Drug Offences',

            # Public Order
            'Public Fear Alarm Or Distress': 'Public Fear, Alarm Or Distress', # Normalize
            'Other Public Order Offences': 'Other Public Order Offences',
            'Racially/Religiously Aggravated Public Order': 'Racially/Religiously Aggravated Public Order', # Keep specific if needed
            'Racially Or Religiously Aggravated Public Fear, Al': 'Racially/Religiously Aggravated Public Order',

            # Other / Misc
            'Other Notifiable Offences': 'Other Notifiable Offences',
            'Going Equipped For Stealing': 'Going Equipped', # Simplify
            'Going Equipped': 'Going Equipped',
            'Other Offences Against The State, Or Public Order': 'Other State/Public Order Offences', # Simplify
            'Absconding From Lawful Custody': 'Absconding / Bail Offences',
            'Bail Offences': 'Absconding / Bail Offences',
            'Fraud Or Forgery': 'Fraud & Forgery', # Combine if appropriate
            'Forgery Or Use Of Drug Prescription': 'Fraud & Forgery', # Example sub-category

            # Unknown / Catch-all
            'Unknown': 'Unknown'
        }

        # Apply the mapping using the standardized 'minor_category' column
        df_combined['minor_category_std'] = df_combined['minor_category'].map(minor_crime_mapping).fillna(df_combined['minor_category'])

        # Log categories that weren't mapped (helps refine the mapping)
        unmapped_mask = df_combined['minor_category_std'] == df_combined['minor_category']
        # Exclude 'Unknown' from the list of unmapped, as it's expected
        unmapped = df_combined.loc[unmapped_mask & (df_combined['minor_category'] != 'Unknown'), 'minor_category'].unique()
        if len(unmapped) > 0:
             logging.warning(f"Minor categories not explicitly mapped (or mapped to themselves): {sorted(unmapped.tolist())}")
             logging.warning("Consider adding these to the minor_crime_mapping dictionary for standardization.")

        df_combined['minor_category'] = df_combined['minor_category_std']
        df_combined.drop(columns=['minor_category_std'], inplace=True)

        logging.info("Applied standardization mapping to minor crime categories.")

        # --- Final Aggregation ---
        # Group by all identifying columns and sum counts again to consolidate
        # historical + recent data for the same period/crime/borough, and handle standardized categories
        final_cols = ['borough', 'major_category', 'minor_category', 'date']
        logging.info(f"Performing final aggregation by: {final_cols}")
        df_final = df_combined.groupby(final_cols, as_index=False)['count'].sum()

        # Filter out rows where count is zero after aggregation
        original_rows = len(df_final)
        df_final = df_final[df_final['count'] > 0].copy()
        logging.info(f"Removed {original_rows - len(df_final)} rows with zero count after final aggregation.")

        # --- Filter out non-geographic boroughs/entities ---
        # Ensure filtering uses the standardized names
        boroughs_to_exclude = ['London Heathrow And London City Airports', 'Aviation Security (So18)', 'City Of London', 'Unknown'] # Add 'Unknown' borough if it appears
        original_rows = len(df_final)
        df_final = df_final[~df_final['borough'].isin(boroughs_to_exclude)].copy()
        rows_removed = original_rows - len(df_final)
        if rows_removed > 0:
            logging.info(f"Removed {rows_removed} rows for excluded entities: {boroughs_to_exclude}")

        # Final check for empty DataFrame
        if df_final.empty:
            logging.error("Final combined DataFrame is empty after filtering. No data to write.")
            return False

        # Optional: Sort for better readability in the DB
        df_final.sort_values(by=['borough', 'date', 'major_category', 'minor_category'], inplace=True)

        logging.info(f"Final combined table ready with {len(df_final)} rows.")

        # --- Write to SQLite ---
        target_table = "crime_borough_combined"
        logging.info(f"Writing final combined data to table '{target_table}'...")
        try:
            # Use chunking again for writing the final large table
            chunksize = 10000
            df_final.to_sql(target_table, conn, if_exists='replace', index=False, chunksize=chunksize, method='multi')
            conn.commit() # Commit final table
            logging.info(f"Successfully created/updated table '{target_table}'.")
            return True
        except sqlite3.Error as e:
            logging.error(f"SQLite error writing final combined table '{target_table}': {e}")
            conn.rollback() # Rollback on error
            return False

    except pd.errors.EmptyDataError as e:
        logging.error(f"Pandas EmptyDataError encountered during combination processing: {e}")
        return False
    except sqlite3.Error as e:
        logging.error(f"SQLite error during combination setup or reading: {e}")
        return False
    except KeyError as e:
         logging.error(f"Missing expected column during combination: {e}. Check source table structures and cleaning steps.")
         return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during table combination: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()
            logging.info("Closed SQLite connection for combined table processing.")


# --- Main Execution Logic ---
if __name__ == "__main__":
    start_time_main = time.time()
    logging.info("--- Starting Enhanced London Crime Data Scraper ---")

    # Ensure base data directory exists
    db_dir = os.path.dirname(DB_NAME)
    if db_dir and not os.path.exists(db_dir): # Check if db_dir is not empty
        os.makedirs(db_dir)
        logging.info(f"Created base directory: {db_dir}")
    # Ensure download directory exists
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logging.info(f"Created download directory: {DOWNLOAD_DIR}")

    # 1. Find the latest file URLs
    logging.info("--- Step 1: Finding latest file URLs ---")
    latest_files_info = find_latest_files(DATASET_URL, FILENAME_PATTERNS)

    if latest_files_info is None:
        logging.critical("Failed to retrieve file information. Scraper cannot proceed.")
        exit(1)

    # Filter out entries where no file was found (value is None)
    files_to_process = {k: v for k, v in latest_files_info.items() if v is not None}

    if not files_to_process:
        logging.warning("No files found matching the specified patterns after searching the page. Exiting.")
        exit(0) # Exit gracefully

    # Define table names (using cleaned/standard names)
    table_names = {
        "borough": "crime_borough_historical", # Raw historical data
        "lsoa": "crime_lsoa",
        "ward": "crime_ward",
    }

    success_count = 0
    error_count = 0
    processed_files = [] # Keep track of successfully processed file keys ('borough', 'lsoa', 'ward')

    # 2. Download and process each file
    logging.info("--- Step 2: Downloading and Loading Individual Files ---")
    for key, file_info in files_to_process.items():
        if file_info is None:
            logging.warning(f"No file info found for key '{key}', skipping.")
            continue

        file_url, file_date, filename = file_info
        logging.info(f"--- Processing '{key}' data ({filename} | Date: {file_date.strftime('%Y-%m-%d')}) ---")

        # Use a more descriptive local filename pattern including the key and date
        # Sanitize filename slightly for filesystem compatibility
        safe_filename_part = re.sub(r'[^\w\.-]', '_', filename.split('.')[0]) # Keep extension separate
        local_filename = os.path.join(DOWNLOAD_DIR, f"london_crime_{key}_{safe_filename_part}_{file_date.strftime('%Y%m%d')}.csv")

        table_name = table_names.get(key)
        if not table_name:
             logging.error(f"No table name defined in 'table_names' dictionary for key '{key}'. Skipping.")
             error_count += 1
             continue

        # 3. Download the file
        logging.info(f"Attempting download from: {file_url}")
        download_success = download_file(file_url, local_filename)

        # 4. Load CSV into SQLite (only if download succeeded)
        load_success = False
        if download_success:
            load_success = csv_to_sqlite(local_filename, DB_NAME, table_name)
        else:
            logging.error(f"Skipping database load for '{key}' because download failed.")

        # Update counts and track processed files
        if download_success and load_success:
            success_count += 1
            processed_files.append(key) # Add key to list of successes
            logging.info(f"Successfully processed and loaded '{filename}' to table '{table_name}'.")
            # Optional: Clean up downloaded file
            # Consider keeping the downloaded file for auditing/debugging, maybe move to an 'archive' folder?
            # try:
            #     os.remove(local_filename)
            #     logging.info(f"Removed downloaded file: {local_filename}")
            # except OSError as e:
            #     logging.warning(f"Could not remove file {local_filename}: {e}")
        else:
            error_count += 1
            # Log specific failure reason already done in download/load functions
            logging.error(f"Failed to complete processing for '{key}' file: {filename}")


    logging.info(f"--- Individual File Processing Summary ---")
    logging.info(f"Successfully processed: {success_count} files ({', '.join(sorted(processed_files))})")
    logging.info(f"Failed to process: {error_count} files.")

    # 6. Create the combined table only if essential source tables were processed
    logging.info("--- Step 3: Creating Combined Borough Table ---")
    # Define which keys are absolutely required for the combination step
    required_for_combination = ['borough'] # Historical is essential
    # Optional: Add 'ward' if you strictly require it:
    # required_for_combination = ['borough', 'ward']

    # Check if all required keys are in the list of successfully processed files
    can_combine = all(key in processed_files for key in required_for_combination)

    if can_combine:
        if create_combined_borough_table(DB_NAME):
            logging.info("Successfully created/updated the combined borough crime table.")
        else:
            logging.error("Failed to create the combined borough crime table.")
            error_count += 1 # Increment error count if combination fails
    elif not processed_files:
         logging.warning("Skipping table combination as no files were successfully processed.")
    else:
        missing_keys = [key for key in required_for_combination if key not in processed_files]
        logging.warning(f"Skipping table combination because required source data ({', '.join(missing_keys)}) was not successfully processed.")


    # --- Final Summary ---
    end_time_main = time.time()
    total_duration = end_time_main - start_time_main
    logging.info(f"--- Scraper Finished in {total_duration:.2f} seconds ---")
    if error_count > 0:
        logging.error(f"Completed with {error_count} errors.")
        exit(1) # Exit with error code
    else:
        logging.info("Completed successfully with 0 errors.")
        exit(0) # Exit successfully
