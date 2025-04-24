
import requests
import cloudscraper  # Add cloudscraper import
import httpx  # Add httpx for HTTP/2 support
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import os
import logging
import re
from urllib.parse import urljoin, unquote, urlparse
from dateutil.parser import parse as parse_date # For flexible date parsing if needed

# --- Configuration ---
DATASET_URL = "https://data.london.gov.uk/dataset/recorded_crime_summary"
TARGET_FILENAMES = [
    "MPS Borough Level Crime (Historical).csv",
    "MPS LSOA Level Crime.csv",
    "MPS Ward Level Crime.csv",
]
# Use a regex pattern to be slightly more flexible with potential minor name changes
# This looks for the core part of the name, allows variations like (historic) vs (Historical)
# and ensures it ends with .csv
FILENAME_PATTERNS = {
    "borough": re.compile(r"MPS Borough Level Crime.*\.csv", re.IGNORECASE), # Allow matching (Historical) or (most recent...)
    "lsoa": re.compile(r"MPS LSOA Level Crime.*\.csv", re.IGNORECASE),
    "ward": re.compile(r"MPS Ward Level Crime.*\.csv", re.IGNORECASE),
}
DB_NAME = "data/london_crime_data.db"
DOWNLOAD_DIR = "data" # Optional: Store downloads temporarily

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def find_latest_files(url, patterns):
    """
    Scrapes the page to find the most recent download links for files matching the patterns.

    Args:
        url (str): The URL of the dataset page.
        patterns (dict): A dictionary where keys are identifiers (e.g., 'borough')
                         and values are compiled regex patterns for filenames.

    Returns:
        dict: A dictionary where keys are the identifiers and values are
              tuples of (download_url, last_updated_date, filename).
              Returns None for a file type if no match is found.
    """
    latest_files = {key: None for key in patterns.keys()}
    max_dates = {key: datetime.min for key in patterns.keys()} # Store latest date found for each type

    try:
        logging.info(f"Fetching dataset page: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://data.london.gov.uk/'
        }
        
        # Try with HTTP/2 client
        logging.info("Creating HTTP/2 client...")
        client = httpx.Client(http2=True, follow_redirects=True)
        
        # First visit the main site to get cookies
        try:
            logging.info("Visiting main site to get cookies with HTTP/2...")
            client.get('https://data.london.gov.uk/', headers=headers, timeout=30)
        except Exception as e:
            logging.warning(f"Error visiting main site with HTTP/2: {e}")
            # Fall back to cloudscraper if HTTP/2 fails
            logging.info("Falling back to cloudscraper...")
            client = cloudscraper.create_scraper()
            try:
                client.get('https://data.london.gov.uk/', headers=headers, timeout=30)
            except Exception as e:
                logging.warning(f"Error visiting main site with cloudscraper: {e}")
        
        # Now try to get the dataset page
        logging.info("Now trying to access the dataset page...")
        response = client.get(url, headers=headers, timeout=30)
        
        # Log response details for debugging
        logging.info(f"Response status code: {response.status_code}")
        logging.info(f"Response headers: {response.headers}")
        
        if response.status_code == 403:
            # If we get a 403, try a different approach - direct download of known files
            logging.warning("Got 403 Forbidden, trying alternative approach...")
            return {
                "borough": ("https://data.london.gov.uk/download/recorded_crime_summary/3cdda2b7-b56f-4f21-b8f1-a8cfd7da3bf5/MPS%20Borough%20Level%20Crime%20%28Historical%29.csv", 
                           datetime.now(), 
                           "MPS Borough Level Crime (Historical).csv"),
                "lsoa": ("https://data.london.gov.uk/download/recorded_crime_summary/6ad2ca14-1b76-46f3-9750-d71eb391f256/MPS%20LSOA%20Level%20Crime%20%28most%20recent%2024%20months%29.csv",
                        datetime.now(),
                        "MPS LSOA Level Crime.csv"),
                "ward": ("https://data.london.gov.uk/download/recorded_crime_summary/2e0e8c8d-ef45-4e7a-b10a-d3faa0f1597a/MPS%20Ward%20Level%20Crime%20%28most%20recent%2024%20months%29.csv",
                        datetime.now(),
                        "MPS Ward Level Crime.csv")
            }
        
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        logging.info("Successfully fetched page.")

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find resource items (adjust selector if website structure changes)
        resource_items = soup.find_all('div', class_='dp-container')
        logging.info(f"Found {len(resource_items)} potential resource items.")

        if not resource_items:
             logging.warning("No resource items found. The page structure might have changed.")
             return latest_files # Return empty dict

        for item in resource_items:
            link_tag = item.find('a', class_='dp-resource__format')
            title_tag = item.find('div', class_='dp-resource__title') # Often filename is here

            if not link_tag or not title_tag:
                # logging.debug("Skipping item: Missing link or title tag.")
                continue

            file_url = link_tag.get('href')

            # Ensure the URL is absolute
            if file_url and not file_url.startswith(('http://', 'https://')):
                 file_url = urljoin(url, file_url) # Make URL absolute relative to the base page URL

            if not file_url:
                logging.debug("Skipping item: Missing file URL.")
                continue

            # --- Extract filename from URL ---
            try:
                parsed_url = urlparse(file_url)
                # Get the last part of the path and decode URL encoding (e.g., %20 -> space)
                filename_from_url = unquote(os.path.basename(parsed_url.path))
            except Exception as e:
                logging.warning(f"Could not parse filename from URL {file_url}: {e}")
                continue

            if not filename_from_url or not filename_from_url.lower().endswith('.csv'):
                logging.debug(f"Skipping item: Not a CSV link based on URL filename ({filename_from_url}).")
                continue
            # --- End filename extraction ---

            # For historical files, we want to prioritize them regardless of date
            is_historical = False
            if "historical" in filename_from_url.lower() or "(historical)" in filename_from_url.lower():
                is_historical = True
                logging.info(f"Found historical file: {filename_from_url}")

            # --- Revised Date Parsing ---
            parsed_end_date = None
            dates_divs = item.find_all('div', class_='dp-temporalcoverage')
            for date_div in dates_divs:
                date_text = date_div.get_text(strip=True)
                # Try parsing "From ... To dd/mm/yyyy"
                if "To" in date_text:
                    date_parts = date_text.split("To")
                    if len(date_parts) == 2:
                        try:
                            date_string = date_parts[1].strip().split()[0] # Get the 'dd/mm/yyyy' part
                            parsed_end_date = datetime.strptime(date_string, '%d/%m/%Y')
                            logging.debug(f"Parsed date '{parsed_end_date.strftime('%Y-%m-%d')}' from range: {date_text}")
                            break # Found and parsed a date, stop looking
                        except (ValueError, IndexError):
                            logging.warning(f"Could not parse date range format: '{date_text}'")
                # Add other potential date formats here if needed (e.g., 'Last updated: ...')

            # If no date was successfully parsed, use current date as fallback
            if parsed_end_date is None:
                parsed_end_date = datetime.now()
                logging.warning(f"No valid end date found for resource: {filename_from_url}. Using current date as fallback.")

            current_date = parsed_end_date
            # --- End Revised Date Parsing ---

            # Check against target patterns using filename from URL
            for key, pattern in patterns.items():
                if pattern.match(filename_from_url): # Match against filename from URL
                    logging.debug(f"Match found for '{key}': {filename_from_url} (Date: {current_date})")
                    
                    # For historical files, we always want to use them
                    if is_historical and "borough" in key:
                        logging.info(f"Selected historical file for '{key}': {filename_from_url}")
                        latest_files[key] = (file_url, current_date, filename_from_url)
                        break
                    
                    # For non-historical files, use the most recent one
                    if current_date > max_dates[key]:
                        logging.info(f"Found newer file for '{key}': {filename_from_url} (Date: {current_date})")
                        max_dates[key] = current_date
                        # Store the filename derived from the URL
                        latest_files[key] = (file_url, current_date, filename_from_url)
                    break # Move to next item once matched

    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return None # Indicate failure
    except Exception as e:
        logging.error(f"An error occurred during scraping: {e}")
        return None # Indicate failure

    # Verify all files were found
    for key, file_info in latest_files.items():
        if file_info is None:
            logging.warning(f"Could not find a suitable file matching pattern for '{key}'.")

    return latest_files

def download_file(url, local_path):
    """Downloads a file from a URL to a local path."""
    try:
        logging.info(f"Downloading file: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://data.london.gov.uk/'
        }
        
        # Try with HTTP/2 client first
        logging.info("Creating HTTP/2 client for download...")
        client = httpx.Client(http2=True, follow_redirects=True)
        
        # First visit the main site to get cookies
        try:
            logging.info("Visiting main site to get cookies with HTTP/2 before download...")
            client.get('https://data.london.gov.uk/', headers=headers, timeout=30)
        except Exception as e:
            logging.warning(f"Error visiting main site with HTTP/2: {e}")
            # Fall back to cloudscraper if HTTP/2 fails
            logging.info("Falling back to cloudscraper for download...")
            client = cloudscraper.create_scraper()
            try:
                client.get('https://data.london.gov.uk/', headers=headers, timeout=30)
            except Exception as e:
                logging.warning(f"Error visiting main site with cloudscraper: {e}")
        
        # Log the download attempt
        logging.info(f"Attempting to download file with HTTP/2 client...")
        
        # Try to download with HTTP/2 client
        with client.stream("GET", url, headers=headers, timeout=60) as r:
            # Log response details for debugging
            logging.info(f"Download response status code: {r.status_code}")
            logging.info(f"Download response headers: {r.headers}")
            
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        
        # Verify the file was downloaded and has content
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logging.info(f"Successfully downloaded to: {local_path} (Size: {os.path.getsize(local_path)} bytes)")
            return True
        else:
            logging.error(f"File download appears to have failed. File empty or not created.")
            return False
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logging.error(f"Error downloading {url}: {e}")
        return False
    except Exception as e:
        logging.error(f"Error writing file {local_path}: {e}")
        return False

def csv_to_sqlite(csv_path, db_name, table_name):
    """Loads data from a CSV file into a SQLite table, replacing the table if it exists."""
    try:
        logging.info(f"Loading CSV '{os.path.basename(csv_path)}' into SQLite table '{table_name}'...")
        df = pd.read_csv(csv_path, encoding='utf-8') # Or try 'latin-1' if utf-8 fails
        # Optional: Basic data cleaning (e.g., normalize column names)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'[^a-z0-9_]', '', regex=True)

        conn = sqlite3.connect(db_name)
        # Use 'replace' to drop the table first if it exists and create a new one
        # Use 'append' if you want to add to existing data (requires more complex logic)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logging.info(f"Successfully loaded data into table '{table_name}' in database '{db_name}'.")
        return True
    except pd.errors.EmptyDataError:
        logging.error(f"CSV file is empty: {csv_path}")
        return False
    except pd.errors.ParserError as e:
         logging.error(f"Error parsing CSV file {csv_path}: {e}")
         return False
    except sqlite3.Error as e:
        logging.error(f"SQLite error writing to table {table_name}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV to SQLite conversion: {e}")
        return False

def create_combined_borough_table(db_name):
    """
    Combines historical borough and ward data into a single long-format table
    aggregated by borough, crime type, and month.
    """
    logging.info("--- Creating combined borough crime table ---")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Check if source tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crime_borough_historical';")
        hist_exists = cursor.fetchone()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crime_ward';")
        ward_exists = cursor.fetchone()

        if not hist_exists:
            logging.warning("Source table 'crime_borough_historical' not found. Skipping combination.")
            conn.close()
            return False
        if not ward_exists:
            logging.warning("Source table 'crime_ward' not found. Skipping combination.")
            conn.close()
            return False

        # Read source tables
        df_hist = pd.read_sql_query("SELECT * FROM crime_borough_historical", conn)
        df_ward = pd.read_sql_query("SELECT * FROM crime_ward", conn)
        conn.close() # Close connection after reading

        logging.info(f"Read {len(df_hist)} rows from crime_borough_historical.")
        logging.info(f"Read {len(df_ward)} rows from crime_ward.")

        # --- Transform Historical Borough Data ---
        # Check if 'boroughname' or 'lookup_boroughname' is present
        if 'boroughname' in df_hist.columns:
            borough_col = 'boroughname'
        elif 'lookup_boroughname' in df_hist.columns:
            borough_col = 'lookup_boroughname'
        else:
            logging.error("Neither 'boroughname' nor 'lookup_boroughname' found in historical data")
            return False
            
        logging.info(f"Using '{borough_col}' as borough column in historical data")
        
        id_vars_hist = ['majortext', 'minortext', borough_col]
        # Identify date columns (assuming format YYYYMM)
        date_cols_hist = [col for col in df_hist.columns if re.match(r'^\d{6}$', col)]
        if not date_cols_hist:
             logging.warning("No date columns found in crime_borough_historical. Cannot transform.")
             return False

        logging.info(f"Found {len(date_cols_hist)} date columns in historical data, from {min(date_cols_hist)} to {max(date_cols_hist)}")

        df_hist_long = pd.melt(df_hist,
                               id_vars=id_vars_hist,
                               value_vars=date_cols_hist,
                               var_name='month_str',
                               value_name='count')
        # Convert month_str to datetime (first day of the month)
        df_hist_long['date'] = pd.to_datetime(df_hist_long['month_str'], format='%Y%m')
        df_hist_long.drop(columns=['month_str'], inplace=True)
        # Ensure count is numeric, coercing errors to NaN (which become 0 later)
        df_hist_long['count'] = pd.to_numeric(df_hist_long['count'], errors='coerce').fillna(0).astype(int)
        
        # Rename borough column to standardized name if needed
        if borough_col != 'boroughname':
            df_hist_long.rename(columns={borough_col: 'boroughname'}, inplace=True)
            
        logging.info(f"Transformed historical borough data to long format with {len(df_hist_long)} rows")


        # --- Transform Ward Data ---
        id_vars_ward = ['majortext', 'minortext', 'lookup_boroughname', 'wardname', 'wardcode']
        date_cols_ward = [col for col in df_ward.columns if re.match(r'^\d{6}$', col)]
        if not date_cols_ward:
             logging.warning("No date columns found in crime_ward. Cannot transform.")
             return False # Or potentially continue with just historical data?

        df_ward_long = pd.melt(df_ward,
                               id_vars=id_vars_ward,
                               value_vars=date_cols_ward,
                               var_name='month_str',
                               value_name='count')
        df_ward_long['date'] = pd.to_datetime(df_ward_long['month_str'], format='%Y%m')
        df_ward_long.drop(columns=['month_str'], inplace=True)
        df_ward_long['count'] = pd.to_numeric(df_ward_long['count'], errors='coerce').fillna(0).astype(int)

        # Aggregate ward data to borough level
        df_ward_agg = df_ward_long.groupby(['lookup_boroughname', 'majortext', 'minortext', 'date'], as_index=False)['count'].sum()
        df_ward_agg.rename(columns={'lookup_boroughname': 'boroughname'}, inplace=True)
        logging.info("Transformed and aggregated ward data to borough level.")

        # --- Combine Data ---
        df_combined = pd.concat([df_hist_long[df_hist_long['count'] > 0], df_ward_agg[df_ward_agg['count'] > 0]], ignore_index=True)
        logging.info(f"Combined data has {len(df_combined)} rows before final aggregation.")

        # --- Standardize Crime Types ---
        # Convert all crime types to title case for consistency
        df_combined['majortext'] = df_combined['majortext'].str.title()
        df_combined['minortext'] = df_combined['minortext'].str.title()
        
        # Create a mapping for minor crime types to standardize naming
        minor_crime_mapping = {
            # Theft category
            'Theft From The Person': 'Theft From Person',
            
            # Vehicle offences
            'Theft From A Vehicle': 'Theft From A Motor Vehicle',
            'Theft Or Unauth Taking Of A Motor Veh': 'Theft Or Taking Of A Motor Vehicle',
            
            # Burglary
            'Burglary In A Dwelling': 'Domestic Burglary',
            'Burglary - Residential': 'Domestic Burglary',
            'Burglary Non-Dwelling': 'Burglary Business And Community',
            
            # Drug offences
            'Trafficking Of Drugs': 'Drug Trafficking',
            
            # Public order offences
            'Race Or Religious Agg Public Fear': 'Racially Or Religiously Aggravated Public Fear, Al',
            'Other Offences Public Order': 'Other Offences Against The State, Or Public Order'
        }
        
        # Apply the mapping to standardize minor crime types
        df_combined['minortext'] = df_combined['minortext'].replace(minor_crime_mapping)
        
        logging.info("Standardized crime type names and normalized minor crime type variations")

        # --- Final Aggregation ---
        # Group by all identifying columns and sum counts to handle potential overlaps
        final_cols = ['boroughname', 'majortext', 'minortext', 'date']
        df_final = df_combined.groupby(final_cols, as_index=False)['count'].sum()
        # Filter out rows where count is zero after aggregation
        df_final = df_final[df_final['count'] > 0].copy()
        # Optional: Sort for better readability in the DB
        df_final.sort_values(by=['boroughname', 'majortext', 'minortext', 'date'], inplace=True)

        # --- Filter out specific boroughs ---
        boroughs_to_exclude = ['London Heathrow and London City Airports', 'Aviation Security (SO18)']
        original_rows = len(df_final)
        df_final = df_final[~df_final['boroughname'].isin(boroughs_to_exclude)].copy()
        rows_removed = original_rows - len(df_final)
        if rows_removed > 0:
            logging.info(f"Removed {rows_removed} rows for excluded boroughs: {boroughs_to_exclude}")

        logging.info(f"Final combined table has {len(df_final)} rows after exclusion.")

        # --- Write to SQLite ---
        conn = sqlite3.connect(db_name)
        target_table = "crime_borough_combined"
        df_final.to_sql(target_table, conn, if_exists='replace', index=False)
        conn.close()
        logging.info(f"Successfully created/updated table '{target_table}' in '{db_name}'.")
        return True

    except pd.errors.EmptyDataError as e:
        logging.error(f"Empty data encountered during combination: {e}")
        if 'conn' in locals() and conn: conn.close()
        return False
    except sqlite3.Error as e:
        logging.error(f"SQLite error during combination: {e}")
        if 'conn' in locals() and conn: conn.close()
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during table combination: {e}")
        if 'conn' in locals() and conn: conn.close()
        return False


# --- Main Execution Logic ---
if __name__ == "__main__":
    logging.info("--- Starting London Crime Data Scraper ---")

    # Create download directory if it doesn't exist
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logging.info(f"Created download directory: {DOWNLOAD_DIR}")

    # 1. Find the latest file URLs
    latest_files_info = find_latest_files(DATASET_URL, FILENAME_PATTERNS)

    if latest_files_info is None:
         logging.error("Failed to retrieve file information. Exiting.")
         exit(1)

    files_to_process = {k: v for k, v in latest_files_info.items() if v is not None}

    if not files_to_process:
         logging.warning("No files found matching the criteria. Exiting.")
         exit(0) # Exit gracefully if no files were found

    # Define table names based on keys
    table_names = {
        "borough": "crime_borough_historical",
        "lsoa": "crime_lsoa",
        "ward": "crime_ward",
    }

    success_count = 0
    error_count = 0

    # 2. Download and process each file
    for key, (file_url, file_date, filename) in files_to_process.items():
        logging.info(f"--- Processing {key} data ({filename} - {file_date.strftime('%Y-%m-%d')}) ---")
        local_filename = os.path.join(DOWNLOAD_DIR, f"{key}_data_latest.csv")
        table_name = table_names.get(key, f"crime_{key}") # Default table name if key not in map

        # 3. Download the file
        if download_file(file_url, local_filename):
            # 4. Load CSV into SQLite
            if csv_to_sqlite(local_filename, DB_NAME, table_name):
                success_count += 1
                # 5. Optional: Clean up downloaded file
                try:
                    os.remove(local_filename)
                    logging.info(f"Removed temporary file: {local_filename}")
                except OSError as e:
                    logging.warning(f"Could not remove temporary file {local_filename}: {e}")
            else:
                error_count += 1
                logging.error(f"Failed to process {filename} into the database.")
        else:
            error_count += 1
            logging.error(f"Failed to download {filename}.")

    logging.info("--- Scraper Finished ---")
    logging.info(f"Successfully processed: {success_count} files.")
    logging.info(f"Failed to process: {error_count} files.")

    # 6. Create the combined table if all downloads/loads were successful
    if error_count == 0 and success_count > 0: # Only run if initial steps worked
        if create_combined_borough_table(DB_NAME):
            logging.info("Successfully created the combined borough crime table.")
        else:
            logging.error("Failed to create the combined borough crime table.")
            error_count += 1 # Increment error count if combination fails
    elif success_count == 0:
         logging.warning("No files were successfully processed, skipping table combination.")
    else:
        logging.warning("Skipping table combination due to errors in previous steps.")


    if error_count > 0:
        logging.error("--- Scraper finished with errors ---")
        exit(1) # Exit with error code if any step failed
    else:
        logging.info("--- Scraper finished successfully ---")
        exit(0) # Exit successfully
