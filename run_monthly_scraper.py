#!/usr/bin/env python3
"""
Monthly scraper script for Railway deployment.
This script will run the main.py scraper on the 30th of each month.
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def calculate_next_run_time():
    """Calculate the next time to run (30th of current or next month)."""
    now = datetime.now()
    
    # If today is the 30th, run immediately
    if now.day == 30:
        logging.info("Today is the 30th - running scraper immediately")
        return now
    
    # Calculate the next 30th
    if now.day < 30:
        # Later this month
        next_run = now.replace(day=30)
    else:
        # Next month
        if now.month == 12:
            next_run = now.replace(year=now.year + 1, month=1, day=30)
        else:
            next_run = now.replace(month=now.month + 1, day=30)
    
    return next_run

def run_scraper():
    """Run the main.py scraper script."""
    logging.info("Starting scraper...")
    try:
        result = subprocess.run(["python", "main.py"], check=True)
        if result.returncode == 0:
            logging.info("Scraper completed successfully")
        else:
            logging.error(f"Scraper failed with return code {result.returncode}")
    except subprocess.SubprocessError as e:
        logging.error(f"Error running scraper: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    """Main function to run the scraper on a schedule."""
    logging.info("Monthly scraper service started")
    
    # Run immediately on startup if RAILWAY_ENVIRONMENT is set
    # This ensures we have data on first deployment
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        logging.info("Initial run on deployment")
        run_scraper()
    
    while True:
        next_run = calculate_next_run_time()
        now = datetime.now()
        
        # Calculate seconds until next run
        wait_seconds = (next_run - now).total_seconds()
        
        if wait_seconds <= 0:
            # Run now
            logging.info("Running scraper now")
            run_scraper()
            # Sleep for a day to avoid re-running on the same day
            time.sleep(86400)
        else:
            # Wait until the next run time
            wait_hours = wait_seconds / 3600
            logging.info(f"Next run scheduled for {next_run.strftime('%Y-%m-%d')} "
                         f"({wait_hours:.1f} hours from now)")
            
            # Sleep for a while, then check again
            # This allows the script to respond to system signals more quickly
            time.sleep(min(wait_seconds, 3600))  # Sleep for at most 1 hour at a time

if __name__ == "__main__":
    main()
