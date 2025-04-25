#!/bin/bash

# Wrapper script for running the London Crime Data Scraper
# This script handles Chrome installation and error handling

set -e  # Exit on error

echo "Starting London Crime Data Scraper wrapper script"

# Ensure the install_chrome.sh script is executable
chmod +x install_chrome.sh

# Try to install Chrome, but continue even if it fails
echo "Attempting to install Chrome/Chromium..."
./install_chrome.sh || {
    echo "Chrome installation failed, but continuing anyway"
}

# Install required Python packages
echo "Installing required Python packages..."
pip install -r requirements.txt || {
    echo "Warning: Failed to install some Python packages"
}

# Run the main script
echo "Running main.py..."
python main.py

# Exit with the status of the main script
exit_code=$?
echo "main.py exited with code: $exit_code"
exit $exit_code
