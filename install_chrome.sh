#!/bin/bash

# Script to install Chrome/Chromium in Railway environment
# This script will be called from main.py if Chrome is not found

echo "Checking for Chrome/Chromium installation..."

# Check if Chrome or Chromium is already installed
if command -v google-chrome &> /dev/null || command -v chromium-browser &> /dev/null; then
    echo "Chrome/Chromium is already installed."
    exit 0
fi

echo "Chrome/Chromium not found. Installing Chromium..."

# Update package lists
apt-get update

# Install dependencies
apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    apt-transport-https

# Install Chromium (more lightweight than Chrome)
apt-get install -y --no-install-recommends chromium-browser

# Verify installation
if command -v chromium-browser &> /dev/null; then
    echo "Chromium installed successfully at: $(which chromium-browser)"
    exit 0
else
    echo "Failed to install Chromium. Trying Google Chrome..."
    
    # Add Google Chrome repository
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
    
    # Update package lists again
    apt-get update
    
    # Install Google Chrome
    apt-get install -y --no-install-recommends google-chrome-stable
    
    # Verify installation
    if command -v google-chrome &> /dev/null; then
        echo "Google Chrome installed successfully at: $(which google-chrome)"
        exit 0
    else
        echo "Failed to install Chrome/Chromium."
        exit 1
    fi
fi
