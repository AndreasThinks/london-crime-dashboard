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

# Check if we have sudo access
if command -v sudo &> /dev/null; then
    SUDO="sudo"
else
    # No sudo, try to run as current user (might work if we're root)
    SUDO=""
fi

# Update package lists
$SUDO apt-get update

# Install dependencies
$SUDO apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    apt-transport-https

# Install Chromium directly (avoid snap)
$SUDO apt-get install -y --no-install-recommends chromium-browser --no-install-suggests

# If that fails (might be a snap package), try installing the debian package directly
if ! command -v chromium-browser &> /dev/null; then
    echo "Standard installation failed. Trying to install chromium-browser without snap..."
    # Disable snap for this installation
    $SUDO apt-get install -y --no-install-recommends chromium-browser --no-install-suggests
    
    # If still not installed, try the chromium package (some distros use this name)
    if ! command -v chromium-browser &> /dev/null; then
        echo "Trying chromium package instead..."
        $SUDO apt-get install -y --no-install-recommends chromium --no-install-suggests
        
        # If still not installed, try downloading Chrome directly
        if ! command -v chromium-browser &> /dev/null && ! command -v chromium &> /dev/null; then
            echo "Package installation failed. Trying to download Chrome directly..."
            
            # Create a temporary directory for the download
            TEMP_DIR=$(mktemp -d)
            cd $TEMP_DIR
            
            # Download Chrome
            wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
            
            # Install the package
            $SUDO apt-get install -y ./google-chrome-stable_current_amd64.deb
            
            # Clean up
            cd -
            rm -rf $TEMP_DIR
            
            # Check if Google Chrome was installed
            if command -v google-chrome &> /dev/null || command -v google-chrome-stable &> /dev/null; then
                echo "Google Chrome installed successfully via direct download."
                exit 0
            fi
        fi
    fi
fi

# Verify installation
if command -v chromium-browser &> /dev/null; then
    echo "Chromium installed successfully at: $(which chromium-browser)"
    exit 0
else
    echo "Failed to install Chromium. Trying Google Chrome..."
    
    # Add Google Chrome repository
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | $SUDO apt-key add -
    $SUDO bash -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list'
    
    # Update package lists again
    $SUDO apt-get update
    
    # Install Google Chrome
    $SUDO apt-get install -y --no-install-recommends google-chrome-stable
    
    # Verify installation
    if command -v google-chrome &> /dev/null; then
        echo "Google Chrome installed successfully at: $(which google-chrome)"
        exit 0
    else
        echo "Failed to install Chrome/Chromium."
        exit 1
    fi
fi
