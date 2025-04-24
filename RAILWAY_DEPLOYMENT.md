# Railway Deployment Guide

This guide provides step-by-step instructions for deploying the London Crime Data Scraper and Visualizer to Railway.

## Prerequisites

- A [Railway](https://railway.app/) account
- Git repository with your project (GitHub, GitLab, etc.)

## Deployment Steps

### 1. Create a New Project on Railway

1. Log in to your Railway account
2. Click on "New Project"
3. Select "Deploy from GitHub repo"
4. Connect your GitHub account if not already connected
5. Select the repository containing this project

### 2. Configure the Project

Railway will automatically detect the configuration from the `railway.toml` file and `Procfile`. However, you may need to set up the following:

1. **Set up the volume**:
   - Go to your project settings
   - Click on "Volumes"
   - Add a new volume named "data"
   - Set the mount path to "./data"

2. **Verify services**:
   - Railway should create two services based on the Procfile:
     - `web`: Runs the Datasette interface
     - `scraper`: Runs the monthly scraper

### 3. Deploy the Project

1. Click on "Deploy" to start the deployment process
2. Railway will build and deploy your project
3. Once deployed, you can access the Datasette interface via the provided URL

### 4. Verify the Deployment

1. Check that the Datasette interface is accessible
2. Verify that the data is being displayed correctly
3. Check the logs to ensure the scraper is running as expected

## Troubleshooting

### Database Not Found

If the database is not found, it may be because the scraper hasn't run yet. Check the logs for the scraper service to see if it's running correctly.

### Scraper Not Running

If the scraper is not running, check the logs for any errors. You may need to manually trigger the scraper by restarting the service.

### Volume Not Mounting Correctly

If the volume is not mounting correctly, check the volume configuration in the Railway dashboard. Make sure the mount path is set to "./data".

## Monitoring

You can monitor the scraper's activity through the Railway logs. The scraper will log when it's waiting for the next run date and when it's running.
