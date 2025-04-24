# London Crime Data Scraper and Visualizer

This project scrapes crime data from the London Datastore, processes it, and provides a web interface to explore the data using Datasette.

## Features

- Automatically scrapes crime data from the London Datastore
- Processes and combines data from different sources (borough, LSOA, ward)
- Provides a web interface to explore the data using Datasette
- Configured for deployment on Railway with monthly updates

## Local Development

### Prerequisites

- Python 3.12 or higher
- pip or another package manager

### Setup

1. Clone the repository:
   ```
   git clone <repository-url>
   cd london-crime-scraper
   ```

2. Install dependencies:
   ```
   pip install -e .
   ```

3. Run the scraper to fetch the latest data:
   ```
   python main.py
   ```

4. Start the Datasette web interface:
   ```
   ./run_datasette.sh
   ```

5. Open your browser and navigate to http://localhost:8001 to explore the data.

## Deployment on Railway

This project is configured for easy deployment on Railway.

### Setup on Railway

1. Create a new project on Railway and connect your repository.

2. Railway will automatically detect the configuration from the `railway.toml` file and `Procfile`.

3. The deployment will:
   - Run the scraper on startup to fetch the latest data
   - Start the Datasette web interface
   - Schedule the scraper to run on the 30th of each month

4. The data is stored in a persistent volume, so it will be preserved between deployments.

## Project Structure

- `main.py`: The main scraper script
- `run_datasette.sh`: Script to run the Datasette web interface
- `run_monthly_scraper.py`: Script to run the scraper on a monthly schedule
- `metadata.json`: Configuration for Datasette
- `Procfile`: Configuration for Railway processes
- `railway.toml`: Configuration for Railway deployment
- `data/`: Directory containing the SQLite database

## License

[Your License Here]
