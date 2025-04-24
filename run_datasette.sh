#!/bin/bash

# Run Datasette with our configuration
datasette data/london_crime_data.db \
  --metadata metadata.json \
  --cors \
  --port ${PORT:-8001} \
  --host 0.0.0.0
