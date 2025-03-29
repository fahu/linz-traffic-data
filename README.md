# Linz Traffic Data Scraper

This script fetches traffic data from the Linz WebGIS portal's API and saves it as JSON files, organized by date.

## Features

- Direct access to the Linz traffic data API endpoints
- Handles multiple traffic datasets (7-day and 30-day)
- Saves data with dataset ID and date in filename
- Organizes raw data into day-by-day files
- Comprehensive logging for debugging
- **Automatic authentication** using the OAuth2 endpoint
- Interactive token entry as fallback for authentication

## Setup

1. Clone this repository
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

To run the script with automatic authentication:

```bash
python linz_traffic_scraper.py
```

To run the script with an interactive prompt for a token (if automatic authentication fails):

```bash
python linz_traffic_scraper.py --prompt-token
```

The script will:
1. Attempt to obtain an authentication token automatically
2. Connect to the Linz WebGIS API endpoints
3. Fetch data from the available traffic datasets
4. Process the data and organize it by day where possible
5. Save the data as JSON files in the `data/` directory with structured filenames

## Authentication

The script now includes three methods for authentication:

1. **Automatic authentication** (default) - Uses the OAuth2 endpoint to obtain a token
2. **Manual token via prompt** - Use the `--prompt-token` flag to enter a token interactively 
3. **Fallback prompt** - If automatic authentication fails, the script will offer to let you enter a token manually

If automatic authentication fails and you need to manually obtain a token:

1. Visit the Linz WebGIS portal in your browser
2. Open developer tools (F12) and go to the Network tab
3. Interact with the traffic data visualization 
4. Look for API requests to the `featureanalyzer` endpoints
5. Copy the Bearer token from the Authorization header (it will start with `awse_`)

## Available Datasets

The script accesses the following datasets:

- `VDNB_VKT_VERLAUF_7t_60min_V2_LINZ` - 7-day traffic data
- `VDNB_VKT_VERLAUF_30t_60min_V2_LINZ` - 30-day traffic data

## File Formats

The script saves data in multiple formats:

1. Raw dataset files: `DATASET_NAME_YYYY-MM-DD.json` containing the complete API response
2. Day-specific files: `DATASET_NAME_YYYY-MM-DD.json` containing traffic data for specific days

## Customization

The script contains several parameters that you can adjust:
- `BASE_URL`: The base URL of the Linz WebGIS service
- `API_URL`: The base URL for the featureanalyzer API
- `AUTH_URL`: The URL for OAuth2 authentication
- `DATA_DIR`: The directory where data files will be saved
- `DATASETS`: The list of dataset IDs to fetch
- `ATTRIBUTES`: The attributes to request from the API 