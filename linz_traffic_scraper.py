import requests
import json
import os
from datetime import datetime, timedelta
import time
import logging
from pathlib import Path
import sys
import csv
import io
import argparse
from typing import Optional, Dict, List, Any, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("traffic_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://webgis.linz.at/MAppEnterprise"
API_URL = f"{BASE_URL}/api/v1/featureanalyzer/datasets"
AUTH_URL = f"{BASE_URL}/api/v1/oauth2/token"
DATA_DIR = Path("data")
TENANT = "linz_db"
APP_ID = "b20c5cbc-a2be-4890-92e5-a179e44d2daf"

# Available datasets
DATASETS = [
    "VDNB_VKT_VERLAUF_7t_60min_V2_LINZ",   # 7 day traffic data
]

# Attributes to request
ATTRIBUTES = ["pkw", "datum", "ID"]

# Output format options
OUTPUT_FORMAT_JSON = "json"
OUTPUT_FORMAT_CSV = "csv"
OUTPUT_FORMAT = OUTPUT_FORMAT_JSON  # Default output format

# Create data directory if it doesn't exist
DATA_DIR.mkdir(exist_ok=True)

class LinzTrafficScraper:
    """Scrapes traffic data from the Linz WebGIS portal."""
    
    def __init__(self, token: Optional[str] = None):
        """
        Initialize the scraper with optional authentication token.
        
        Args:
            token: Optional authentication token
        """
        self.session = requests.Session()
        
        # Set default headers based on the provided curl command
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Accept": "text/plain, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,de-AT;q=0.8,de;q=0.7",
            "Tenant": TENANT,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": f"{BASE_URL}/Dashboard/?tenant={TENANT}&view=8dcab3bd-ca14-40f6-8631-25a816457cfb&viewer=1&appId=0c86a969-5a3c-4299-b567-8229fc692cca&runtime=dashboard-1",
            "X-Requested-With": "XMLHttpRequest",
            "tenant": TENANT
        })
        
        # Set authorization token if provided
        if token:
            self.set_auth_token(token)
        else:
            logger.warning("No authentication token provided, will attempt to obtain one automatically.")
            print("No authentication token provided, will attempt to obtain one automatically.")
    
    def set_auth_token(self, token: str) -> None:
        """
        Set the authorization token for API requests.
        
        Args:
            token: The authorization token
        """
        self.session.headers["Authorization"] = f"Bearer {token}"
        logger.info("Authorization token set")
    
    def get_auth_token(self) -> Optional[str]:
        """
        Attempt to obtain an authentication token automatically.
        
        Returns:
            The authentication token or None if the request failed
        """
        logger.info("Attempting to obtain authentication token automatically...")
        
        # Set specific headers for the authentication request
        auth_headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9,de-AT;q=0.8,de;q=0.7",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "DNT": "1",
            "Origin": "https://webgis.linz.at",
            "Pragma": "no-cache",
            "Referer": f"{BASE_URL}/Apps/?appId={APP_ID}&tenant={TENANT}",
            "tenant": TENANT,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        }
        
        # Prepare the form data for the authentication request
        auth_data = {
            "grant_type": "password",
            "client_id": "App",
            "scope": "public"
        }
        
        try:
            # Make the authentication request
            response = requests.post(
                AUTH_URL,
                headers=auth_headers,
                data=auth_data,
                timeout=30  # Added timeout
            )
            
            # Check if the request was successful
            if response.status_code == 200:
                auth_result = response.json()
                if "access_token" in auth_result:
                    token = auth_result["access_token"]
                    logger.info("Successfully obtained authentication token automatically")
                    print("Successfully obtained authentication token automatically")
                    
                    # Set the token in the session
                    self.set_auth_token(token)
                    return token
                else:
                    logger.warning("Authentication response did not contain an access token")
                    print("Authentication response did not contain an access token")
            else:
                logger.warning(f"Failed to obtain authentication token. Status code: {response.status_code}")
                print(f"Failed to obtain authentication token. Status code: {response.status_code}")
                if response.text:
                    logger.debug(f"Authentication response: {response.text}")
            
            return None
        
        except Exception as e:
            logger.error(f"Error obtaining authentication token: {e}")
            print(f"Error obtaining authentication token: {e}")
            return None
    
    def fetch_dataset(self, dataset_name: str) -> Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]:
        """
        Fetch data from a specific dataset.
        
        Args:
            dataset_name: Name of the dataset to fetch
            
        Returns:
            Dict containing the traffic data or None if request failed
        """
        logger.info(f"Fetching data from dataset: {dataset_name}")
        
        # Construct the URL
        attributes_param = ",".join(ATTRIBUTES)
        url = f"{API_URL}/{dataset_name}/values?attributes={attributes_param}"
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                response = self.session.get(url, timeout=60)  # Added timeout
                
                # Handle unauthorized errors specifically
                if response.status_code == 401:
                    logger.error(f"Authentication failed (401 Unauthorized) for dataset {dataset_name}")
                    logger.error("The Bearer token has likely expired. Attempting to obtain a new token automatically...")
                    print("\nERROR: Authentication failed (401 Unauthorized)")
                    print("The Bearer token has expired. Attempting to obtain a new token automatically...")
                    
                    # Try to get a new token
                    new_token = self.get_auth_token()
                    if new_token:
                        # Retry the request with the new token
                        response = self.session.get(url, timeout=60)
                        if response.status_code == 401:
                            logger.error("Still unauthorized after obtaining a new token. Manual intervention required.")
                            print("\nERROR: Still unauthorized after obtaining a new token.")
                            print("Manual intervention may be required.")
                            print("Consider providing a token manually with the --token flag")
                            return None
                    else:
                        print("Failed to obtain new token automatically.")
                        print("Please obtain a new token manually:")
                        print("1. Visit the Linz WebGIS portal in your browser")
                        print("2. Open developer tools (F12) and go to the Network tab")
                        print("3. Interact with the traffic data visualization")
                        print("4. Look for API requests to the featureanalyzer endpoints")
                        print("5. Copy the Bearer token from the Authorization header")
                        print("6. Update the token in the script or use --token\n")
                        return None
                
                # Handle other error codes
                if response.status_code != 200:
                    logger.error(f"Error fetching data: {response.status_code} - {response.reason}")
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count  # Exponential backoff
                        logger.info(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Max retries reached for {dataset_name}")
                        return None
                
                # Check if the response is JSON
                try:
                    data = response.json()
                    logger.info(f"Successfully fetched data from dataset: {dataset_name}")
                    return data
                except json.JSONDecodeError:
                    # If not JSON, try to process it as CSV
                    logger.info(f"Response is not JSON. Processing as CSV from dataset: {dataset_name}")
                    return self.process_csv_response(response.text)
                
            except requests.RequestException as e:
                logger.error(f"Error fetching data from dataset {dataset_name}: {e}")
                if hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'text'):
                    logger.error(f"Response text: {e.response.text[:500]}...")
                
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for {dataset_name}")
                    return None
        
        return None

    def process_csv_response(self, csv_text: str) -> Union[List[Dict[str, str]], Dict[str, str]]:
        """
        Process the raw CSV text response into a structured format.
        
        Args:
            csv_text: Raw CSV text from the API
            
        Returns:
            Either a list of dictionaries (for JSON output) or the cleaned CSV text (for CSV output)
        """
        try:
            # Clean up CSV text - remove any BOM characters or other non-standard characters
            csv_text = csv_text.strip()
            
            # Parse CSV using Python's csv module
            csv_reader = csv.reader(io.StringIO(csv_text), delimiter=',', quotechar='"')
            
            # Get headers from first row
            try:
                headers = next(csv_reader)
                if not headers:
                    logger.error("CSV has no headers")
                    return {"raw_text": csv_text}
                
                headers = [h.strip() for h in headers]
                
                if OUTPUT_FORMAT == OUTPUT_FORMAT_JSON:
                    # Convert to list of dictionaries for JSON output
                    result = []
                    for row in csv_reader:
                        if row and len(row) == len(headers):
                            # Create a dictionary mapping headers to values
                            entry = {headers[i]: row[i].strip() for i in range(len(headers))}
                            result.append(entry)
                    logger.info(f"Processed CSV response into {len(result)} JSON records")
                    return result
                else:
                    # For CSV output, just return the cleaned CSV text
                    # Rebuild the CSV with consistent formatting
                    output = io.StringIO()
                    csv_writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
                    
                    # Write headers
                    csv_writer.writerow(headers)
                    
                    # Write data rows
                    rows_written = 0
                    for row in csv_reader:
                        if row and len(row) == len(headers):
                            csv_writer.writerow(row)
                            rows_written += 1
                    
                    cleaned_csv = output.getvalue()
                    logger.info(f"Processed and cleaned CSV response with {rows_written} rows")
                    return {"csv_text": cleaned_csv}
            except StopIteration:
                logger.error("CSV is empty or malformed")
                return {"raw_text": csv_text}
                
        except Exception as e:
            logger.error(f"Error processing CSV response: {e}")
            # If processing fails, return the raw text
            return {"raw_text": csv_text}

    def save_dataset(self, dataset_name: str, data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Optional[Path]:
        """
        Save dataset data to a file.
        
        Args:
            dataset_name: Name of the dataset
            data: The data to save (either a dict/list for JSON or CSV text)
            
        Returns:
            Path to the saved file or None if save failed
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Determine the appropriate file extension and format
        if isinstance(data, dict) and "csv_text" in data:
            # This is CSV data
            filename = DATA_DIR / f"{dataset_name}_{today}.csv"
            try:
                with open(filename, 'w', encoding='utf-8', newline='') as f:
                    f.write(data["csv_text"])
                logger.info(f"Saved CSV data from dataset {dataset_name} to {filename}")
                print(f"Saved CSV data to {filename}")
                return filename
            except Exception as e:
                logger.error(f"Error saving CSV data to {filename}: {e}")
                return None
        else:
            # This is JSON data
            filename = DATA_DIR / f"{dataset_name}_{today}.json"
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved JSON data from dataset {dataset_name} to {filename}")
                print(f"Saved JSON data to {filename}")
                return filename
            except Exception as e:
                logger.error(f"Error saving JSON data to {filename}: {e}")
                return None
    
    def process_traffic_data(self, data: Union[List[Dict[str, Any]], Dict[str, Any]], dataset_name: str) -> Dict[str, Any]:
        """
        Process and organize traffic data by day.
        
        Args:
            data: Raw data from the API
            dataset_name: Name of the dataset
            
        Returns:
            Dict mapping dates to traffic data
        """
        logger.info(f"Processing data from dataset: {dataset_name}")
        
        # Check if we have raw text or CSV text
        if isinstance(data, dict) and ("raw_text" in data or "csv_text" in data):
            logger.warning(f"Data from {dataset_name} is text data, cannot process by day")
            return data  # Just pass through the raw or CSV text
            
        # If we have JSON data (as a list of dictionaries)
        if isinstance(data, list):
            # Organize data by date
            organized_data = {}
            skipped_records = 0
            
            for record in data:
                if not isinstance(record, dict):
                    skipped_records += 1
                    continue
                    
                if "datum" in record:
                    try:
                        date = record["datum"]
                        # Try to standardize date format
                        if " " in date:  # Contains time component
                            date_key = date.split(" ")[0]  # Extract just the date part
                        else:
                            date_key = date
                        
                        # Normalize the date format to MM-DD-YYYY
                        if "/" in date_key:
                            date_parts = date_key.split("/")
                            if len(date_parts) == 3:
                                date_key = f"{date_parts[0]}-{date_parts[1]}-{date_parts[2]}"
                        
                        if date_key not in organized_data:
                            organized_data[date_key] = []
                        
                        organized_data[date_key].append(record)
                    except Exception as e:
                        logger.warning(f"Error processing record date: {e}")
                        skipped_records += 1
                else:
                    logger.warning(f"Record missing 'datum' field: {record}")
                    skipped_records += 1
            
            if skipped_records > 0:
                logger.warning(f"Skipped {skipped_records} records due to missing or invalid data")
                
            logger.info(f"Organized data from {dataset_name} by {len(organized_data)} dates")
            return organized_data
        
        # If the data isn't a list or a dict with raw/csv text, return as is
        logger.warning(f"Unexpected data format for {dataset_name}: {type(data)}")
        return {"unexpected_format": True, "raw_data": data}
    
    def save_data_by_day(self, organized_data: Dict[str, Any], dataset_name: str) -> None:
        """
        Save the organized traffic data with one file per day.
        
        Args:
            organized_data: Dict mapping dates to traffic data
            dataset_name: Name of the dataset
        """
        # Check for CSV data
        if isinstance(organized_data, dict) and "csv_text" in organized_data:
            # For CSV data, we already saved the full file in save_dataset
            logger.info(f"CSV data from {dataset_name} already saved in main file")
            return
            
        # Check for raw text data
        if isinstance(organized_data, dict) and "raw_text" in organized_data:
            # We couldn't organize by day, save as is
            if OUTPUT_FORMAT == OUTPUT_FORMAT_CSV:
                filename = DATA_DIR / f"{dataset_name}_raw_{datetime.now().strftime('%Y-%m-%d')}.csv"
                try:
                    with open(filename, 'w', encoding='utf-8', newline='') as f:
                        f.write(organized_data["raw_text"])
                    print(f"Saved raw data to {filename}")
                except Exception as e:
                    logger.error(f"Error saving raw CSV data: {e}")
            else:
                filename = DATA_DIR / f"{dataset_name}_raw_{datetime.now().strftime('%Y-%m-%d')}.json"
                try:
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(organized_data["raw_text"], f, indent=2, ensure_ascii=False)
                    print(f"Saved raw data to {filename}")
                except Exception as e:
                    logger.error(f"Error saving raw JSON data: {e}")
                    
            logger.info(f"Saved raw data from {dataset_name} to {filename}")
            return
            
        # For unexpected format, save as is
        if isinstance(organized_data, dict) and "unexpected_format" in organized_data:
            filename = DATA_DIR / f"{dataset_name}_unexpected_{datetime.now().strftime('%Y-%m-%d')}.json"
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(organized_data["raw_data"], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved unexpected format data from {dataset_name} to {filename}")
                print(f"Saved data with unexpected format to {filename}")
            except Exception as e:
                logger.error(f"Error saving unexpected format data: {e}")
            return
            
        # For JSON data organized by day, save each day as a separate file
        files_saved = 0
        for date, data in organized_data.items():
            # Clean up the date string if needed
            date_str = date.replace('/', '-')
            
            if OUTPUT_FORMAT == OUTPUT_FORMAT_CSV:
                # Convert the JSON data back to CSV for this date
                filename = DATA_DIR / f"{dataset_name}_{date_str}.csv"
                try:
                    # Extract headers from the first record
                    if data and len(data) > 0:
                        headers = list(data[0].keys())
                        
                        with open(filename, 'w', encoding='utf-8', newline='') as f:
                            csv_writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_ALL)
                            csv_writer.writeheader()
                            csv_writer.writerows(data)
                            
                        logger.info(f"Saved CSV data for {date_str} from {dataset_name} to {filename}")
                        files_saved += 1
                    else:
                        logger.warning(f"No data to save for {date_str}")
                except Exception as e:
                    logger.error(f"Error saving CSV data for {date_str} to {filename}: {e}")
            else:
                # Save as JSON
                filename = DATA_DIR / f"{dataset_name}_{date_str}.json"
                try:
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    logger.info(f"Saved JSON data for {date_str} from {dataset_name} to {filename}")
                    files_saved += 1
                except Exception as e:
                    logger.error(f"Error saving JSON data for {date_str} to {filename}: {e}")
        
        if files_saved > 0:
            print(f"Saved {files_saved} daily data files for {dataset_name}")

    def fetch_and_save_all_datasets(self) -> bool:
        """
        Fetch and save data from all available datasets.
        
        Returns:
            True if at least one dataset was successfully fetched and saved
        """
        success = False
        total_datasets = len(DATASETS)
        successful_datasets = 0
        
        for dataset in DATASETS:
            print(f"\nFetching dataset: {dataset}")
            data = self.fetch_dataset(dataset)
            if data:
                successful_datasets += 1
                success = True
                # First save the raw dataset
                self.save_dataset(dataset, data)
                
                # Then try to process and organize by day
                organized_data = self.process_traffic_data(data, dataset)
                self.save_data_by_day(organized_data, dataset)
            else:
                logger.warning(f"No data available for dataset: {dataset}")
                print(f"Failed to fetch data for dataset: {dataset}")
            
            # Be nice to the server
            time.sleep(2)
        
        print(f"\nSuccessfully processed {successful_datasets} out of {total_datasets} datasets")    
        return success

def prompt_for_token() -> str:
    """
    Prompt the user to enter a new Bearer token.
    
    Returns:
        The token entered by the user
    """
    print("\nPlease enter a new Bearer token for authentication:")
    print("(You can get this from the Network tab in browser developer tools)")
    print("Token should start with 'awse_' and is quite long.\n")
    
    token = input("Bearer token: ").strip()
    
    if not token:
        print("No token provided. Exiting.")
        sys.exit(1)
        
    if not token.startswith("awse_"):
        print("Warning: Token does not start with 'awse_'. This may not be a valid token.")
        
    return token

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Parsed argument namespace
    """
    parser = argparse.ArgumentParser(description="Linz Traffic Data Scraper")
    
    parser.add_argument("--token", help="Bearer token for authentication")
    parser.add_argument("--prompt-token", action="store_true", help="Prompt for a bearer token")
    parser.add_argument("--format", choices=[OUTPUT_FORMAT_JSON, OUTPUT_FORMAT_CSV], 
                        default=OUTPUT_FORMAT_JSON, help="Output format (json or csv)")
    parser.add_argument("--datasets", nargs="+", choices=DATASETS, 
                        help="Specific datasets to fetch (default: all)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    return parser.parse_args()

def main():
    """Main entry point for the scraper."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Set logging level based on debug flag
        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.debug("Debug logging enabled")
        
        # Set output format
        global OUTPUT_FORMAT
        OUTPUT_FORMAT = args.format
        print(f"Output format set to: {OUTPUT_FORMAT}")
        
        # Get token either from args or by prompting
        token = None
        if args.token:
            token = args.token
        elif args.prompt_token:
            token = prompt_for_token()
        
        # Override datasets if specified
        global DATASETS
        datasets_to_fetch = args.datasets if args.datasets else DATASETS
        if args.datasets:
            print(f"Will fetch only the following datasets: {', '.join(datasets_to_fetch)}")
            DATASETS = datasets_to_fetch
        
        logger.info("Starting Linz Traffic Data Scraper")
        scraper = LinzTrafficScraper(token)
        
        # If no token was provided, try to get one automatically
        if not token:
            token = scraper.get_auth_token()
            
            # If automatic token fetching failed, offer to use manual method
            if not token:
                print("\nFailed to obtain authentication token automatically.")
                print("Would you like to enter a token manually? (y/n)")
                if input().lower().startswith('y'):
                    token = prompt_for_token()
                    scraper.set_auth_token(token)
        
        # Fetch and save data from all datasets
        success = scraper.fetch_and_save_all_datasets()
        
        if not success:
            print("\nNo datasets were successfully fetched.")
            print("You may need to provide a new authentication token.")
            print("Run the script with the --token or --prompt-token flag:")
            print("    python linz_traffic_scraper.py --prompt-token\n")
        
        logger.info("Completed data scraping")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 