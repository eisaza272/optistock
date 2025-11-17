"""
Items Data Extraction Script

This script queries the Alegra API to fetch items/inventory data,
processes the response, and saves the results to a CSV file.

Author: Your Name
Date: August 2025
"""

import os
import requests
import pandas as pd
from typing import List, Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration constants
API_BASE_URL = "https://api.alegra.com/api/v1"
BATCH_SIZE = 30  # Number of records to fetch per request
OUTPUT_FILE = "items_inventory.csv"

# API credentials from environment variable
AUTHORIZATION_TOKEN = os.getenv("KEY_ALEGRA")
if not AUTHORIZATION_TOKEN:
    raise ValueError("KEY_ALEGRA environment variable is not set")


def fetch_items_data() -> List[Dict[str, Any]]:
    """
    Fetch all items data from Alegra API using pagination.
    
    Returns:
        List[Dict[str, Any]]: List of all items from the API
        
    Raises:
        requests.RequestException: If the API request fails
        ValueError: If the response is not valid JSON
    """
    all_items = []
    start = 0
    
    while True:
        url = f"{API_BASE_URL}/items"
        headers = {
            "accept": "application/json",
            "authorization": AUTHORIZATION_TOKEN
        }
        params = {"start": start}
        
        logging.info(f"Fetching items starting from position: {start}")
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            batch_data = response.json()
            
            # If no more data, break the loop
            if not batch_data or len(batch_data) == 0:
                logging.info("No more items to fetch")
                break
                
            all_items.extend(batch_data)

            # Process and save this batch immediately
            processed_batch = process_items_data(batch_data)
            if processed_batch:
                save_batch_to_csv(processed_batch, OUTPUT_FILE, start == 0)
            
            logging.info(f"Fetched and saved {len(batch_data)} items in this batch")
            
            # If we got fewer records than BATCH_SIZE, we've reached the end
            if len(batch_data) < BATCH_SIZE:
                logging.info("Reached end of data (partial batch received)")
                break
                
            # Move to next batch
            start += BATCH_SIZE
            
        except requests.RequestException as e:
            logging.error(f"API request failed at start={start}: {e}")
            raise
        except ValueError as e:
            logging.error(f"Failed to parse JSON response at start={start}: {e}")
            raise
    
    logging.info(f"Total items fetched: {len(all_items)}")
    return all_items


def process_items_data(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process raw items data and extract relevant information.
    
    Args:
        items (List[Dict[str, Any]]): List of raw items data from API
        
    Returns:
        List[Dict[str, Any]]: List of processed items with normalized structure
    """
    processed_items = []
    
    logging.info(f"Processing {len(items)} items")
    
    for item in items:
        # Extract item-level information with individual field error handling
        item_data = {}
        
        # Handle each field individually, setting to null if error occurs
        try:
            item_data["id"] = item["id"]
        except (KeyError, TypeError):
            item_data["id"] = None
            
        try:
            item_data["name"] = item["name"]
        except (KeyError, TypeError):
            item_data["name"] = None
            
        try:
            item_data["initial_quantity"] = item["inventory"]["initialQuantity"]
        except (KeyError, TypeError):
            item_data["initial_quantity"] = None
            
        try:
            item_data["initial_quantity_date"] = item["inventory"]["initialQuantityDate"]
        except (KeyError, TypeError):
            item_data["initial_quantity_date"] = None
            
        try:
            item_data["final_available_quantity"] = item["inventory"]["availableQuantity"]
        except (KeyError, TypeError):
            item_data["final_available_quantity"] = None
            
        try:
            item_data["photo_url"] = item["images"][0]["url"]
        except (KeyError, TypeError, IndexError):
            item_data["photo_url"] = None
        processed_items.append(item_data)
    
    logging.info(f"Successfully processed {len(processed_items)} items")
    return processed_items


def save_to_csv(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Save processed data to CSV file.
    
    Args:
        data (List[Dict[str, Any]]): List of processed invoice items
        filename (str): Output CSV filename
    """
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logging.info(f"Data successfully saved to {filename}")
        logging.info(f"Total records saved: {len(df)}")
    except Exception as e:
        logging.error(f"Failed to save data to CSV: {e}")
        raise


def save_batch_to_csv(data: List[Dict[str, Any]], filename: str, is_first_batch: bool = False) -> None:
    """
    Save processed batch data to CSV file, appending to existing file or creating new one.
    
    Args:
        data (List[Dict[str, Any]]): List of processed invoice items for this batch
        filename (str): Output CSV filename
        is_first_batch (bool): If True, create new file with headers; if False, append without headers
    """
    try:
        df = pd.DataFrame(data)
        mode = 'w' if is_first_batch else 'a'
        header = is_first_batch
        df.to_csv(filename, mode=mode, header=header, index=False)
        logging.info(f"Batch data ({'with' if header else 'without'} headers) appended to {filename}")
        logging.info(f"Batch records saved: {len(df)}")
    except Exception as e:
        logging.error(f"Failed to save batch data to CSV: {e}")
        raise


def main():
    """
    Main function to orchestrate the items data extraction process.
    """
    try:
        # Fetch all items data from API using pagination
        # Data is processed and saved incrementally as each batch is fetched
        items_data = fetch_items_data()
     
        # At this point, all data has already been processed and saved batch by batch
        logging.info("All items data has been processed and saved incrementally")
        final_output_file = "final_" + OUTPUT_FILE
        logging.info(f"Final CSV file: {final_output_file}")
    
    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise


if __name__ == "__main__":
    main()