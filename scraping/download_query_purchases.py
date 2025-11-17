"""
Purchase Orders Data Extraction Script

This script queries the Alegra API to fetch purchase orders data,
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
OUTPUT_FILE = "purchase_orders.csv"

# API credentials from environment variable
AUTHORIZATION_TOKEN = os.getenv("KEY_ALEGRA")
if not AUTHORIZATION_TOKEN:
    raise ValueError("KEY_ALEGRA environment variable is not set")


def fetch_purchase_orders_data() -> List[Dict[str, Any]]:
    """
    Fetch all purchase orders data from Alegra API using pagination.
    
    Returns:
        List[Dict[str, Any]]: List of all purchase orders from the API
        
    Raises:
        requests.RequestException: If the API request fails
        ValueError: If the response is not valid JSON
    """
    all_orders = []
    start = 0
    
    while True:
        url = f"{API_BASE_URL}/purchase-orders"
        headers = {
            "accept": "application/json",
            "authorization": AUTHORIZATION_TOKEN
        }
        params = {"start": start, "order_direction": "ASC"}
        
        logging.info(f"Fetching purchase orders starting from position: {start}")
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            batch_data = response.json()
            
            # If no more data, break the loop
            if not batch_data or len(batch_data) == 0:
                logging.info("No more purchase orders to fetch")
                break
                
            all_orders.extend(batch_data)

            # Process and save this batch immediately
            processed_batch = process_purchase_orders_data(batch_data)
            if processed_batch:
                save_batch_to_csv(processed_batch, OUTPUT_FILE, start == 0)
            
            logging.info(f"Fetched and saved {len(batch_data)} purchase orders in this batch")
            
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
    
    logging.info(f"Total purchase orders fetched: {len(all_orders)}")
    return all_orders


def process_purchase_orders_data(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process raw purchase orders data and extract relevant information.
    
    Args:
        orders (List[Dict[str, Any]]): List of raw purchase orders data from API
        
    Returns:
        List[Dict[str, Any]]: List of processed purchase order items with normalized structure
    """
    processed_items = []
    
    logging.info(f"Processing {len(orders)} purchase orders")
    
    for order in orders:
        # Extract order-level information
        try:
            order_id = order["id"]
            delivery_date = order.get("deliveryDate")
            warehouse_name = order.get("warehouse", {}).get("name") if order.get("warehouse") else None
            
            # Process items within each purchase order
            purchases_items = order.get("purchases", {}).get("items", [])
            if not purchases_items and "items" in order:
                purchases_items = order["items"]
            
            for item in purchases_items:
                item_data = {}
                
                # Handle each field individually with error handling
                try:
                    item_data["invoice_id"] = order_id
                except (KeyError, TypeError):
                    item_data["invoice_id"] = None
                    
                try:
                    item_data["added_inventory_date"] = delivery_date
                except (KeyError, TypeError):
                    item_data["added_inventory_date"] = None
                    
                try:
                    item_data["provider_id"] = order_id  # Using order ID as provider reference
                except (KeyError, TypeError):
                    item_data["provider_id"] = None
                    
                try:
                    item_data["warehouse_name"] = warehouse_name
                except (KeyError, TypeError):
                    item_data["warehouse_name"] = None
                    
                try:
                    item_data["price_provider"] = item.get("price")
                except (KeyError, TypeError):
                    item_data["price_provider"] = None
                    
                try:
                    item_data["quantity"] = item.get("quantity")
                except (KeyError, TypeError):
                    item_data["quantity"] = None
                    
                try:
                    item_data["item_id"] = item.get("id")
                except (KeyError, TypeError):
                    item_data["item_id"] = None
                    
                try:
                    item_data["item_name"] = item.get("name")
                except (KeyError, TypeError):
                    item_data["item_name"] = None

                processed_items.append(item_data)
                
        except Exception as e:
            logging.warning(f"Error processing order {order.get('id', 'unknown')}: {e}")
            continue
    
    logging.info(f"Successfully processed {len(processed_items)} purchase order items")
    return processed_items


def save_to_csv(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Save processed data to CSV file.
    
    Args:
        data (List[Dict[str, Any]]): List of processed purchase order items
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
        data (List[Dict[str, Any]]): List of processed purchase order items for this batch
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
    Main function to orchestrate the purchase orders data extraction process.
    """
    try:
        # Fetch all purchase orders data from API using pagination
        # Data is processed and saved incrementally as each batch is fetched
        orders_data = fetch_purchase_orders_data()
     
        # At this point, all data has already been processed and saved batch by batch
        logging.info("All purchase orders data has been processed and saved incrementally")
        final_output_file = OUTPUT_FILE
        logging.info(f"Final CSV file: {final_output_file}")
    
    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise


if __name__ == "__main__":
    main()