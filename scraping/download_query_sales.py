"""
Invoice Data Extraction Script

This script queries the Alegra API to fetch invoice data for a specific item,
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
OUTPUT_FILE = "factura_items.csv"

# API credentials from environment variable
AUTHORIZATION_TOKEN = os.getenv("KEY_ALEGRA")
if not AUTHORIZATION_TOKEN:
    raise ValueError("KEY_ALEGRA environment variable is not set")


def fetch_invoice_data(item_id: str = None) -> List[Dict[str, Any]]:
    """
    Fetch all invoice data from Alegra API using pagination.
    
    Args:
        item_id (str, optional): If provided, filter invoices by this item ID
    
    Returns:
        List[Dict[str, Any]]: List of all invoices from the API
        
    Raises:
        requests.RequestException: If the API request fails
        ValueError: If the response is not valid JSON
    """
    all_invoices = []
    start = 0
    
    while True:
        url = f"{API_BASE_URL}/invoices"
        headers = {
            "accept": "application/json",
            "authorization": AUTHORIZATION_TOKEN
        }
        params = {"start": start}
        
        # Add item_id filter if provided
        if item_id:
            params["item_id"] = item_id
            logging.info(f"Fetching invoices for item ID {item_id}, starting from position: {start}")
        else:
            logging.info(f"Fetching all invoices starting from position: {start}")
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            batch_data = response.json()
            
            # If no more data, break the loop
            if not batch_data or len(batch_data) == 0:
                logging.info("No more invoices to fetch")
                break
                
            all_invoices.extend(batch_data)

            # Process and save this batch immediately
            processed_batch = process_invoice_data(batch_data)
            if processed_batch:
                save_batch_to_csv(processed_batch, OUTPUT_FILE, start == 0)
            
            logging.info(f"Fetched and saved {len(batch_data)} invoices in this batch")
            
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
    
    logging.info(f"Total invoices fetched: {len(all_invoices)}")
    return all_invoices


def process_invoice_data(invoices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process raw invoice data and extract item information.
    
    Args:
        invoices (List[Dict[str, Any]]): List of raw invoice data from API
        
    Returns:
        List[Dict[str, Any]]: List of processed invoice items with normalized structure
    """
    processed_items = []
    
    logging.info(f"Processing {len(invoices)} invoices")
    
    for invoice in invoices:
        try:
            # Extract invoice-level information
            invoice_id = invoice.get("id")
            sale_date = invoice.get("date")
            items = invoice.get("items", [])
            warehouse_name = invoice.get("warehouse", {}).get("name", "Unknown")
            
            # Process each item in the invoice
            for item in items:
                item_data = {
                    "factura_id": invoice_id,
                    "fecha_venta": sale_date,
                    "item_id": item.get("id"),
                    "item_name": item.get("name"),
                    "item_quantity": item.get("quantity"),
                    "warehouse_name": warehouse_name
                }
                processed_items.append(item_data)
                
        except (KeyError, TypeError) as e:
            logging.warning(f"Error processing invoice {invoice.get('id', 'Unknown')}: {e}")
            continue
    
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
    Main function to orchestrate the invoice data extraction process.
    """
    try:
        # Fetch all invoice data from API using pagination
        # Data is processed and saved incrementally as each batch is fetched
        # To filter by specific item ID, pass it as parameter: fetch_invoice_data("222")
        invoice_data = fetch_invoice_data()
     
        # At this point, all data has already been processed and saved batch by batch
        logging.info("All invoice data has been processed and saved incrementally")
        OUTPUT_FILE = "final_" + OUTPUT_FILE
        logging.info(f"Final CSV file: {OUTPUT_FILE}")
    
    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise


if __name__ == "__main__":
    main()