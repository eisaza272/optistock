#!/usr/bin/env python3
"""
Example script showing how to upload CSV files to BigQuery.

This script demonstrates how to use the BigQueryCSVUploader class
to upload the warehouse movements CSV file to BigQuery.
"""

from upload_csv_to_bigquery import BigQueryCSVUploader
import os


def main():
    """Example usage of the CSV uploader."""
    
    # Configuration
    PROJECT_ID = "your-project-id"  # Replace with your Google Cloud project ID
    DATASET_ID = "optistock"
    CREDENTIALS_PATH = None  # Path to service account key file, or None to use default credentials
    
    # Initialize the uploader
    uploader = BigQueryCSVUploader(
        project_id=PROJECT_ID,
        credentials_path=CREDENTIALS_PATH
    )
    
    # CSV files to upload with their corresponding table names
    csv_uploads = [
        {
            "csv_file": "warehouse_movements.csv",
            "table_name": "warehouse_movements",
            "write_disposition": "WRITE_TRUNCATE"  # Replace table data
        },
        {
            "csv_file": "items_inventory.csv",
            "table_name": "inventory",
            "write_disposition": "WRITE_TRUNCATE"
        },
        {
            "csv_file": "purchase_orders.csv",
            "table_name": "purchases",
            "write_disposition": "WRITE_TRUNCATE"
        },
        {
            "csv_file": "factura_items.csv",
            "table_name": "sales",
            "write_disposition": "WRITE_TRUNCATE"
        }
    ]
    
    # Upload each CSV file
    for upload_config in csv_uploads:
        csv_file = upload_config["csv_file"]
        table_name = upload_config["table_name"]
        write_disposition = upload_config["write_disposition"]
        
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"Skipping {csv_file} - file not found")
            continue
        
        # Construct full table ID
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        print(f"\n{'='*50}")
        print(f"Uploading {csv_file} to {table_id}")
        print(f"{'='*50}")
        
        # Upload the CSV
        success = uploader.upload_csv_to_table(
            csv_file=csv_file,
            table_id=table_id,
            write_disposition=write_disposition,
            auto_detect_schema=True
        )
        
        if success:
            print(f"✅ Successfully uploaded {csv_file}")
            
            # Show table information
            info = uploader.get_table_info(table_id)
            if info:
                print(f"   📊 Table now contains {info['num_rows']:,} rows")
                print(f"   💾 Size: {info['num_bytes']:,} bytes")
        else:
            print(f"❌ Failed to upload {csv_file}")
    
    # List all tables in the dataset
    print(f"\n{'='*50}")
    print(f"Tables in dataset {DATASET_ID}")
    print(f"{'='*50}")
    tables = uploader.list_tables_in_dataset(DATASET_ID)
    for table in tables:
        print(f"  📋 {table}")


if __name__ == "__main__":
    main()
