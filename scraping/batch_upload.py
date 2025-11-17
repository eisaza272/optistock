#!/usr/bin/env python3
"""
Batch upload script for uploading multiple CSV files to BigQuery.
Uses configuration from config.py
"""

import os
from pathlib import Path
from upload_csv_to_bigquery import BigQueryCSVUploader
from config import (
    PROJECT_ID, DATASET_ID, CREDENTIALS_PATH, 
    DEFAULT_WRITE_DISPOSITION, TABLE_MAPPING
)


def batch_upload_csvs():
    """Upload all configured CSV files to BigQuery."""
    
    print("🚀 Starting batch upload to BigQuery...")
    print(f"   Project: {PROJECT_ID}")
    print(f"   Dataset: {DATASET_ID}")
    print("-" * 50)
    
    # Initialize uploader
    try:
        uploader = BigQueryCSVUploader(
            project_id=PROJECT_ID,
            credentials_path=CREDENTIALS_PATH if CREDENTIALS_PATH != "path/to/your/service-account-key.json" else None
        )
    except Exception as e:
        print(f"❌ Failed to initialize BigQuery client: {e}")
        print("   Make sure to configure your credentials in config.py")
        return False
    
    success_count = 0
    total_files = 0
    
    # Process each CSV file
    for csv_file, table_name in TABLE_MAPPING.items():
        total_files += 1
        
        # Check if file exists
        if not Path(csv_file).exists():
            print(f"⚠️  Skipping {csv_file} - file not found")
            continue
        
        # Construct table ID
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        print(f"\n📂 Processing: {csv_file}")
        print(f"   Target table: {table_id}")
        
        # Get file size for progress info
        file_size = Path(csv_file).stat().st_size
        print(f"   File size: {file_size:,} bytes")
        
        # Upload the file
        try:
            success = uploader.upload_csv_to_table(
                csv_file=csv_file,
                table_id=table_id,
                write_disposition=DEFAULT_WRITE_DISPOSITION,
                auto_detect_schema=True
            )
            
            if success:
                success_count += 1
                print(f"   ✅ Upload successful!")
                
                # Get updated table info
                info = uploader.get_table_info(table_id)
                if info:
                    print(f"   📊 Rows: {info['num_rows']:,}")
                    print(f"   💾 Size: {info['num_bytes']:,} bytes")
            else:
                print(f"   ❌ Upload failed!")
                
        except Exception as e:
            print(f"   ❌ Upload failed with error: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 UPLOAD SUMMARY")
    print("=" * 50)
    print(f"   Total files processed: {total_files}")
    print(f"   Successful uploads: {success_count}")
    print(f"   Failed uploads: {total_files - success_count}")
    
    if success_count > 0:
        print(f"\n🎉 Batch upload completed! {success_count} files uploaded successfully.")
        
        # List final dataset contents
        print(f"\n📋 Tables in dataset '{DATASET_ID}':")
        tables = uploader.list_tables_in_dataset(DATASET_ID)
        for table in tables:
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table}"
            info = uploader.get_table_info(table_id)
            if info:
                print(f"   • {table}: {info['num_rows']:,} rows")
    else:
        print("\n❌ No files were uploaded successfully.")
        return False
    
    return True


def main():
    """Main function."""
    # Validate configuration
    if PROJECT_ID == "your-project-id":
        print("❌ Please update PROJECT_ID in config.py with your actual Google Cloud project ID")
        return
    
    if CREDENTIALS_PATH == "path/to/your/service-account-key.json":
        print("⚠️  No credentials path set. Using default authentication...")
        print("   Make sure you've run: gcloud auth application-default login")
    
    # Run batch upload
    success = batch_upload_csvs()
    
    if not success:
        print("\n💡 Troubleshooting tips:")
        print("   1. Make sure PROJECT_ID is set correctly in config.py")
        print("   2. Verify your Google Cloud authentication")
        print("   3. Check that CSV files exist in the current directory")
        print("   4. Ensure you have BigQuery permissions")


if __name__ == "__main__":
    main()
