#!/usr/bin/env python3
"""
Script to upload CSV files to BigQuery tables.

This script provides functionality to:
1. Upload CSV files to BigQuery tables
2. Create tables if they don't exist
3. Handle schema inference or use predefined schemas
4. Support different write dispositions (append, overwrite, etc.)

Usage:
    python upload_csv_to_bigquery.py --csv_file path/to/file.csv --table_id project.dataset.table
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core import exceptions


class BigQueryCSVUploader:
    """Class to handle CSV uploads to BigQuery."""
    
    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Initialize the BigQuery client.
        
        Args:
            project_id: Google Cloud project ID. If None, uses default from environment.
            credentials_path: Path to service account key file. If None, uses default credentials.
        """
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        try:
            self.client = bigquery.Client(project=project_id)
            self.project_id = self.client.project
            print(f"Connected to BigQuery project: {self.project_id}")
        except Exception as e:
            print(f"Error initializing BigQuery client: {e}")
            sys.exit(1)
    
    def create_dataset_if_not_exists(self, dataset_id: str, location: str = "US") -> None:
        """
        Create a dataset if it doesn't exist.
        
        Args:
            dataset_id: The dataset ID (not the full dataset reference).
            location: The location for the dataset.
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_ref} already exists.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            print(f"Created dataset {dataset_ref}")
    
    def infer_schema_from_csv(self, csv_file: str, sample_size: int = 1000) -> List[bigquery.SchemaField]:
        """
        Infer BigQuery schema from CSV file.
        
        Args:
            csv_file: Path to the CSV file.
            sample_size: Number of rows to sample for schema inference.
        
        Returns:
            List of BigQuery schema fields.
        """
        try:
            # Read a sample of the CSV to infer types
            df_sample = pd.read_csv(csv_file, nrows=sample_size)
            
            schema = []
            for column, dtype in df_sample.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    field_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    field_type = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    field_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    field_type = "TIMESTAMP"
                else:
                    field_type = "STRING"
                
                schema.append(bigquery.SchemaField(column, field_type))
            
            return schema
        except Exception as e:
            print(f"Error inferring schema: {e}")
            return []
    
    def get_predefined_schema(self, table_name: str) -> Optional[List[bigquery.SchemaField]]:
        """
        Get predefined schema for known tables.
        
        Args:
            table_name: Name of the table.
        
        Returns:
            List of BigQuery schema fields or None if no predefined schema exists.
        """
        schemas = {
            "warehouse_movements": [
                bigquery.SchemaField("movement_date", "DATE"),
                bigquery.SchemaField("warehouse_origin", "STRING"),
                bigquery.SchemaField("warehouse_destination", "STRING"),
                bigquery.SchemaField("item_id", "INTEGER"),
                bigquery.SchemaField("item_name", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
            ],
            "inventory": [
                bigquery.SchemaField("item_id", "INTEGER"),
                bigquery.SchemaField("item_name", "STRING"),
                bigquery.SchemaField("warehouse", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("last_updated", "TIMESTAMP"),
            ],
            "sales": [
                bigquery.SchemaField("sale_date", "DATE"),
                bigquery.SchemaField("item_id", "INTEGER"),
                bigquery.SchemaField("item_name", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("unit_price", "FLOAT"),
                bigquery.SchemaField("total_amount", "FLOAT"),
            ],
            "purchases": [
                bigquery.SchemaField("purchase_date", "DATE"),
                bigquery.SchemaField("item_id", "INTEGER"),
                bigquery.SchemaField("item_name", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("unit_cost", "FLOAT"),
                bigquery.SchemaField("total_cost", "FLOAT"),
                bigquery.SchemaField("supplier", "STRING"),
            ]
        }
        
        return schemas.get(table_name.lower())
    
    def upload_csv_to_table(
        self,
        csv_file: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
        schema: Optional[List[bigquery.SchemaField]] = None,
        auto_detect_schema: bool = True,
        skip_leading_rows: int = 1,
        field_delimiter: str = ",",
        encoding: str = "UTF-8"
    ) -> bool:
        """
        Upload a CSV file to a BigQuery table.
        
        Args:
            csv_file: Path to the CSV file.
            table_id: Full table ID (project.dataset.table).
            write_disposition: How to handle existing data (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY).
            schema: Optional schema. If None, will try to infer or use predefined schema.
            auto_detect_schema: Whether to auto-detect schema from CSV.
            skip_leading_rows: Number of header rows to skip.
            field_delimiter: CSV field delimiter.
            encoding: File encoding.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            # Validate file exists
            if not Path(csv_file).exists():
                print(f"Error: File {csv_file} does not exist.")
                return False
            
            # Parse table ID
            parts = table_id.split('.')
            if len(parts) != 3:
                print(f"Error: Table ID must be in format 'project.dataset.table', got: {table_id}")
                return False
            
            project_id, dataset_id, table_name = parts
            
            # Create dataset if it doesn't exist
            self.create_dataset_if_not_exists(dataset_id)
            
            # Get table reference
            table_ref = self.client.dataset(dataset_id, project=project_id).table(table_name)
            
            # Determine schema
            if schema is None:
                # Try predefined schema first
                schema = self.get_predefined_schema(table_name)
                if schema is None and not auto_detect_schema:
                    # Infer schema from CSV
                    schema = self.infer_schema_from_csv(csv_file)
                    if not schema:
                        print("Error: Could not determine schema for the table.")
                        return False
            
            # Configure job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=skip_leading_rows,
                field_delimiter=field_delimiter,
                encoding=encoding,
                write_disposition=write_disposition,
                autodetect=auto_detect_schema and schema is None,
                schema=schema if schema else None,
            )
            
            # Upload file
            print(f"Uploading {csv_file} to {table_id}...")
            print(f"Write disposition: {write_disposition}")
            
            with open(csv_file, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )
            
            # Wait for job to complete
            job.result()  # Waits for the job to complete
            
            # Get table info
            table = self.client.get_table(table_ref)
            print(f"Successfully uploaded {table.num_rows} rows to {table_id}")
            
            return True
            
        except exceptions.BadRequest as e:
            print(f"Bad request error: {e}")
            if hasattr(e, 'errors'):
                for error in e.errors:
                    print(f"  - {error}")
            return False
        except Exception as e:
            print(f"Error uploading CSV to BigQuery: {e}")
            return False
    
    def list_tables_in_dataset(self, dataset_id: str) -> List[str]:
        """
        List all tables in a dataset.
        
        Args:
            dataset_id: The dataset ID.
        
        Returns:
            List of table names.
        """
        try:
            dataset_ref = f"{self.project_id}.{dataset_id}"
            tables = self.client.list_tables(dataset_ref)
            return [table.table_id for table in tables]
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []
    
    def get_table_info(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a table.
        
        Args:
            table_id: Full table ID (project.dataset.table).
        
        Returns:
            Dictionary with table information or None if table doesn't exist.
        """
        try:
            table = self.client.get_table(table_id)
            return {
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created,
                "modified": table.modified,
                "schema": [{"name": field.name, "type": field.field_type} for field in table.schema]
            }
        except NotFound:
            print(f"Table {table_id} not found.")
            return None
        except Exception as e:
            print(f"Error getting table info: {e}")
            return None


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(description="Upload CSV files to BigQuery")
    parser.add_argument("--csv_file", required=True, help="Path to the CSV file")
    parser.add_argument("--table_id", required=True, 
                       help="BigQuery table ID (format: project.dataset.table)")
    parser.add_argument("--project_id", help="Google Cloud project ID")
    parser.add_argument("--credentials_path", help="Path to service account key file")
    parser.add_argument("--write_disposition", default="WRITE_APPEND",
                       choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
                       help="Write disposition (default: WRITE_APPEND)")
    parser.add_argument("--auto_detect_schema", action="store_true", default=True,
                       help="Auto-detect schema from CSV")
    parser.add_argument("--skip_leading_rows", type=int, default=1,
                       help="Number of header rows to skip (default: 1)")
    parser.add_argument("--field_delimiter", default=",",
                       help="CSV field delimiter (default: ,)")
    parser.add_argument("--encoding", default="UTF-8",
                       help="File encoding (default: UTF-8)")
    parser.add_argument("--list_tables", help="List tables in dataset")
    parser.add_argument("--table_info", help="Get information about a table")
    
    args = parser.parse_args()
    
    # Initialize uploader
    uploader = BigQueryCSVUploader(
        project_id=args.project_id,
        credentials_path=args.credentials_path
    )
    
    # Handle different operations
    if args.list_tables:
        tables = uploader.list_tables_in_dataset(args.list_tables)
        print(f"Tables in dataset {args.list_tables}:")
        for table in tables:
            print(f"  - {table}")
        return
    
    if args.table_info:
        info = uploader.get_table_info(args.table_info)
        if info:
            print(f"Table information for {args.table_info}:")
            print(f"  Rows: {info['num_rows']:,}")
            print(f"  Size: {info['num_bytes']:,} bytes")
            print(f"  Created: {info['created']}")
            print(f"  Modified: {info['modified']}")
            print("  Schema:")
            for field in info['schema']:
                print(f"    - {field['name']}: {field['type']}")
        return
    
    # Upload CSV
    success = uploader.upload_csv_to_table(
        csv_file=args.csv_file,
        table_id=args.table_id,
        write_disposition=args.write_disposition,
        auto_detect_schema=args.auto_detect_schema,
        skip_leading_rows=args.skip_leading_rows,
        field_delimiter=args.field_delimiter,
        encoding=args.encoding
    )
    
    if success:
        print("CSV upload completed successfully!")
        
        # Show table info after upload
        info = uploader.get_table_info(args.table_id)
        if info:
            print(f"\nTable now contains {info['num_rows']:,} rows")
    else:
        print("CSV upload failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
