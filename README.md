# OptiStock v2

A Python-based data pipeline for extracting inventory, sales, purchase, and warehouse movement data from the Alegra API and uploading it to Google BigQuery for analytics and reporting.

## Overview

OptiStock v2 automates the process of:
- Fetching inventory and transaction data from the Alegra API
- Processing and normalizing the data
- Uploading CSV files to Google BigQuery
- Supporting batch operations for multiple data sources

## Features

- **Data Extraction**: Automated scripts to fetch data from Alegra API endpoints:
  - Inventory/Items data
  - Sales/Invoice data
  - Purchase orders
  - Warehouse movements/transfers
  - Warehouse-specific inventory

- **BigQuery Integration**: 
  - Automatic schema detection
  - Batch upload capabilities
  - Dataset and table management
  - Support for append/truncate write modes

- **Data Processing**:
  - Pagination handling for large datasets
  - Incremental batch processing
  - Error handling and logging
  - CSV export functionality

## Requirements

- Python 3.13+
- Google Cloud Platform account with BigQuery enabled
- Alegra API credentials

## Installation

### Using Poetry (Recommended)

```bash
poetry install
```

### Using pip

```bash
pip install -r requirements.txt
```

### Dependencies

- `requests` >= 2.32.4 - For API calls
- `pandas` >= 2.3.1 - For data processing
- `google-cloud-bigquery` >= 3.25.0 - For BigQuery operations
- `google-cloud-bigquery-storage` >= 2.25.0 - For BigQuery storage operations

## Configuration

### 1. Google Cloud Setup

1. Create a Google Cloud project or use an existing one
2. Enable the BigQuery API
3. Set up authentication using one of the following methods:

   **Option A: Service Account Key**
   - Create a service account in Google Cloud Console
   - Download the JSON key file
   - Update `CREDENTIALS_PATH` in `config.py`

   **Option B: Application Default Credentials**
   - Run: `gcloud auth application-default login`
   - Set `CREDENTIALS_PATH = None` in `config.py`

### 2. Update Configuration File

Edit `config.py` with your settings:

```python
PROJECT_ID = "your-project-id"  # Your GCP project ID
DATASET_ID = "optistock"         # BigQuery dataset name
LOCATION = "US"                  # Dataset location
CREDENTIALS_PATH = "path/to/your/service-account-key.json"  # Or None for ADC
```

### 3. Alegra API Credentials

Set the `KEY_ALEGRA` environment variable with your Alegra API authorization token:

```bash
export KEY_ALEGRA="Basic <your_base64_encoded_credentials>"
```

**For persistent setup**, add this to your shell profile (e.g., `~/.bashrc`, `~/.zshrc`):

```bash
echo 'export KEY_ALEGRA="Basic <your_base64_encoded_credentials>"' >> ~/.bashrc
source ~/.bashrc
```

**Format**: The token should be in the format `Basic <base64_encoded_credentials>`

**Note**: All scraping scripts now use the `KEY_ALEGRA` environment variable. The scripts will raise an error if this variable is not set.

## Usage

### Data Extraction

Run individual scraping scripts to extract data from Alegra API:

```bash
# Extract inventory/items data
python scraping/download_query_inventory.py

# Extract sales/invoice data
python scraping/download_query_sales.py

# Extract purchase orders
python scraping/download_query_purchases.py

# Extract warehouse movements
python scraping/download_query_movements.py

# Extract warehouse-specific inventory
python scraping/download_query_inventory_warehouse.py
```

Each script will:
1. Fetch data from the Alegra API using pagination
2. Process and normalize the data
3. Save results to CSV files incrementally (batch by batch)

**Output Files:**
- `items_inventory.csv` - Inventory data
- `factura_items.csv` - Sales/invoice data
- `purchase_orders.csv` - Purchase order data
- `warehouse_movements.csv` - Warehouse transfer data

### Upload to BigQuery

#### Single File Upload

```bash
python upload_csv_to_bigquery.py \
    --csv_file items_inventory.csv \
    --table_id project.dataset.table \
    --write_disposition WRITE_APPEND
```

#### Batch Upload

Upload all configured CSV files at once:

```bash
python batch_upload.py
```

The batch upload script uses the `TABLE_MAPPING` configuration in `config.py` to automatically map CSV files to BigQuery tables.

#### Additional Upload Options

```bash
# List tables in a dataset
python upload_csv_to_bigquery.py --list_tables optistock

# Get table information
python upload_csv_to_bigquery.py --table_info project.dataset.table
```

### Write Dispositions

- `WRITE_APPEND` - Append data to existing table (default)
- `WRITE_TRUNCATE` - Replace all data in the table
- `WRITE_EMPTY` - Only write if table is empty

## Project Structure

```
optistock_v2/
├── config.py                          # Configuration settings
├── batch_upload.py                    # Batch upload script
├── upload_csv_to_bigquery.py         # BigQuery upload utility
├── requirements.txt                   # Python dependencies
├── pyproject.toml                     # Poetry configuration
├── scraping/                          # Data extraction scripts
│   ├── download_query_inventory.py
│   ├── download_query_sales.py
│   ├── download_query_purchases.py
│   ├── download_query_movements.py
│   ├── download_query_inventory_warehouse.py
│   └── example_upload.py
├── queries/                           # SQL queries
│   └── rolling_inventory.sql
├── jsons_example/                     # Example JSON files
└── test_jsons/                        # Test data files
```

## BigQuery Tables

The following tables are created/updated in BigQuery:

| CSV File | BigQuery Table | Description |
|----------|---------------|-------------|
| `warehouse_movements.csv` | `warehouse_movements` | Warehouse transfer movements |
| `items_inventory.csv` | `inventory` | Current inventory levels |
| `purchase_orders.csv` | `purchases` | Purchase order data |
| `factura_items.csv` | `sales` | Sales/invoice data |

## SQL Queries

Example SQL queries for data analysis are available in the `queries/` directory:

- `rolling_inventory.sql` - Calculate rolling inventory over time

## Error Handling

The scripts include comprehensive error handling:
- API request failures are logged with context
- Missing fields are handled gracefully (set to None)
- BigQuery upload errors are caught and reported
- Batch processing continues even if individual items fail

## Logging

All scripts use Python's logging module with INFO level by default. Logs include:
- API request progress
- Data processing status
- Upload progress and results
- Error messages with context

## Troubleshooting

### Authentication Issues

- Verify your Google Cloud credentials are set up correctly
- Check that the service account has BigQuery permissions
- Ensure `PROJECT_ID` matches your GCP project

### API Issues

- Verify the `KEY_ALEGRA` environment variable is set correctly
- Check that the token format is correct: `Basic <base64_encoded_credentials>`
- Verify your Alegra API credentials are valid
- Check API rate limits if requests are failing
- Ensure network connectivity to Alegra API
- If you see "KEY_ALEGRA environment variable is not set", export the variable before running scripts

### Upload Issues

- Verify CSV files exist and are not empty
- Check BigQuery table permissions
- Ensure dataset exists or can be created
- Review error messages in the console output

## Development

### Running Tests

Test JSON files are available in `test_jsons/` for development and testing.

### Adding New Data Sources

1. Create a new script in `scraping/` following the pattern of existing scripts
2. Add the CSV-to-table mapping in `config.py` `TABLE_MAPPING`
3. Update this README with the new data source

## License

[Add your license information here]

## Author

[Add author information here]

## Contributing

[Add contributing guidelines if applicable]

