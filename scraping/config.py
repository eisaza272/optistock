# BigQuery CSV Upload Configuration

# Google Cloud Configuration
PROJECT_ID = "your-project-id"
DATASET_ID = "optistock"
LOCATION = "US"  # Dataset location

# Authentication
# Option 1: Use service account key file
CREDENTIALS_PATH = "path/to/your/service-account-key.json"

# Option 2: Use Application Default Credentials (ADC)
# Set CREDENTIALS_PATH = None and run: gcloud auth application-default login

# Default upload settings
DEFAULT_WRITE_DISPOSITION = "WRITE_APPEND"  # Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
DEFAULT_ENCODING = "UTF-8"
DEFAULT_DELIMITER = ","
DEFAULT_SKIP_ROWS = 1

# Table mapping (CSV filename -> BigQuery table name)
TABLE_MAPPING = {
    "warehouse_movements.csv": "warehouse_movements",
    "items_inventory.csv": "inventory", 
    "purchase_orders.csv": "purchases",
    "factura_items.csv": "sales"
}
