import os
import time
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery Configuration
PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'your-project-id')
DATASET_ID = os.environ.get('BQ_DATASET_ID', 'swap_events')
TABLE_ID = os.environ.get('BQ_TABLE_ID', 'real_time_swaps')

# Google Cloud Authentication - Set credentials from .env
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
    logger.info(f"Using Google Cloud credentials from: {GOOGLE_APPLICATION_CREDENTIALS}")
else:
    logger.warning("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Define the BigQuery table schema
# Note: TIMESTAMP fields expect ISO format strings (YYYY-MM-DDTHH:MM:SS.sssZ)
SCHEMA = [
    bigquery.SchemaField("block", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("amount0", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("amount1", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("sqrtPriceX96", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pool_address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("log_index", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("tx_hash", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("network", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("alchemy_createdAt", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("server_createdAt", "TIMESTAMP", mode="NULLABLE"),
]

def ensure_table_exists():
    """Ensure the BigQuery table exists with the correct schema."""
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} already exists")
        return table
    except NotFound:
        logger.info(f"Table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} not found, creating...")
        
        # Create dataset if it doesn't exist
        dataset_ref = client.dataset(DATASET_ID)
        try:
            dataset = client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = client.create_dataset(dataset)
            logger.info(f"Created dataset {PROJECT_ID}.{DATASET_ID}")
        
        # Create table
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = client.create_table(table)
        logger.info(f"Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        return table

def convert_row_to_bq_format(row_data):
    """Convert a row dictionary to BigQuery format."""
    try:
        # Convert Unix timestamp to datetime object
        timestamp_val = None
        if row_data.get('timestamp'):
            try:
                # Convert Unix timestamp (like 1751065247) to UTC datetime
                timestamp_val = datetime.fromtimestamp(float(row_data['timestamp']), tz=timezone.utc)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse timestamp: {row_data.get('timestamp')}")
        
        alchemy_created_at = None
        if row_data.get('alchemy_createdAt'):
            try:
                alchemy_created_at = datetime.fromisoformat(row_data['alchemy_createdAt'].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                logger.warning(f"Could not parse alchemy_createdAt: {row_data.get('alchemy_createdAt')}")
        
        server_created_at = None
        if row_data.get('server_createdAt'):
            try:
                server_created_at = datetime.fromisoformat(row_data['server_createdAt'].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                logger.warning(f"Could not parse server_createdAt: {row_data.get('server_createdAt')}")
        
        # Convert to BigQuery row format
        # Convert datetime objects to ISO format strings for JSON serialization
        bq_row = {
            "block": int(row_data.get('block', 0)),
            "timestamp": timestamp_val.isoformat() if timestamp_val else None,
            "amount0": float(row_data.get('amount0', 0)) if row_data.get('amount0') else None,
            "amount1": float(row_data.get('amount1', 0)) if row_data.get('amount1') else None,
            "sqrtPriceX96": str(row_data.get('sqrtPriceX96', '')),
            "pool_address": str(row_data.get('pool_address', '')),
            "log_index": int(row_data.get('log_index', 0)) if row_data.get('log_index') else None,
            "tx_hash": str(row_data.get('tx_hash', '')),
            "network": str(row_data.get('network', '')),
            "alchemy_createdAt": alchemy_created_at.isoformat() if alchemy_created_at else None,
            "server_createdAt": server_created_at.isoformat() if server_created_at else None,
        }
        
        return bq_row
    except Exception as e:
        logger.error(f"Error converting row to BQ format: {e}, Row: {row_data}")
        return None

def save_events_to_bq_batch(event_rows):
    """Save multiple events to BigQuery in a batch operation."""
    if not event_rows:
        logger.info("No rows to save to BigQuery")
        return True
    
    try:
        # Ensure table exists
        table = ensure_table_exists()
        
        # Convert all rows to BigQuery format
        bq_rows = []
        for row_data in event_rows:
            bq_row = convert_row_to_bq_format(row_data)
            if bq_row:
                bq_rows.append(bq_row)
        
        if not bq_rows:
            logger.warning("No valid rows to insert after conversion")
            return True
        
        # Insert rows
        errors = client.insert_rows_json(table, bq_rows)
        
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            return False
        else:
            logger.info(f"Successfully inserted {len(bq_rows)} rows into BigQuery")
            return True
            
    except Exception as e:
        logger.error(f"Error saving to BigQuery: {e}")
        return False



def test_connection():
    """Test BigQuery connection and table access."""
    try:
        # Test connection
        datasets = list(client.list_datasets())
        logger.info(f"Successfully connected to BigQuery. Found {len(datasets)} datasets.")
        
        # Test table access
        table = ensure_table_exists()
        logger.info(f"Table access confirmed: {table.full_table_id}")
        
        return True
    except Exception as e:
        logger.error(f"BigQuery connection test failed: {e}")
        return False

def test_convert_row_to_bq_format():
    """Test the convert_row_to_bq_format function with sample data."""
    print("\n=== Testing convert_row_to_bq_format ===")
    
    # Test data from actual processed swap event
    test_row = {
        'block': 22486139, 
        'timestamp': 1747283291, 
        'amount0': '-85.206687', 
        'amount1': '0.032934854495497892', 
        'sqrtPriceX96': 1557263334585053979682119080434449, 
        'pool_address': '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640', 
        'log_index': 397, 
        'tx_hash': '0xfac2c92365bf282c819666024f69ec200f2f8a24e04ca89e40409cd158acf0f1', 
        'network': 'ETH_MAINNET', 
        'alchemy_createdAt': '2025-05-15T04:28:16.043Z', 
        'server_createdAt': '2025-06-27T23:32:39.086116+00:00'
    }
    
    print(f"Input data: {test_row}")
    
    # Test the conversion
    result = convert_row_to_bq_format(test_row)
    print(f"Result: {result}")

if __name__ == "__main__":
    # Test the connection when run directly
    test_connection()
    
    # Test the conversion function
    test_convert_row_to_bq_format() 
    