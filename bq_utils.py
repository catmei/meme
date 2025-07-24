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
SWAP_EVENTS_TABLE_ID = os.environ.get('BQ_SWAP_EVENTS_TABLE_ID', 'real_time_swaps')
REDDIT_POSTS_TABLE_ID = os.environ.get('BQ_REDDIT_POSTS_TABLE_ID', 'reddit_posts')
REDDIT_COMMENTS_TABLE_ID = os.environ.get('BQ_REDDIT_COMMENTS_TABLE_ID', 'reddit_comments')

# Google Cloud Authentication - Set credentials from .env
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
    logger.info(f"Using Google Cloud credentials from: {GOOGLE_APPLICATION_CREDENTIALS}")
else:
    logger.warning("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Define the BigQuery table schema for swap events
# Note: TIMESTAMP fields expect ISO format strings (YYYY-MM-DDTHH:MM:SS.sssZ)
SWAP_EVENTS_SCHEMA = [
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

# Define the BigQuery table schema for Reddit posts
REDDIT_POSTS_SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("selftext", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("score", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("upvote_ratio", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("num_comments", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_utc", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("permalink", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("link_flair_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("stickied", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("server_createdAt", "TIMESTAMP", mode="NULLABLE"),
]

# Define the BigQuery table schema for Reddit comments
REDDIT_COMMENTS_SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("post_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("parent_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("score", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("created_utc", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("is_submitter", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("server_createdAt", "TIMESTAMP", mode="NULLABLE"),
]


def ensure_table_exists(table_id, schema):
    """Ensure a BigQuery table exists with the correct schema."""
    table_ref = client.dataset(DATASET_ID).table(table_id)
    
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table {PROJECT_ID}.{DATASET_ID}.{table_id} already exists")
        # TODO: Add schema validation/update logic if needed
        return table
    except NotFound:
        logger.info(f"Table {PROJECT_ID}.{DATASET_ID}.{table_id} not found, creating...")
        
        # Create dataset if it doesn't exist
        dataset_ref = client.dataset(DATASET_ID)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            client.create_dataset(dataset)
            logger.info(f"Created dataset {PROJECT_ID}.{DATASET_ID}")
        
        # Create table
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        logger.info(f"Created table {PROJECT_ID}.{DATASET_ID}.{table_id}")
        return table

def convert_swap_event_to_bq_format(row_data):
    """Convert a swap event row dictionary to BigQuery format."""
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

def convert_reddit_post_to_bq_format(row_data):
    """Convert a Reddit post dictionary to BigQuery format."""
    try:
        created_utc = None
        if row_data.get('created_utc'):
            try:
                created_utc = datetime.fromtimestamp(float(row_data['created_utc']), tz=timezone.utc)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse created_utc for post: {row_data.get('id')}")

        server_created_at = datetime.now(timezone.utc)

        return {
            "id": str(row_data.get('id', '')),
            "title": str(row_data.get('title', '')),
            "selftext": str(row_data.get('selftext', '')),
            "score": int(row_data.get('score', 0)),
            "upvote_ratio": float(row_data.get('upvote_ratio', 0.0)),
            "num_comments": int(row_data.get('num_comments', 0)),
            "author": str(row_data.get('author', '')),
            "created_utc": created_utc.isoformat() if created_utc else None,
            "url": str(row_data.get('url', '')),
            "permalink": str(row_data.get('permalink', '')),
            "link_flair_text": str(row_data.get('link_flair_text', '')),
            "stickied": bool(row_data.get('stickied', False)),
            "server_createdAt": server_created_at.isoformat(),
        }
    except Exception as e:
        logger.error(f"Error converting Reddit post to BQ format: {e}, Row: {row_data}")
        return None

def convert_reddit_comment_to_bq_format(row_data):
    """Convert a Reddit comment dictionary to BigQuery format."""
    try:
        created_utc = None
        if row_data.get('created_utc'):
            try:
                created_utc = datetime.fromtimestamp(float(row_data['created_utc']), tz=timezone.utc)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse created_utc for comment: {row_data.get('id')}")
        
        server_created_at = datetime.now(timezone.utc)

        return {
            "id": str(row_data.get('id', '')),
            "post_id": str(row_data.get('post_id', '')),
            "parent_id": str(row_data.get('parent_id', '')),
            "author": str(row_data.get('author', '')),
            "body": str(row_data.get('body', '')),
            "score": int(row_data.get('score', 0)),
            "created_utc": created_utc.isoformat() if created_utc else None,
            "is_submitter": bool(row_data.get('is_submitter', False)),
            "server_createdAt": server_created_at.isoformat(),
        }
    except Exception as e:
        logger.error(f"Error converting Reddit comment to BQ format: {e}, Row: {row_data}")
        return None

def _save_to_bq_batch(rows, table_id, schema, conversion_func):
    """
    Generic function to save a batch of rows to a specified BigQuery table.

    Args:
        rows (list): A list of dictionaries, where each dictionary represents a row.
        table_id (str): The ID of the BigQuery table to insert rows into.
        schema (list): The schema of the BigQuery table.
        conversion_func (function): The function to convert a raw dictionary to a BQ-compatible format.
    """
    if not rows:
        logger.info(f"No rows to save to BigQuery for table {table_id}")
        return True

    try:
        table = ensure_table_exists(table_id, schema)
        
        bq_rows = [conversion_func(row) for row in rows if row]
        bq_rows = [row for row in bq_rows if row]  # Filter out None results from conversion errors

        if not bq_rows:
            logger.warning(f"No valid rows to insert into {table_id} after conversion")
            return True
        
        errors = client.insert_rows_json(table, bq_rows)
        
        if errors:
            logger.error(f"BigQuery insert errors for table {table_id}: {errors}")
            return False
        else:
            logger.info(f"Successfully inserted {len(bq_rows)} rows into BigQuery table {table_id}")
            return True
            
    except Exception as e:
        logger.error(f"Error saving to BigQuery table {table_id}: {e}")
        return False

def save_swap_events_to_bq_batch(event_rows):
    """Save multiple swap events to BigQuery in a batch operation."""
    return _save_to_bq_batch(event_rows, SWAP_EVENTS_TABLE_ID, SWAP_EVENTS_SCHEMA, convert_swap_event_to_bq_format)

def save_reddit_posts_to_bq_batch(post_rows):
    """Save multiple Reddit posts to BigQuery in a batch operation."""
    return _save_to_bq_batch(post_rows, REDDIT_POSTS_TABLE_ID, REDDIT_POSTS_SCHEMA, convert_reddit_post_to_bq_format)

def save_reddit_comments_to_bq_batch(comment_rows):
    """Save multiple Reddit comments to BigQuery in a batch operation."""
    return _save_to_bq_batch(comment_rows, REDDIT_COMMENTS_TABLE_ID, REDDIT_COMMENTS_SCHEMA, convert_reddit_comment_to_bq_format)


def test_connection():
    """Test BigQuery connection and table access."""
    try:
        # Test connection
        datasets = list(client.list_datasets())
        logger.info(f"Successfully connected to BigQuery. Found {len(datasets)} datasets.")
        
        # Test table access for all tables
        ensure_table_exists(SWAP_EVENTS_TABLE_ID, SWAP_EVENTS_SCHEMA)
        logger.info(f"Swap events table access confirmed.")
        
        ensure_table_exists(REDDIT_POSTS_TABLE_ID, REDDIT_POSTS_SCHEMA)
        logger.info(f"Reddit posts table access confirmed.")

        ensure_table_exists(REDDIT_COMMENTS_TABLE_ID, REDDIT_COMMENTS_SCHEMA)
        logger.info(f"Reddit comments table access confirmed.")

        return True
    except Exception as e:
        logger.error(f"BigQuery connection test failed: {e}")
        return False

def test_convert_swap_event_to_bq_format():
    """Test the convert_swap_event_to_bq_format function with sample data."""
    print("\n=== Testing Swap Event Conversion ===")
    
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
    print(f"\nInput Swap Event: {test_row}")
    converted_swap_event = convert_swap_event_to_bq_format(test_row)
    print(f"Converted Swap Event: {converted_swap_event}")

def test_convert_reddit_post_to_bq_format():
    """Test the conversion function for Reddit posts."""
    print("\n=== Testing Reddit Post Conversion ===")

    # Test Post Conversion
    post_row = {
        'id': '1m242hx', 
        'title': 'Looking back at everyone who was in doubt', 
        'selftext': '', 
        'score': 160, 
        'upvote_ratio': 0.95, 
        'num_comments': 90, 
        'author': 'bazooka_star', 
        'created_utc': 1752748418.0, 
        'url': 'https://i.redd.it/4o3ryzoxwedf1.jpeg', 
        'permalink': '/r/ethtrader/comments/1m242hx/looking_back_at_everyone_who_was_in_doubt/', 
        'link_flair_text': 'Meme', 
        'stickied': False
    }
    print(f"\nInput Post: {post_row}")
    converted_post = convert_reddit_post_to_bq_format(post_row)
    print(f"Converted Post: {converted_post}")

def test_convert_reddit_comment_to_bq_format():
    """Test the conversion function for Reddit comments."""
    print("\n=== Testing Reddit Comment Conversion ===")

    # Test Comment Conversion
    comment_row = {
        'id': 'n4rjk7o', 
        'post_id': '1m7d4vj', 
        'parent_id': 't3_1m7d4vj', 
        'author': 'Thorp1', 
        'body': 'Gotta protect yourself u know\n\n!tip 1', 
        'score': 1, 
        'created_utc': 1753297319.0, 
        'is_submitter': False
    }
    print(f"\nInput Comment: {comment_row}")
    converted_comment = convert_reddit_comment_to_bq_format(comment_row)
    print(f"Converted Comment: {converted_comment}")


if __name__ == "__main__":
    # Test the connection when run directly
    test_connection()
    
    # Test the conversion functions
    test_convert_swap_event_to_bq_format()
    test_convert_reddit_post_to_bq_format()
    test_convert_reddit_comment_to_bq_format() 
    