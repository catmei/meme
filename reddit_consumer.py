import os
import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from bq_utils import (
    save_reddit_posts_to_bq_batch, 
    save_reddit_comments_to_bq_batch, 
    test_connection
)
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Configuration ---
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPICS = ['reddit_posts', 'reddit_comments']
# CONSUMER_GROUP_ID = 'reddit-bq-consumer-group'

# Batching Configuration
BQ_BATCH_SIZE_THRESHOLD = 100
BQ_SAVE_INTERVAL_SECONDS = 60

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Preprocessing Placeholders ---

def preprocess_post_data(post_data):
    """
    Placeholder function for any preprocessing on Reddit post data.
    Currently, it returns the data as-is.
    """
    # Example: Add a new field
    # post_data['processed_at'] = time.time()
    return post_data

def preprocess_comment_data(comment_data):
    """
    Placeholder function for any preprocessing on Reddit comment data.
    Currently, it returns the data as-is.
    """
    return comment_data

# --- Main Consumer Logic ---

def run_consumer():
    """
    Consumes Reddit data from Kafka, processes it in batches, 
    and saves it to BigQuery.
    """
    logger.info("Starting Reddit BigQuery consumer...")

    # 1. Test BigQuery Connection
    if not test_connection():
        logger.error("BigQuery connection test failed. Exiting.")
        return
    logger.info("BigQuery connection successful.")

    # 2. Initialize Kafka Consumer
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=[KAFKA_BROKER_URL],
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Let consumer handle commits for simplicity here
                consumer_timeout_ms=1000,
                # group_id=CONSUMER_GROUP_ID,
            )
            logger.info(f"Subscribed to Kafka topics: {KAFKA_TOPICS}")
        except NoBrokersAvailable:
            logger.error(f"Could not connect to Kafka broker at {KAFKA_BROKER_URL}. Retrying in 5 seconds...")
            time.sleep(5)
    
    # 3. Message Processing Loop
    posts_batch = []
    comments_batch = []
    last_upload_time = time.time()

    try:
        while True:
            for message in consumer:
                topic = message.topic
                try:
                    record = json.loads(message.value.decode('utf-8'))
                    
                    if topic == 'reddit_posts':
                        processed_record = preprocess_post_data(record)
                        posts_batch.append(processed_record)
                    elif topic == 'reddit_comments':
                        processed_record = preprocess_comment_data(record)
                        comments_batch.append(processed_record)

                    # --- RULE 1 (Safety Valve & Efficiency) ---
                    # Flush if the batch gets too big, regardless of time.
                    # This protects memory during traffic spikes and is efficient.
                    if len(posts_batch) >= BQ_BATCH_SIZE_THRESHOLD or \
                       len(comments_batch) >= BQ_BATCH_SIZE_THRESHOLD:
                        
                        logger.info("Batch size limit reached. Flushing...")
                        flush_batches(posts_batch, comments_batch)
                        posts_batch.clear()
                        comments_batch.clear()
                        last_upload_time = time.time() # Reset timer

                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON from topic {topic}: {message.value}")
                except Exception as e:
                    logger.error(f"An error occurred processing message from {topic}: {e}")
            
            # --- RULE 2 (Latency Guarantee) ---
            # This code is reached on inactivity (for loop timeout).
            # Flush whatever is left if our time limit is up.
            if time.time() - last_upload_time > BQ_SAVE_INTERVAL_SECONDS:
                logger.info("Batch save interval reached. Flushing...")
                flush_batches(posts_batch, comments_batch)
                posts_batch.clear()
                comments_batch.clear()
                last_upload_time = time.time() # Reset timer

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down...")
    finally:
        logger.info("Flushing final batches before exit...")
        if consumer:
            consumer.close()
        logger.info("Reddit consumer closed.")

def flush_batches(posts, comments):
    """Flushes the collected batches to their respective BigQuery tables."""
    if posts:
        logger.info(f"Uploading {len(posts)} posts to BigQuery...")
        success = save_reddit_posts_to_bq_batch(posts)
        if not success:
            logger.error("Failed to upload posts batch. Data will be lost in this simple consumer.")
            # For a more robust system, consider a dead-letter queue or retries.
    
    if comments:
        logger.info(f"Uploading {len(comments)} comments to BigQuery...")
        success = save_reddit_comments_to_bq_batch(comments)
        if not success:
            logger.error("Failed to upload comments batch. Data will be lost.")

if __name__ == "__main__":
    run_consumer()   