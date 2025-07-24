import time
import datetime
import os
import collections
import numpy as np
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Import functions from process.py ---
from process import process_swap_event
# --- Import BigQuery functions ---
from bq_utils import save_swap_events_to_bq_batch

# --- Kafka Configuration ---
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'alchemy_full_payloads'
# KAFKA_CONSUMER_GROUP = 'swap_event_consumer_group'

# --- Configuration ---
BQ_SAVE_INTERVAL_SECONDS = 60
BQ_BATCH_SIZE_THRESHOLD = 10
ROLLING_WINDOW_SIZE = 10
ALERT_TRACKER_RETENTION_PER_POOL = 100
Z_SCORE_THRESHOLD = 2.0
PRUNE_INTERVAL_SECONDS = 300

def alert_volume_spike(pool_address, z_score, latest_volume, block_number):
    print(f"ALERT! Volume spike in pool {pool_address} (Block: {block_number})! Z-score: {z_score:.2f}, Latest Vol: {latest_volume:.2f}")

def run_consumer():
    """
    Consumes swap events from Kafka, performs real-time analysis, 
    and saves data to BigQuery in batches.
    """
    print("Starting Swap Event consumer...")

    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER_URL],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )
            print(f"Kafka consumer subscribed to topic: {KAFKA_TOPIC}")
        except NoBrokersAvailable:
            print(f"CRITICAL: Could not connect to Kafka broker at {KAFKA_BROKER_URL}. Retrying in 5 seconds...")
            time.sleep(5)
        except ImportError:
            print("ERROR: kafka-python library is not installed. Please install it: pip install kafka-python")
            return # Exit if library is missing

    # --- Consumer State (local to this function) ---
    bq_data_cache = []
    pool_rolling_block_volumes = collections.defaultdict(lambda: collections.deque(maxlen=ROLLING_WINDOW_SIZE))
    last_activity_block_for_pool = collections.defaultdict(int)
    pool_processed_blocks_for_hot_alert = collections.defaultdict(set)
    last_upload_time = time.time()
    last_tracker_prune_timestamp = time.time()

    try:
        while True:
            for message in consumer:
                print(f"--- Received Kafka Message --- Offset: {message.offset}, Key: {message.key}")
                try:
                    kafka_message_data = json.loads(message.value.decode('utf-8'))
                except json.JSONDecodeError as e:
                    print(f"ERROR: Could not decode JSON from Kafka message: {e} - Data: {message.value}")
                    continue
                except Exception as e:
                    print(f"ERROR: Unexpected error decoding Kafka message: {e}")
                    continue
                
                # 1. Process swap event to get structured data
                parsed_rows = process_swap_event(kafka_message_data)
                if not parsed_rows:
                    print("DEBUG: No processable rows from swap event.")
                    continue

                current_message_aggregated_volumes = collections.defaultdict(lambda: collections.defaultdict(float))
                for row_data in parsed_rows:
                    bq_data_cache.append(row_data)
                    pool_address = row_data['pool_address']
                    try:
                        block_number_int = int(row_data['block'])
                        vol0 = float(row_data.get('amount0', "0"))
                        swap_volume = abs(vol0)
                        current_message_aggregated_volumes[pool_address][block_number_int] += swap_volume
                    except (ValueError, TypeError):
                        continue

                # 2. Update Rolling Volume Windows (with zero-filling) & Hot Pair Detection
                for pool_addr, blocks_data_in_msg in current_message_aggregated_volumes.items():
                    sorted_event_blocks_for_this_pool = sorted(blocks_data_in_msg.keys())

                    for current_event_block_num in sorted_event_blocks_for_this_pool:
                        actual_volume_for_event_block = blocks_data_in_msg[current_event_block_num]
                        last_recorded_block = last_activity_block_for_pool.get(pool_addr, current_event_block_num - 1)

                        block_to_fill_with_zero = last_recorded_block + 1
                        while block_to_fill_with_zero < current_event_block_num:
                            if block_to_fill_with_zero not in pool_processed_blocks_for_hot_alert[pool_addr]:
                                pool_rolling_block_volumes[pool_addr].append(0.0)
                                pool_processed_blocks_for_hot_alert[pool_addr].add(block_to_fill_with_zero)
                            last_activity_block_for_pool[pool_addr] = block_to_fill_with_zero
                            block_to_fill_with_zero += 1
                        
                        if current_event_block_num not in pool_processed_blocks_for_hot_alert[pool_addr]:
                            pool_rolling_block_volumes[pool_addr].append(actual_volume_for_event_block)
                            pool_processed_blocks_for_hot_alert[pool_addr].add(current_event_block_num)
                        
                        last_activity_block_for_pool[pool_addr] = current_event_block_num

                        # Hot Pair Detection (after updating the deque for this pool and block)
                        if len(pool_rolling_block_volumes[pool_addr]) == ROLLING_WINDOW_SIZE:
                            volumes = list(pool_rolling_block_volumes[pool_addr])
                            latest_vol = volumes[-1]
                            previous_vols = volumes[:-1]

                            if len(previous_vols) >= 2: # Need at least 2 for std dev
                                avg_vol = np.mean(previous_vols)
                                std_vol = np.std(previous_vols)

                                if std_vol > 0:
                                    z = (latest_vol - avg_vol) / std_vol
                                    if z > Z_SCORE_THRESHOLD:
                                        print(pool_rolling_block_volumes)
                                        alert_volume_spike(pool_addr, z, latest_vol, current_event_block_num)
                                elif avg_vol > 0 and latest_vol > avg_vol : # Handle case where std_dev is 0 but volume increased
                                    if latest_vol > avg_vol * Z_SCORE_THRESHOLD : # If it's a significant jump from constant
                                        alert_volume_spike(pool_addr, float('inf'), latest_vol, current_event_block_num)

                # --- RULE 1 (Size Trigger) ---
                if len(bq_data_cache) >= BQ_BATCH_SIZE_THRESHOLD:
                    print(f"INFO: Batch size threshold ({BQ_BATCH_SIZE_THRESHOLD}) reached. Flushing...")
                    data_to_save = list(bq_data_cache)
                    if flush_batches(data_to_save):
                        bq_data_cache.clear()
                        last_upload_time = time.time()
            
            # --- IDLE-TIME TASKS ---

            # --- RULE 2 (Time Trigger) ---
            if time.time() - last_upload_time >= BQ_SAVE_INTERVAL_SECONDS:
                print(f"INFO: Save interval ({BQ_SAVE_INTERVAL_SECONDS}s) reached. Flushing...")
                data_to_save = list(bq_data_cache)
                if flush_batches(data_to_save):
                    bq_data_cache.clear()
                    last_upload_time = time.time()
            
            # --- Other Periodic Tasks ---
            if time.time() - last_tracker_prune_timestamp >= PRUNE_INTERVAL_SECONDS:
                prune_alert_tracker(pool_processed_blocks_for_hot_alert, last_activity_block_for_pool)
                last_tracker_prune_timestamp = time.time()

    except KeyboardInterrupt:
        print("\nConsumer interrupted by user. Performing final flush...")
    except Exception as e:
        print(f"\nAn unexpected error occurred in the consumer: {e}")
        print("Attempting a final flush...")
    finally:
        if consumer:
            print("Closing Kafka consumer...")
            consumer.close()
        print("Consumer finished.")

def flush_batches(data_to_save):
    """Saves a batch of data to BigQuery and returns success status."""
    if not data_to_save:
        print("INFO: Flush triggered, but no new data in cache.")
        return True

    print(f"INFO: Saving {len(data_to_save)} records to BigQuery...")
    success = save_swap_events_to_bq_batch(data_to_save)
    if success:
        print(f"INFO: BigQuery save complete.")
    else:
        print(f"ERROR: BigQuery save failed. Data will be retried on next flush.")
    return success

def prune_alert_tracker(processed_blocks_tracker, last_activity_tracker):
    """Prunes old block entries from the alert tracker to manage memory."""
    print("INFO: Pruning pool_processed_blocks_for_hot_alert tracker...")
    pools_to_prune = list(processed_blocks_tracker.keys())
    pruned_count_total = 0
    for pool_addr in pools_to_prune:
        latest_block_for_this_pool = last_activity_tracker.get(pool_addr, 0)
        if latest_block_for_this_pool == 0:
            continue

        cutoff_block = latest_block_for_this_pool - ALERT_TRACKER_RETENTION_PER_POOL
        
        current_set = processed_blocks_tracker[pool_addr]
        blocks_to_remove_from_set = {b for b in current_set if b < cutoff_block}
        
        if blocks_to_remove_from_set:
            current_set.difference_update(blocks_to_remove_from_set)
            pruned_count_total += len(blocks_to_remove_from_set)
        
        if not current_set:
            del processed_blocks_tracker[pool_addr]

    if pruned_count_total > 0:
        print(f"INFO: Pruned {pruned_count_total} old block entries from alert tracker.")
    else:
        print("INFO: Alert tracker prune run, no entries were old enough to prune.")

if __name__ == "__main__":
    run_consumer()
