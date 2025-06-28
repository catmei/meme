import time
import datetime
import os
import collections
import numpy as np # For z-score calculation
import json # For parsing Kafka messages

# --- Import functions from process.py ---
from process import process_swap_event
# --- Import BigQuery functions ---
from bq_utils import save_events_to_bq_batch

# --- Kafka Configuration ---
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'alchemy_full_payloads' # Target Kafka topic
# KAFKA_CONSUMER_GROUP = 'swap_event_consumer_group' # Optional: define a consumer group

# --- Configuration ---
BQ_SAVE_INTERVAL_SECONDS = 60
BQ_BATCH_SIZE_THRESHOLD = 10
ROLLING_WINDOW_SIZE = 10
ALERT_TRACKER_RETENTION_PER_POOL = 100  # Keep track of the last 100 blocks per pool for alerting
Z_SCORE_THRESHOLD = 2.0

# --- Timers and Counters ---
last_bq_save_timestamp = time.time()
last_tracker_prune_timestamp = time.time()
PRUNE_INTERVAL_SECONDS = 300 # Prune every 5 minutes, for example

# --- Consumer Data Structures ---
master_bq_data_cache = []
pool_rolling_block_volumes = collections.defaultdict(lambda: collections.deque(maxlen=ROLLING_WINDOW_SIZE))
last_activity_block_for_pool = collections.defaultdict(int) # Stores the latest block number processed (real or zero-filled) for a pool
pool_processed_blocks_for_hot_alert = collections.defaultdict(set) # Tracks (pool, block_num) for alerting window to prevent duplicates

def alert_volume_spike(pool_address, z_score, latest_volume, block_number):
    print(f"ALERT! Volume spike in pool {pool_address} (Block: {block_number})! Z-score: {z_score:.2f}, Latest Vol: {latest_volume:.2f}")

def run_consumer():
    global last_bq_save_timestamp, master_bq_data_cache, last_tracker_prune_timestamp

    print("Starting Kafka consumer...")
    print(f"Attempting to connect to Kafka broker at: {KAFKA_BROKER_URL}, topic: {KAFKA_TOPIC}")

    consumer = None
    try:
        from kafka import KafkaConsumer # Moved import here to avoid error if kafka-python is not installed
        from kafka.errors import NoBrokersAvailable

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER_URL],
            auto_offset_reset='earliest', # Start reading at the earliest message if no offset is stored
            enable_auto_commit=False,    # We will manually commit or rely on shutdown handling
            # group_id=KAFKA_CONSUMER_GROUP, # Uncomment if using a consumer group
            # consumer_timeout_ms=5000,    # Timeout in ms to allow loop to break and check other conditions
            # value_deserializer=lambda m: json.loads(m.decode('utf-8')) # Optional: deserialize directly
        )
        print("Kafka consumer created successfully. Waiting for messages...")

        for message in consumer:
            print(f"--- Received Kafka Message --- Offset: {message.offset}, Key: {message.key}")
            try:
                kafka_message_data = json.loads(message.value.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"ERROR: Could not decode JSON from Kafka message: {e} - Data: {message.value}")
                continue # Skip this message
            except Exception as e:
                print(f"ERROR: Unexpected error decoding Kafka message: {e} - Data: {message.value}")
                continue
            
            # 1. Process swap event to get structured data
            parsed_rows = process_swap_event(kafka_message_data)
            if not parsed_rows:
                print("DEBUG: No processable rows from swap event.")
                # Still check for timed save even if no new rows
                check_and_save_bq()
                check_and_prune_tracker()
                continue

            # Temporary aggregation for current message (in case one message has multiple swaps for same pool/block)
            current_message_aggregated_volumes = collections.defaultdict(lambda: collections.defaultdict(float))

            for row_data in parsed_rows:
                master_bq_data_cache.append(row_data) # Add to main BigQuery cache

                pool_address = row_data['pool_address']
                # Ensure block is an integer for logic
                try:
                    block_number_int = int(row_data['block'])
                except ValueError:
                    print(f"WARN: Could not parse block number to int: {row_data['block']} for pool {pool_address}")
                    continue
                
                try:
                    # Calculate volume (sum of absolute amounts for simplicity)
                    vol0 = float(row_data.get('amount0', "0"))
                    swap_volume = abs(vol0)
                    current_message_aggregated_volumes[pool_address][block_number_int] += swap_volume
                except ValueError:
                    print(f"WARN: Could not parse amount0/amount1 to float for pool {pool_address}, block {block_number_int}")
                    continue


            # 2. Update Rolling Volume Windows (with zero-filling) & Hot Pair Detection
            for pool_addr, blocks_data_in_msg in current_message_aggregated_volumes.items():
                sorted_event_blocks_for_this_pool = sorted(blocks_data_in_msg.keys())

                for current_event_block_num in sorted_event_blocks_for_this_pool:
                    actual_volume_for_event_block = blocks_data_in_msg[current_event_block_num]
                    
                    last_recorded_block = last_activity_block_for_pool.get(pool_addr, current_event_block_num - 1)

                    # Zero-Filling Loop
                    block_to_fill_with_zero = last_recorded_block + 1
                    while block_to_fill_with_zero < current_event_block_num:
                        if block_to_fill_with_zero not in pool_processed_blocks_for_hot_alert[pool_addr]:
                            print(f"DEBUG: Filling Pool {pool_addr}, Block {block_to_fill_with_zero} with 0 volume.")
                            pool_rolling_block_volumes[pool_addr].append(0.0)
                            pool_processed_blocks_for_hot_alert[pool_addr].add(block_to_fill_with_zero)
                        last_activity_block_for_pool[pool_addr] = block_to_fill_with_zero
                        block_to_fill_with_zero += 1
                    
                    # Process Actual Event Volume
                    if current_event_block_num not in pool_processed_blocks_for_hot_alert[pool_addr]:
                        print(f"DEBUG: Adding Pool {pool_addr}, Block {current_event_block_num} with volume {actual_volume_for_event_block:.2f}.")
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


            # 3. Batch Save to BigQuery (Timed)
            check_and_save_bq()

            # 4. Prune Alert Tracker
            check_and_prune_tracker()
            

        # Final save on shutdown
        print("Consumer loop finished or timed out. Performing final BigQuery save...")
        perform_bq_save()
        

    except NoBrokersAvailable:
        print(f"CRITICAL: NoBrokersAvailable. Could not connect to any Kafka brokers at {KAFKA_BROKER_URL}.")
        print("Please ensure Kafka is running and accessible, and the KAFKA_BROKER_URL is correct.")
    except ImportError:
        print("ERROR: kafka-python library is not installed. Please install it: pip install kafka-python")
    except Exception as e:
        print(f"An unexpected error occurred in the consumer: {e}")
    finally:
        if consumer:
            print("Closing Kafka consumer...")
            consumer.close()
        print("Consumer finished.")

def check_and_save_bq():
    global last_bq_save_timestamp, master_bq_data_cache
    current_time = time.time()

    # Check both conditions
    time_trigger = (current_time - last_bq_save_timestamp) >= BQ_SAVE_INTERVAL_SECONDS
    size_trigger = len(master_bq_data_cache) >= BQ_BATCH_SIZE_THRESHOLD

    if time_trigger or size_trigger:
        perform_bq_save()
        last_bq_save_timestamp = current_time

def perform_bq_save():
    global master_bq_data_cache
    if master_bq_data_cache:
        print(f"INFO: Saving {len(master_bq_data_cache)} records to BigQuery...")
        data_to_save = list(master_bq_data_cache) # Shallow copy
        master_bq_data_cache.clear()
        
        # Use batch insert for better performance
        success = save_events_to_bq_batch(data_to_save)
        if success:
            print(f"INFO: BigQuery save complete. Cache cleared.")
        else:
            print(f"ERROR: BigQuery save failed. Re-adding {len(data_to_save)} records to cache.")
            # Re-add data to cache if save failed
            master_bq_data_cache.extend(data_to_save)
        print(f"INFO: BigQuery save complete. Cache cleared.")
    else:
        print("INFO: BigQuery save interval reached, but no new data in cache.")

def check_and_prune_tracker():
    global last_tracker_prune_timestamp
    current_time = time.time()
    if current_time - last_tracker_prune_timestamp >= PRUNE_INTERVAL_SECONDS:
        prune_alert_tracker()
        last_tracker_prune_timestamp = current_time

def prune_alert_tracker():
    print("INFO: Pruning pool_processed_blocks_for_hot_alert tracker...")
    pools_to_prune = list(pool_processed_blocks_for_hot_alert.keys()) # Iterate over copy of keys
    pruned_count_total = 0
    for pool_addr in pools_to_prune:
        latest_block_for_this_pool = last_activity_block_for_pool.get(pool_addr, 0)
        if latest_block_for_this_pool == 0:
            continue

        cutoff_block = latest_block_for_this_pool - ALERT_TRACKER_RETENTION_PER_POOL
        
        current_set = pool_processed_blocks_for_hot_alert[pool_addr]
        blocks_to_remove_from_set = {b for b in current_set if b < cutoff_block}
        
        if blocks_to_remove_from_set:
            current_set.difference_update(blocks_to_remove_from_set)
            pruned_count_total += len(blocks_to_remove_from_set)
            # print(f"DEBUG: Pruned {len(blocks_to_remove_from_set)} entries for pool {pool_addr}. Kept {len(current_set)}.")
        
        if not current_set: # If set becomes empty
            del pool_processed_blocks_for_hot_alert[pool_addr]
            # print(f"DEBUG: Removed empty tracker set for pool {pool_addr}")
            # We can also remove from last_activity_block_for_pool if it's old and pool is inactive for long
            # but for simplicity, let's keep last_activity_block_for_pool entries.

    if pruned_count_total > 0:
        print(f"INFO: Pruned {pruned_count_total} old block entries from alert tracker across all pools.")
    else:
        print("INFO: Alert tracker prune run, no entries were old enough to prune.")


if __name__ == "__main__":
    run_consumer()
