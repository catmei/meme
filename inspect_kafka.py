from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import os

# Kafka Broker URL - uses environment variable or defaults to localhost:9092
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'localhost:9092')

print(f"Attempting to connect to Kafka AdminClient at: {KAFKA_BROKER_URL}")

admin_client = None
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=[KAFKA_BROKER_URL],
        # client_id='my-admin-client', # Optional client ID
        # request_timeout_ms=5000 # Optional: time to wait for responses from broker
    )
    print("KafkaAdminClient object created.")
    
    print("\nFetching list of topics...")
    topics = admin_client.list_topics()
    
    if topics:
        print("\nAvailable topics:")
        for topic in topics:
            print(f"- {topic}")
            # Try to fetch and print some data from each topic
            try:
                from kafka import KafkaConsumer
                
                # First, count all messages in the topic
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=[KAFKA_BROKER_URL],
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=5000,  # Longer timeout for counting
                    group_id=None
                )
                total_msg_count = 0
                first_message = None
                for message in consumer:
                    if total_msg_count == 0:
                        first_message = message  # Store the first message
                    total_msg_count += 1
                consumer.close()
                
                print(f"  Total messages: {total_msg_count}")
                if first_message:
                    import json
                    first_message_value = json.loads(first_message.value)
                    print(f"  First message (Offset {first_message.offset}):")
                    print(json.dumps(first_message_value, indent=4))
                else:
                    print("    (No messages found in this topic.)")
                    
            except Exception as e:
                print(f"    Error reading messages from topic '{topic}': {e}")
    else:
        print("No topics found in the cluster (or client couldn't retrieve them).")

except NoBrokersAvailable:
    print(f"CRITICAL: NoBrokersAvailable. Could not connect to any Kafka brokers at {KAFKA_BROKER_URL}.")
    print("Please ensure Kafka is running and accessible, and the KAFKA_BROKER_URL is correct.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if admin_client:
        print("\nClosing AdminClient connection.")
        admin_client.close()
