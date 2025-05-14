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
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=[KAFKA_BROKER_URL],
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=2000,  # Short timeout for demo
                    group_id=None
                )
                print(f"  Sample messages from topic '{topic}':")
                msg_count = 0
                for message in consumer:
                    print(f"    Offset {message.offset}: {message.value}")
                    msg_count += 1
                    if msg_count >= 3:
                        break
                if msg_count == 0:
                    print("    (No messages found in this topic.)")
                consumer.close()
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
