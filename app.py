import json
import os
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Kafka Producer Initialization
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC_RAW_JSON_PAYLOADS = os.environ.get('KAFKA_TOPIC_RAW_JSON_PAYLOADS', 'alchemy_full_payloads') # New topic name
kafka_producer = None

try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serializes the whole JSON dict to string
        acks='all',
        retries=1 
    )
    print(f"KafkaProducer object created. Topic: {KAFKA_TOPIC_RAW_JSON_PAYLOADS}")
except Exception as e:
    print(f"CRITICAL: Error initializing KafkaProducer: {e}. Webhook will fail.")

@app.route("/webhook", methods=["POST"])
def webhook():
    if not kafka_producer:
        print("ERROR: Kafka producer is not available. Cannot process webhook.")
        return jsonify({"status": "error", "message": "Kafka producer not available"}), 503

    # Get the entire JSON payload from the request
    data = request.json

    if not data:
        print("Webhook received empty payload.")
        return jsonify({"status": "empty_payload"}), 200

    try:
        print(f"Payload to send: {json.dumps(data, indent=2)}")
        kafka_producer.send(KAFKA_TOPIC_RAW_JSON_PAYLOADS, value=data)
        kafka_producer.flush() 
        print(f"Successfully sent entire payload to Kafka.")
        return jsonify({"status": "success", "message": "Entire payload sent to Kafka"}), 200
    except Exception as e:
        print(f"ERROR sending payload to Kafka: {e}")
        # Optionally, log the data that failed if it's not too large or sensitive for console
        # print(f"Failed payload: {json.dumps(data, indent=2)}") 
        return jsonify({"status": "error", "message": "Failed to send payload to Kafka"}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print(f"Flask app (super-simple mode) starting. Port: {port}")
    app.run(host='0.0.0.0', port=port, debug=True) 