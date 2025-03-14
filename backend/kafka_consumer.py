import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import time
import signal
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="fraud_detection.log",
    filemode="a"
)

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "transactions"
GROUP_ID = "fraud_detection_group"

# Flag for controlling the main loop
running = True

# Handle shutdown signals
def handle_shutdown(signum, frame):
    global running
    logging.info("Shutdown signal received, closing consumer...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Create Kafka consumer
def create_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            session_timeout_ms=30000,
            request_timeout_ms=40000
        )
        logging.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
        return consumer
    except KafkaError as e:
        logging.error(f"Failed to create consumer: {e}")
        return None

# Process a transaction
def process_transaction(transaction):
    # Skip invalid transactions
    if not isinstance(transaction, dict):
        logging.warning(f"Invalid transaction format: {transaction}")
        return
    
    # Log the transaction
    transaction_id = transaction.get("transaction_id", "unknown")
    logging.info(f"✅ Received: {transaction}")
    
    # Check for fraud
    if transaction.get("is_fraud"):
        logging.warning(f"🚨 FRAUD ALERT! 🚨 {transaction}")

# Main function
def main():
    logging.info("Starting fraud detection service")
    
    # Initial connection
    consumer = create_consumer()
    if not consumer:
        logging.error("Failed to connect to Kafka. Exiting.")
        return
    
    logging.info("🔄 Listening for transactions...")
    
    # Main processing loop
    try:
        while running:
            # Get messages with timeout to check running flag periodically
            messages = consumer.poll(timeout_ms=1000)
            
            for _, records in messages.items():
                for record in records:
                    if not running:
                        break
                    
                    try:
                        # Process the message
                        if record.value is not None:
                            process_transaction(record.value)
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
    
    except KafkaError as e:
        logging.error(f"Kafka error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Clean shutdown
        if consumer:
            logging.info("Closing Kafka consumer...")
            consumer.close()
            
    logging.info("Fraud detection service stopped")

if __name__ == "__main__":
    main()