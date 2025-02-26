import time
import logging
import argparse
from confluent_kafka import Producer
import json
import sys
import signal

# Add the parent directory to the path so we can import the generator module
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generator.transaction_generator import TransactionGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
DEFAULT_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'payment-producer'
}

# Topic names
TRANSACTION_TOPIC = 'payment-transactions'
FRAUD_ALERT_TOPIC = 'fraud-alerts'

# Flag for graceful shutdown
running = True

def delivery_callback(err, msg):
    """Callback function to track message delivery."""
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        logger.debug(f'Message delivered to {topic}[{partition}] at offset {offset}')

def shutdown_handler(signum, frame):
    """Handle shutdown signals to gracefully stop the producer."""
    global running
    logger.info("Shutdown signal received, stopping producer...")
    running = False

def produce_transactions(config, rate=10, fraud_probability=0.02, max_count=None):
    """
    Continuously produce transaction messages to Kafka topics.
    
    Args:
        config: Kafka producer configuration
        rate: Number of transactions per second
        fraud_probability: Probability of generating a fraudulent transaction
        max_count: Maximum number of transactions to send (None for unlimited)
    """
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Create Kafka producer
    producer = Producer(config)
    
    # Create transaction generator
    generator = TransactionGenerator(fraud_probability=fraud_probability)
    
    # Initialize counters
    count = 0
    fraud_count = 0
    start_time = time.time()
    
    logger.info(f"Starting transaction producer with rate: {rate}/sec, fraud probability: {fraud_probability}")
    
    # Main production loop
    try:
        while running and (max_count is None or count < max_count):
            # Generate a transaction
            transaction = generator.generate_transaction()
            transaction_json = generator.to_json(transaction)
            
            # Send to transaction topic
            producer.produce(
                TRANSACTION_TOPIC,
                key=transaction.transaction_id,
                value=transaction_json,
                callback=delivery_callback
            )
            
            # If it's fraudulent, also send to fraud alert topic
            if transaction.is_fraud:
                producer.produce(
                    FRAUD_ALERT_TOPIC,
                    key=transaction.transaction_id,
                    value=transaction_json,
                    callback=delivery_callback
                )
                fraud_count += 1
                
            # Update counters
            count += 1
            
            # Flush every 100 messages
            if count % 100 == 0:
                producer.flush()
                elapsed = time.time() - start_time
                tx_rate = count / elapsed
                fraud_rate = fraud_count / count * 100
                logger.info(f"Sent {count} transactions ({tx_rate:.2f}/sec), {fraud_count} fraudulent ({fraud_rate:.2f}%)")
            
            # Control rate
            time.sleep(1.0 / rate)
            
    except Exception as e:
        logger.error(f"Error in producer: {e}")
    finally:
        # Make sure all messages are delivered before exiting
        logger.info("Flushing remaining messages...")
        producer.flush()
        
        # Log final statistics
        elapsed = time.time() - start_time
        tx_rate = count / elapsed if elapsed > 0 else 0
        fraud_rate = fraud_count / count * 100 if count > 0 else 0
        logger.info(f"Finished: Sent {count} transactions ({tx_rate:.2f}/sec), {fraud_count} fraudulent ({fraud_rate:.2f}%)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Transaction Producer')
    parser.add_argument('--rate', type=int, default=10, 
                        help='Transactions per second')
    parser.add_argument('--fraud-probability', type=float, default=0.02, 
                        help='Probability of generating a fraudulent transaction')
    parser.add_argument('--max-count', type=int, default=None, 
                        help='Maximum number of transactions to send (default: unlimited)')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', 
                        help='Kafka bootstrap servers')
    args = parser.parse_args()
    
    # Update Kafka configuration
    config = DEFAULT_CONFIG.copy()
    config['bootstrap.servers'] = args.bootstrap_servers
    
    produce_transactions(
        config=config,
        rate=args.rate,
        fraud_probability=args.fraud_probability,
        max_count=args.max_count
    )