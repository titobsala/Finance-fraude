import json
import logging
import argparse
import signal
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default Kafka configuration
DEFAULT_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'payment-consumer',
    'auto.offset.reset': 'earliest'
}

# Default PostgreSQL configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'payment_system',
    'user': 'postgres',
    'password': 'postgres'
}

# Topics to subscribe to
TOPICS = ['payment-transactions', 'fraud-alerts']

# Flag for graceful shutdown
running = True

def shutdown_handler(signum, frame):
    """Handle shutdown signals to gracefully stop the consumer."""
    global running
    logger.info("Shutdown signal received, stopping consumer...")
    running = False

def connect_to_db(config):
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            dbname=config['database'],
            user=config['user'],
            password=config['password']
        )
        logger.info("Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def insert_transactions(conn, transactions):
    """Insert batch of transactions into database."""
    if not transactions:
        return 0
    
    cursor = conn.cursor()
    try:
        # Define columns to insert
        columns = [
            'transaction_id', 'user_id', 'amount', 'currency', 
            'payment_method', 'status', 'device_type', 'time',
            'latitude', 'longitude', 'country', 'city',
            'merchant_id', 'merchant_category', 'description',
            'is_fraud', 'fraud_score'
        ]
        
        # Prepare values for batch insert
        values = []
        for tx in transactions:
            geo = tx.get('geo_location', {})
            values.append((
                tx.get('transaction_id'),
                tx.get('user_id'),
                tx.get('amount'),
                tx.get('currency'),
                tx.get('payment_method'),
                tx.get('status'),
                tx.get('device_type'),
                datetime.fromisoformat(tx.get('timestamp').replace('Z', '+00:00')),
                geo.get('latitude'),
                geo.get('longitude'),
                geo.get('country'),
                geo.get('city'),
                tx.get('merchant_id'),
                tx.get('merchant_category'),
                tx.get('description'),
                tx.get('is_fraud', False),
                tx.get('fraud_score', 0.0)
            ))
        
        # Execute batch insert
        query = f"INSERT INTO transactions ({', '.join(columns)}) VALUES %s"
        execute_values(cursor, query, values)
        conn.commit()
        
        return len(values)
    except Exception as e:
        logger.error(f"Error inserting transactions: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def insert_fraud_events(conn, transactions):
    """Insert fraud events for high-risk transactions."""
    if not transactions:
        return 0
    
    cursor = conn.cursor()
    try:
        # Filter only high-risk fraud transactions
        high_risk = [tx for tx in transactions if tx.get('is_fraud', False) and tx.get('fraud_score', 0) > 0.7]
        
        if not high_risk:
            return 0
        
        # Prepare values for batch insert
        values = []
        for tx in high_risk:
            details = {
                'payment_method': tx.get('payment_method'),
                'device_type': tx.get('device_type'),
                'amount': tx.get('amount'),
                'user_id': tx.get('user_id')
            }
            
            values.append((
                tx.get('transaction_id'),
                datetime.fromisoformat(tx.get('timestamp').replace('Z', '+00:00')),
                tx.get('fraud_score', 0.0),
                json.dumps(details)
            ))
        
        # Execute batch insert
        query = """
            INSERT INTO fraud_events 
            (transaction_id, time, fraud_score, details)
            VALUES %s
        """
        execute_values(cursor, query, values)
        conn.commit()
        
        return len(values)
    except Exception as e:
        logger.error(f"Error inserting fraud events: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()

def process_messages(consumer, db_config, batch_size=100, max_messages=None):
    """Process messages from Kafka and store in database."""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Connect to database
    conn = connect_to_db(db_config)
    
    # Initialize counters
    count = 0
    batch = []
    
    try:
        while running and (max_messages is None or count < max_messages):
            # Poll for messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition: {msg.topic()} {msg.partition()}")
                else:
                    logger.error(f"Error: {msg.error()}")
            else:
                try:
                    # Parse message value
                    value = json.loads(msg.value())
                    batch.append(value)
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        tx_count = insert_transactions(conn, batch)
                        fraud_count = insert_fraud_events(conn, batch)
                        logger.info(f"Processed batch: {tx_count} transactions, {fraud_count} fraud events")
                        batch = []
                    
                    count += 1
                    
                    # Log progress
                    if count % 1000 == 0:
                        logger.info(f"Processed {count} messages")
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
    finally:
        # Process any remaining messages in the batch
        if batch:
            tx_count = insert_transactions(conn, batch)
            fraud_count = insert_fraud_events(conn, batch)
            logger.info(f"Processed final batch: {tx_count} transactions, {fraud_count} fraud events")
        
        # Close database connection
        conn.close()
        
        # Close consumer
        consumer.close()
        
        logger.info(f"Consumer stopped. Processed {count} messages in total.")

def create_consumer(config, topics):
    """Create and configure Kafka consumer."""
    consumer = Consumer(config)
    consumer.subscribe(topics)
    logger.info(f"Subscribed to topics: {topics}")
    return consumer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Transaction Consumer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', 
                        help='Kafka bootstrap servers')
    parser.add_argument('--group-id', type=str, default='payment-consumer', 
                        help='Consumer group ID')
    parser.add_argument('--batch-size', type=int, default=100, 
                        help='Batch size for database inserts')
    parser.add_argument('--max-messages', type=int, default=None, 
                        help='Maximum number of messages to process (default: unlimited)')
    parser.add_argument('--db-host', type=str, default='localhost', 
                        help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, 
                        help='Database port')
    parser.add_argument('--db-name', type=str, default='payment_system', 
                        help='Database name')
    parser.add_argument('--db-user', type=str, default='postgres', 
                        help='Database user')
    parser.add_argument('--db-password', type=str, default='postgres', 
                        help='Database password')
    args = parser.parse_args()
    
    # Update Kafka configuration
    kafka_config = DEFAULT_CONFIG.copy()
    kafka_config['bootstrap.servers'] = args.bootstrap_servers
    kafka_config['group.id'] = args.group_id
    
    # Update database configuration
    db_config = DB_CONFIG.copy()
    db_config['host'] = args.db_host
    db_config['port'] = args.db_port
    db_config['database'] = args.db_name
    db_config['user'] = args.db_user
    db_config['password'] = args.db_password
    
    # Create consumer and process messages
    consumer = create_consumer(kafka_config, TOPICS)
    process_messages(
        consumer=consumer,
        db_config=db_config,
        batch_size=args.batch_size,
        max_messages=args.max_messages
    )