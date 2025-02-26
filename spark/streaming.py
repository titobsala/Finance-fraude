import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, explode, split
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, MapType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection properties
DB_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://timescaledb:5432/payment_system"
}

def flatten_geo_location(df):
    """Flatten the geo_location field into separate columns."""
    return df.withColumn("latitude", col("geo_location.latitude")) \
            .withColumn("longitude", col("geo_location.longitude")) \
            .withColumn("country", col("geo_location.country")) \
            .withColumn("city", col("geo_location.city")) \
            .drop("geo_location")

def create_transaction_schema():
    """Create schema for transaction data."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("geo_location", MapType(StringType(), StringType()), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_score", FloatType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("description", StringType(), True)
    ])

def process_transactions(spark, kafka_bootstrap_servers, checkpoint_location):
    """
    Process transaction streams from Kafka.
    
    Args:
        spark: SparkSession
        kafka_bootstrap_servers: Kafka bootstrap servers
        checkpoint_location: Directory for checkpointing
    """
    logger.info("Starting transaction stream processing")
    
    # Define transaction schema
    transaction_schema = create_transaction_schema()

    # Read transaction stream from Kafka
    transaction_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "payment-transactions") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_data")

    # Parse JSON data
    transactions_df = transaction_stream \
        .select(from_json(col("json_data"), transaction_schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", to_timestamp(col("timestamp"))) \
        .drop("timestamp")
    
    # Flatten geo_location
    transactions_df = flatten_geo_location(transactions_df)

    # Save transactions to TimescaleDB
    transaction_writer = transactions_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: save_to_db(df, "transactions")) \
        .option("checkpointLocation", os.path.join(checkpoint_location, "transactions")) \
        .start()

    # Process fraud alerts
    fraud_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "fraud-alerts") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_data")

    # Parse fraud JSON data
    fraud_df = fraud_stream \
        .select(from_json(col("json_data"), transaction_schema).alias("data")) \
        .select(
            col("data.transaction_id").alias("transaction_id"),
            to_timestamp(col("data.timestamp")).alias("time"),
            col("data.fraud_score").alias("fraud_score"),
            col("data.is_fraud").alias("is_fraud")
        )

    # Save fraud events to TimescaleDB
    fraud_writer = fraud_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: process_fraud_events(df)) \
        .option("checkpointLocation", os.path.join(checkpoint_location, "fraud")) \
        .start()

    # Compute aggregations by window
    windowed_counts = transactions_df \
        .withWatermark("time", "10 minutes") \
        .groupBy(
            window(col("time"), "5 minutes"),
            col("payment_method")
        ) \
        .count()

    # Output aggregated counts to console for debugging
    query = windowed_counts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # Wait for all queries to terminate
    spark.streams.awaitAnyTermination()

def save_to_db(df, table_name):
    """
    Save DataFrame to TimescaleDB.
    
    Args:
        df: DataFrame to save
        table_name: Target table name
    """
    if not df.isEmpty():
        logger.info(f"Saving {df.count()} records to {table_name}")
        df.write \
            .format("jdbc") \
            .option("url", DB_PROPERTIES["url"]) \
            .option("dbtable", table_name) \
            .option("user", DB_PROPERTIES["user"]) \
            .option("password", DB_PROPERTIES["password"]) \
            .option("driver", DB_PROPERTIES["driver"]) \
            .mode("append") \
            .save()
    else:
        logger.info(f"No data to save to {table_name}")

def process_fraud_events(df):
    """
    Process fraud events and save to database.
    
    Args:
        df: DataFrame with fraud events
    """
    if not df.isEmpty():
        # Create a details JSON field
        df = df.withColumn("details", 
                          df.select("transaction_id", "fraud_score", "is_fraud").toJSON())
        
        # Get only confirmed fraud cases with a high fraud score
        high_risk_fraud = df.filter(df.fraud_score > 0.7).select(
            col("transaction_id"),
            col("time"),
            col("fraud_score"),
            col("details")
        )
        
        # Save to fraud_events table
        save_to_db(high_risk_fraud, "fraud_events")

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName("PaymentStreamProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.23") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Process transactions
    process_transactions(
        spark=spark,
        kafka_bootstrap_servers="kafka:29092",
        checkpoint_location="/tmp/spark-checkpoints"
    )