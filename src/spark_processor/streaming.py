import os
import yaml
import json
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
from loguru import logger
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from src.fraud_detector.detector import FraudDetector


class TransactionProcessor:
    """Processador de transações usando Spark Streaming."""
    
    def __init__(self, spark_config_path: str = "config/spark.yml", 
                kafka_config_path: str = "/app/config/kafka.yml",
                influxdb_config_path: str = "/app/config/influxdb.yml"):
        """
        Inicializa o processador de transações.
        
        Args:
            spark_config_path: Caminho para o arquivo de configuração do Spark
            kafka_config_path: Caminho para o arquivo de configuração do Kafka
            influxdb_config_path: Caminho para o arquivo de configuração do InfluxDB
        """
        # Carregar configurações
        with open(spark_config_path, 'r') as f:
            self.spark_config = yaml.safe_load(f)['spark']
        
        with open(kafka_config_path, 'r') as f:
            self.kafka_config = yaml.safe_load(f)['kafka']
        
        # Configuração padrão do InfluxDB se o arquivo não existir
        self.influxdb_config = {
            'url': 'http://influxdb:8086',
            'token': 'my-super-secret-token',
            'org': 'fraud-detection',
            'bucket': 'metrics'
        }
        
        # Tentar carregar configuração do InfluxDB se existir
        try:
            with open(influxdb_config_path, 'r') as f:
                self.influxdb_config = yaml.safe_load(f)['influxdb']
        except (FileNotFoundError, KeyError):
            logger.warning(f"Arquivo de configuração do InfluxDB não encontrado ou inválido. Usando configuração padrão.")
        
        # Inicializar cliente InfluxDB
        self.influxdb_client = InfluxDBClient(
            url=self.influxdb_config['url'],
            token=self.influxdb_config['token'],
            org=self.influxdb_config['org']
        )
        self.write_api = self.influxdb_client.write_api(write_options=SYNCHRONOUS)
        
        # Inicializar sessão Spark
        self.spark = self._create_spark_session()
        
        # Configurar processamento de logs
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Inicializar detector de fraudes
        self.fraud_detector = FraudDetector()
        
        logger.info("Processador de transações inicializado")
    
    def _create_spark_session(self) -> SparkSession:
        """Creates and configures a Spark session with fallback to local mode."""
        app_config = self.spark_config['app']
        executor_config = self.spark_config['executor']

        logger.info(f"Iniciando sessão Spark com master: {app_config['master']}")

        # For deployment, create a shared checkpoint dir with proper permissions
        checkpoint_dir = self.spark_config['checkpoint']['dir']

        try:
            # Try connecting to the Spark master
            spark = (SparkSession.builder
                    .appName(app_config['name'])
                    .master(app_config['master'])
                    .config("spark.executor.memory", executor_config['memory'])
                    .config("spark.executor.cores", str(executor_config['cores']))
                    .config("spark.executor.instances", str(executor_config['instances']))
                    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
                    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
                    .config("spark.driver.userClassPathFirst", "true")
                    .config("spark.executor.userClassPathFirst", "true")
                    # Increase timeout for master discovery 
                    .config("spark.network.timeout", "120s")
                    .getOrCreate())

            logger.info("Conexão com Spark master estabelecida")
            return spark

        except Exception as e:
            logger.error(f"Erro ao conectar ao Spark master: {e}")
            logger.warning("Tentando iniciar Spark em modo local")

            # Fallback to local mode
            spark = (SparkSession.builder
                    .appName(app_config['name'])
                    .master("local[*]")
                    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
                    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
                    .getOrCreate())

            logger.info("Sessão Spark local criada com sucesso")
            return spark

    def _define_schema(self) -> StructType:
        """Define o schema para as transações."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("status", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("fraud_type", StringType(), True),
            StructField("fraud_probability", DoubleType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True)
        ])
    
    def _read_transactions_stream(self) -> DataFrame:
        """Configura o streaming de entrada do Kafka."""
        bootstrap_servers = self.kafka_config['bootstrap_servers']
        transactions_topic = self.kafka_config['topics']['raw_transactions']
        
        logger.info(f"Conectando ao Kafka: {bootstrap_servers}, Tópico: {transactions_topic}")
        
        return (self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("subscribe", transactions_topic)
                .option("startingOffsets", self.spark_config['kafka']['starting_offsets'])
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json("value", self._define_schema()).alias("data"))
                .select("data.*"))
        
    def _process_transactions(self, transactions_df):
        """Processes the transactions and detects frauds using a serializable approach."""

        # Extract rules from fraud detector to make them serializable
        amount_thresholds = self.fraud_detector.rules["amount_thresholds"]
        high_risk_categories = self.fraud_detector.rules["high_risk_categories"]
        high_risk_countries = self.fraud_detector.rules["high_risk_countries"]

        # Create a serializable rule-based scoring function
        def calculate_fraud_score(amount, tx_type, merchant_cat, hour, day, country, device_type):
            score = 0.0

            # Check transaction amount
            if tx_type in amount_thresholds:
                threshold = amount_thresholds[tx_type]
                if amount > threshold:
                    ratio = amount / threshold
                    score += min(0.5, ratio / 10)

            # Check merchant category
            if merchant_cat in high_risk_categories:
                score += 0.2

            # Check country
            if country in high_risk_countries:
                score += 0.3

            # Check time (early morning = higher risk)
            if 0 <= hour <= 5:
                score += 0.2

            return min(0.95, score)

        # Register UDF with proper serialization
        fraud_score_udf = udf(calculate_fraud_score, DoubleType())

        # Apply UDF to process transactions
        processed_df = transactions_df.withColumn(
            "ml_fraud_probability",
            fraud_score_udf(
                col("amount"), 
                col("transaction_type"), 
                col("merchant_category"),
                col("hour_of_day"), 
                col("day_of_week"), 
                col("country"), 
                col("device_type")
            )
        )

        # Define final fraud probability and status
        return processed_df.withColumn(
            "final_fraud_probability",
            when(col("is_fraud"), col("fraud_probability"))
            .otherwise(col("ml_fraud_probability"))
        ).withColumn(
            "final_status",
            when(col("final_fraud_probability") >= 0.7, "rejected")
            .when(col("final_fraud_probability") >= 0.4, "review")
            .otherwise("approved")
        )
    
    def _generate_alerts(self, processed_df):
        """Gera alertas para transações suspeitas."""
        # Filtrar apenas transações suspeitas
        alerts_df = processed_df.filter(col("final_fraud_probability") >= 0.4)
        
        # Adicionar metadados de alerta
        return alerts_df.withColumn(
            "alert_level",
            when(col("final_fraud_probability") >= 0.7, "high")
            .otherwise("medium")
        ).withColumn(
            "alert_id", 
            concat(lit("alert-"), col("id"))
        )
    
    def _write_metrics_to_influxdb(self, batch_df, batch_id):
        """Escreve métricas agregadas para o InfluxDB."""
        try:
            # Métricas de transações por segundo
            current_time = datetime.datetime.now()
            
            # Contar total de transações no batch
            total_transactions = batch_df.count()
            
            # Contar transações por status
            status_counts = batch_df.groupBy("final_status").count().collect()
            
            # Contar transações por tipo
            type_counts = batch_df.groupBy("transaction_type").count().collect()
            
            # Contar fraudes detectadas
            fraud_count = batch_df.filter(col("final_fraud_probability") >= 0.7).count()
            
            # Volume total de transações em R$
            total_volume = batch_df.agg(sum("amount").alias("total_amount")).collect()[0]["total_amount"]
            
            # Volume de fraudes em R$
            fraud_volume = batch_df.filter(col("final_fraud_probability") >= 0.7).agg(sum("amount").alias("fraud_amount")).collect()
            fraud_volume = fraud_volume[0]["fraud_amount"] if fraud_volume[0]["fraud_amount"] else 0
            
            # Escrever pontos no InfluxDB
            point = Point("transactions_per_second").tag("batch_id", str(batch_id)).field("count", total_transactions).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            point = Point("total_transactions").tag("batch_id", str(batch_id)).field("count", total_transactions).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            for row in status_counts:
                point = Point("transactions_by_status").tag("status", row["final_status"]).field("count", row["count"]).time(current_time)
                self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            for row in type_counts:
                point = Point("transactions_by_type").tag("type", row["transaction_type"]).field("count", row["count"]).time(current_time)
                self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            point = Point("fraud_detected").field("count", fraud_count).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            fraud_rate = (fraud_count / total_transactions * 100) if total_transactions > 0 else 0
            point = Point("fraud_rate").field("percentage", fraud_rate).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            point = Point("volume_total").tag("currency", "BRL").field("amount", total_volume).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            point = Point("volume_fraud").tag("currency", "BRL").field("amount", fraud_volume).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            logger.info(f"Métricas do batch {batch_id} enviadas para o InfluxDB")
        except Exception as e:
            logger.error(f"Erro ao enviar métricas para o InfluxDB: {e}")
    
    def _write_to_kafka(self, df, topic, output_mode="append"):
        """Escreve um DataFrame para um tópico Kafka."""
        bootstrap_servers = self.kafka_config['bootstrap_servers']
        
        return (df.selectExpr("to_json(struct(*)) AS value")
                .writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", topic)
                .option("checkpointLocation", f"{self.spark_config['checkpoint']['dir']}/{topic}")
                .outputMode(output_mode)
                .start())
    
    def _write_to_console(self, df, output_mode="append"):
        """Escreve um DataFrame para o console (para debugging)."""
        return (df.writeStream
                .format("console")
                .option("truncate", False)
                .outputMode(output_mode)
                .start())
    
    def process_stream(self):
        """Inicia o processamento de streaming."""
        logger.info("Iniciando processamento de streaming")
        
        # Ler stream de transações
        transactions_df = self._read_transactions_stream()
        
        # Processar transações e detectar fraudes
        processed_df = self._process_transactions(transactions_df)
        
        # Gerar alertas para transações suspeitas
        alerts_df = self._generate_alerts(processed_df)
        
        # Escrever métricas para InfluxDB a cada batch processado
        query = (processed_df.writeStream
                .foreachBatch(self._write_metrics_to_influxdb)
                .outputMode("update")
                .start())
        
        # Escrever resultados para Kafka
        processed_stream = self._write_to_kafka(
            processed_df, 
            self.kafka_config['topics']['processed_transactions']
        )
        
        alerts_stream = self._write_to_kafka(
            alerts_df, 
            self.kafka_config['topics']['fraud_alerts']
        )
        
        # Também escrever para console (para debugging)
        if os.environ.get("DEBUG", "false").lower() == "true":
            console_stream = self._write_to_console(processed_df)
            logger.info("Streaming de console iniciado")
        
        logger.info("Streaming iniciado, aguardando término...")
        
        # Aguardar término do processamento
        self.spark.streams.awaitAnyTermination()


def main():
    """Função principal para iniciar o processador."""
    processor = TransactionProcessor()
    processor.process_stream()


if __name__ == "__main__":
    main()