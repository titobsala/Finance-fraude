import os
import yaml
import json
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loguru import logger
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from src.fraud_detector.detector import FraudDetector


class TransactionProcessor:
    """Processador de transações usando Spark Streaming."""
    
    def __init__(self, spark_config_path: str = "config/spark.yml", 
                kafka_config_path: str = "config/kafka.yml",
                influxdb_config_path: str = "config/influxdb.yml"):
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
        """Cria e configura uma sessão Spark."""
        app_config = self.spark_config['app']
        executor_config = self.spark_config['executor']
        
        return (SparkSession.builder
                .appName(app_config['name'])
                .master(app_config['master'])
                .config("spark.executor.memory", executor_config['memory'])
                .config("spark.executor.cores", executor_config['cores'])
                .config("spark.executor.instances", executor_config['instances'])
                .config("spark.sql.streaming.checkpointLocation", self.spark_config['checkpoint']['dir'])
                .getOrCreate())
    
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
        """Processa as transações e detecta fraudes."""
        # Aplicar detector de fraudes usando UDF
        fraud_detector_udf = udf(
            lambda amount, tx_type, merchant_cat, hour, day, country, device_type:
            self.fraud_detector.predict_fraud_probability(
                amount, tx_type, merchant_cat, hour, day, country, device_type
            ),
            DoubleType()
        )
        
        processed_df = transactions_df.withColumn(
            "ml_fraud_probability",
            fraud_detector_udf(
                "amount", "transaction_type", "merchant_category", 
                "hour_of_day", "day_of_week", "country", "device_type"
            )
        )
        
        # Definir status com base na probabilidade de fraude
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
            
            # Transações por segundo
            point = Point("transactions_per_second").tag("batch_id", str(batch_id)).field("count", total_transactions).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Total de transações
            point = Point("total_transactions").tag("batch_id", str(batch_id)).field("count", total_transactions).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Transações por status
            for row in status_counts:
                point = Point("transactions_by_status").tag("status", row["final_status"]).field("count", row["count"]).time(current_time)
                self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Transações por tipo
            for row in type_counts:
                point = Point("transactions_by_type").tag("type", row["transaction_type"]).field("count", row["count"]).time(current_time)
                self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Fraudes detectadas
            point = Point("fraud_detected").field("count", fraud_count).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Taxa de fraude
            fraud_rate = (fraud_count / total_transactions * 100) if total_transactions > 0 else 0
            point = Point("fraud_rate").field("percentage", fraud_rate).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Volume total
            point = Point("volume_total").tag("currency", "BRL").field("amount", total_volume).time(current_time)
            self.write_api.write(bucket=self.influxdb_config['bucket'], record=point)
            
            # Volume de fraudes
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
        
        logger.info("Streaming iniciado, aguardando término...")
        
        # Aguardar término do processamento
        self.spark.streams.awaitAnyTermination()


def main():
    """Função principal para iniciar o processador."""
    processor = TransactionProcessor()
    processor.process_stream()


if __name__ == "__main__":
    main()