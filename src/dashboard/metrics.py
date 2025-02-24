import os
import json
import time
import yaml
from typing import Dict, Any, List, Optional
import requests
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from datetime import datetime


class MetricsCollector:
    """Coletor de métricas para o dashboard Grafana via InfluxDB."""
    
    def __init__(self, config_path: str = "config/kafka.yml"):
        """
        Inicializa o coletor de métricas.
        
        Args:
            config_path: Caminho para configuração do Kafka
        """
        # Carregar configuração
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['kafka']
        
        # Configuração do InfluxDB
        self.influxdb_url = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
        self.influxdb_org = os.environ.get("INFLUXDB_ORG", "fraud-detection")
        self.influxdb_bucket = os.environ.get("INFLUXDB_BUCKET", "metrics")
        self.influxdb_token = os.environ.get("INFLUXDB_TOKEN", "my-super-secret-token")
        
        # Estatísticas internas
        self.stats = {
            "total_transactions": 0,
            "total_fraud": 0,
            "total_amount": 0,
            "fraud_amount": 0,
            "transactions_by_type": {},
            "fraud_by_type": {}
        }
        
        logger.info("Coletor de métricas inicializado")
    
    def _send_to_influxdb(self, measurement: str, fields: Dict[str, Any], 
                         tags: Optional[Dict[str, str]] = None) -> bool:
        """
        Envia métricas para o InfluxDB.
        
        Args:
            measurement: Nome da medição
            fields: Campos e valores
            tags: Tags opcionais
            
        Returns:
            True se sucesso, False caso contrário
        """
        if tags is None:
            tags = {}
        
        # Construir linha de protocolo do InfluxDB
        tag_str = ",".join([f"{k}={v}" for k, v in tags.items()]) if tags else ""
        field_str = ",".join([f"{k}={v}" for k, v in fields.items()])
        
        timestamp = int(time.time() * 1_000_000_000)  # nanosegundos
        
        if tag_str:
            line = f"{measurement},{tag_str} {field_str} {timestamp}"
        else:
            line = f"{measurement} {field_str} {timestamp}"
        
        # Enviar para InfluxDB
        headers = {
            "Authorization": f"Token {self.influxdb_token}",
            "Content-Type": "text/plain"
        }
        
        try:
            url = f"{self.influxdb_url}/api/v2/write?org={self.influxdb_org}&bucket={self.influxdb_bucket}&precision=ns"
            response = requests.post(url, headers=headers, data=line)
            
            if response.status_code == 204:  # Sucesso
                return True
            else:
                logger.error(f"Erro ao enviar para InfluxDB: {response.status_code} - {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"Exceção ao enviar para InfluxDB: {e}")
            return False
    
    def process_transaction(self, transaction: Dict[str, Any]) -> None:
        """
        Processa uma transação e atualiza métricas.
        
        Args:
            transaction: Dados da transação
        """
        # Extrair dados relevantes
        amount = transaction.get('amount', 0.0)
        tx_type = transaction.get('transaction_type', 'unknown')
        is_fraud = transaction.get('is_fraud', False)
        fraud_probability = transaction.get('final_fraud_probability', 
                                          transaction.get('fraud_probability', 0.0))
        
        # Atualizar estatísticas
        self.stats["total_transactions"] += 1
        self.stats["total_amount"] += amount
        
        # Estatísticas por tipo
        if tx_type not in self.stats["transactions_by_type"]:
            self.stats["transactions_by_type"][tx_type] = 0
        self.stats["transactions_by_type"][tx_type] += 1
        
        # Estatísticas de fraude
        if is_fraud or fraud_probability >= 0.7:
            self.stats["total_fraud"] += 1
            self.stats["fraud_amount"] += amount
            
            fraud_type = transaction.get('fraud_type', 'unknown')
            if fraud_type not in self.stats["fraud_by_type"]:
                self.stats["fraud_by_type"][fraud_type] = 0
            self.stats["fraud_by_type"][fraud_type] += 1
        
        # Enviar métricas em tempo real
        self._send_transaction_metrics(transaction)
    
    def _send_transaction_metrics(self, transaction: Dict[str, Any]) -> None:
        """
        Envia métricas de uma transação para o InfluxDB.
        
        Args:
            transaction: Dados da transação
        """
        # Métricas comuns
        amount = transaction.get('amount', 0.0)
        tx_type = transaction.get('transaction_type', 'unknown')
        is_fraud = transaction.get('is_fraud', False)
        fraud_probability = transaction.get('final_fraud_probability', 
                                          transaction.get('fraud_probability', 0.0))
        
        # Métrica de transações por segundo
        self._send_to_influxdb(
            "transactions_per_second",
            {"value": 1},
            {"type": tx_type}
        )
        
        # Métrica de valor
        self._send_to_influxdb(
            "transaction_amount",
            {"value": amount},
            {"type": tx_type}
        )
        
        # Métrica de fraude
        if is_fraud or fraud_probability >= 0.7:
            self._send_to_influxdb(
                "fraud_transaction",
                {"value": 1, "amount": amount, "probability": fraud_probability},
                {"type": tx_type, "fraud_type": transaction.get('fraud_type', 'unknown')}
            )
    
    def send_aggregated_metrics(self) -> None:
        """Envia métricas agregadas para o InfluxDB."""
        # Taxa de fraude
        if self.stats["total_transactions"] > 0:
            fraud_rate = self.stats["total_fraud"] / self.stats["total_transactions"]
        else:
            fraud_rate = 0.0
        
        # Métricas agregadas
        self._send_to_influxdb("total_transactions", {"value": self.stats["total_transactions"]})
        self._send_to_influxdb("total_fraud", {"value": self.stats["total_fraud"]})
        self._send_to_influxdb("total_amount", {"value": self.stats["total_amount"]})
        self._send_to_influxdb("fraud_amount", {"value": self.stats["fraud_amount"]})
        self._send_to_influxdb("fraud_rate", {"value": fraud_rate})
        
        # Métricas por tipo de transação
        for tx_type, count in self.stats["transactions_by_type"].items():
            self._send_to_influxdb(
                "transactions_by_type",
                {"value": count},
                {"transaction_type": tx_type}
            )
        
        # Métricas por tipo de fraude
        for fraud_type, count in self.stats["fraud_by_type"].items():
            self._send_to_influxdb(
                "fraud_by_type",
                {"value": count},
                {"fraud_type": fraud_type}
            )
    
    def process_batch(self, transactions: List[Dict[str, Any]]) -> None:
        """
        Processa um lote de transações.
        
        Args:
            transactions: Lista de transações
        """
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.process_transaction, transactions)
        
        # Enviar métricas agregadas após processar o lote
        self.send_aggregated_metrics()
    
    def process_model_metrics(self, model_metrics: Dict[str, Any]) -> None:
        """
        Processa e envia métricas do modelo.
        
        Args:
            model_metrics: Métricas do modelo de ML
        """
        # Extrair métricas principais
        accuracy = model_metrics.get('accuracy', 0.0)
        roc_auc = model_metrics.get('roc_auc', 0.0)
        model_version = model_metrics.get('model_version', 'unknown')
        
        # Enviar para InfluxDB
        self._send_to_influxdb(
            "model_accuracy",
            {"value": accuracy},
            {"model_version": model_version}
        )
        
        self._send_to_influxdb(
            "model_roc_auc",
            {"value": roc_auc},
            {"model_version": model_version}
        )
        
        # Se temos métricas detalhadas
        if 'classification_report' in model_metrics:
            report = model_metrics['classification_report']
            
            # Metrics por classe
            for class_name, metrics in report.items():
                if isinstance(metrics, dict):  # ignorar métricas agregadas
                    precision = metrics.get('precision', 0.0)
                    recall = metrics.get('recall', 0.0)
                    f1_score = metrics.get('f1-score', 0.0)
                    
                    self._send_to_influxdb(
                        "model_class_metrics",
                        {"precision": precision, "recall": recall, "f1_score": f1_score},
                        {"model_version": model_version, "class": class_name}
                    )


def main():
    """Função principal para iniciar o coletor de métricas."""
    from src.kafka_utils.consumer import MultiTopicConsumer
    
    # Inicializar coletor
    collector = MetricsCollector()
    
    # Tópicos para monitorar
    kafka_config_path = os.environ.get("KAFKA_CONFIG_PATH", "config/kafka.yml")
    with open(kafka_config_path, 'r') as f:
        kafka_config = yaml.safe_load(f)['kafka']
    
    topics = [
        kafka_config['topics']['processed_transactions'],
        kafka_config['topics']['fraud_alerts']
    ]
    
    # Definir handlers para cada tópico
    handlers = {
        kafka_config['topics']['processed_transactions']: collector.process_transaction,
        kafka_config['topics']['fraud_alerts']: lambda x: collector.process_transaction(x.get('transaction', {}))
    }
    
    # Iniciar consumidor
    consumer = MultiTopicConsumer(topics, kafka_config_path, group_id="metrics-collector")
    
    try:
        # Iniciar coleta de métricas
        logger.info("Iniciando coleta de métricas")
        consumer.consume_with_handlers(handlers)
    except KeyboardInterrupt:
        logger.info("Coleta de métricas interrompida pelo usuário")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()