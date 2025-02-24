import json
import yaml
import time
from typing import Dict, Any, Union, Optional
from kafka import KafkaProducer
from loguru import logger


class TransactionProducer:
    """Produtor Kafka para transações."""
    
    def __init__(self, config_path: str = "config/kafka.yml"):
        """
        Inicializa o produtor Kafka.
        
        Args:
            config_path: Caminho para o arquivo de configuração do Kafka
        """
        # Carregar configuração
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['kafka']
        
        # Inicializar produtor
        self.producer = self._create_producer()
        logger.info("Produtor Kafka inicializado")
    
    def _create_producer(self) -> KafkaProducer:
        """Cria e configura um produtor Kafka."""
        bootstrap_servers = self.config['bootstrap_servers']
        producer_config = self.config['producer']
        
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=producer_config['acks'],
            retries=producer_config['retries'],
            batch_size=producer_config['batch_size'],
            linger_ms=producer_config['linger_ms'],
            buffer_memory=producer_config['buffer_memory']
        )
    
    def send_transaction(self, transaction_data: Dict[str, Any], topic: Optional[str] = None) -> None:
        """
        Envia uma transação para o Kafka.
        
        Args:
            transaction_data: Dados da transação em formato dict
            topic: Nome do tópico (se None, usa o tópico de transações)
        """
        if topic is None:
            topic = self.config['topics']['raw_transactions']
        
        self.producer.send(topic, transaction_data)
    
    def send_message(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Envia uma mensagem genérica para o Kafka.
        
        Args:
            topic: Nome do tópico
            message: Mensagem a ser enviada
        """
        self.producer.send(topic, message)
    
    def close(self) -> None:
        """Fecha o produtor e libera recursos."""
        self.producer.flush()
        self.producer.close()
        logger.info("Produtor Kafka fechado")


class FraudAlertProducer:
    """Produtor Kafka para alertas de fraude."""
    
    def __init__(self, config_path: str = "config/kafka.yml"):
        """
        Inicializa o produtor de alertas.
        
        Args:
            config_path: Caminho para o arquivo de configuração do Kafka
        """
        # Carregar configuração
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['kafka']
        
        # Inicializar produtor base
        self.transaction_producer = TransactionProducer(config_path)
        self.alert_topic = self.config['topics']['fraud_alerts']
        
        logger.info("Produtor de alertas inicializado")
    
    def send_alert(self, transaction_id: str, fraud_probability: float, 
                  fraud_type: str, transaction_data: Dict[str, Any]) -> None:
        """
        Envia um alerta de fraude para o Kafka.
        
        Args:
            transaction_id: ID da transação suspeita
            fraud_probability: Probabilidade estimada de fraude
            fraud_type: Tipo de fraude detectado
            transaction_data: Dados completos da transação
        """
        alert = {
            'timestamp': time.time(),
            'transaction_id': transaction_id,
            'fraud_probability': fraud_probability,
            'fraud_type': fraud_type,
            'severity': self._get_severity(fraud_probability),
            'transaction': transaction_data
        }
        
        self.transaction_producer.send_message(self.alert_topic, alert)
    
    def _get_severity(self, probability: float) -> str:
        """Determina a severidade com base na probabilidade."""
        if probability >= 0.9:
            return "critical"
        elif probability >= 0.7:
            return "high"
        elif probability >= 0.5:
            return "medium"
        else:
            return "low"
    
    def close(self) -> None:
        """Fecha o produtor de alertas."""
        self.transaction_producer.close()