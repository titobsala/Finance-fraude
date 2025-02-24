import json
import yaml
import time
from typing import Dict, Any, Callable, List, Optional
from kafka import KafkaConsumer
from loguru import logger


class TransactionConsumer:
    """Consumidor Kafka para transações."""
    
    def __init__(self, config_path: str = "config/kafka.yml", group_id: Optional[str] = None):
        """
        Inicializa o consumidor Kafka.
        
        Args:
            config_path: Caminho para o arquivo de configuração do Kafka
            group_id: ID do grupo de consumidores (se None, usa o ID padrão)
        """
        # Carregar configuração
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['kafka']
        
        # Usar group_id do parâmetro ou da configuração
        self.group_id = group_id if group_id else self.config['consumer']['group_id']
        
        # Inicializar consumidor
        self.topic = self.config['topics']['raw_transactions']
        self.consumer = self._create_consumer([self.topic])
        
        logger.info(f"Consumidor Kafka inicializado para tópico: {self.topic}")
    
    def _create_consumer(self, topics: List[str]) -> KafkaConsumer:
        """
        Cria e configura um consumidor Kafka.
        
        Args:
            topics: Lista de tópicos para inscrever
        
        Returns:
            Consumidor Kafka configurado
        """
        bootstrap_servers = self.config['bootstrap_servers']
        consumer_config = self.config['consumer']
        
        return KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=consumer_config['auto_offset_reset'],
            enable_auto_commit=consumer_config['enable_auto_commit'],
            auto_commit_interval_ms=consumer_config['auto_commit_interval_ms'],
            max_poll_records=consumer_config['max_poll_records'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def consume_batch(self, batch_size: int = 100, timeout_ms: int = 1000) -> List[Dict[str, Any]]:
        """
        Consome um lote de mensagens do Kafka.
        
        Args:
            batch_size: Tamanho máximo do lote
            timeout_ms: Tempo limite em milissegundos
        
        Returns:
            Lista de mensagens consumidas
        """
        messages = []
        end_time = time.time() + (timeout_ms / 1000)
        
        for _ in range(batch_size):
            if time.time() > end_time:
                break
                
            # Poll for a single message
            message_batch = self.consumer.poll(timeout_ms=100, max_records=1)
            
            if not message_batch:
                continue
                
            # Process the first message from the first topic partition
            for tp, records in message_batch.items():
                if records:
                    messages.append(records[0].value)
                    break
        
        return messages
    
    def consume_stream(self, process_func: Callable[[Dict[str, Any]], None],
                      batch_size: int = 100, poll_interval: float = 1.0) -> None:
        """
        Consome mensagens em fluxo contínuo, processando cada uma.
        
        Args:
            process_func: Função para processar cada mensagem
            batch_size: Tamanho do lote para poll
            poll_interval: Intervalo entre polls em segundos
        """
        try:
            while True:
                message_batch = self.consumer.poll(
                    timeout_ms=int(poll_interval * 1000),
                    max_records=batch_size
                )
                
                for tp, records in message_batch.items():
                    for record in records:
                        try:
                            process_func(record.value)
                        except Exception as e:
                            logger.error(f"Erro ao processar mensagem: {e}")
                
                # Commit offsets
                self.consumer.commit()
                
        except KeyboardInterrupt:
            logger.info("Consumo interrompido pelo usuário")
        finally:
            self.close()
    
    def subscribe(self, topics: List[str]) -> None:
        """
        Inscreve o consumidor em tópicos adicionais.
        
        Args:
            topics: Lista de tópicos para inscrever
        """
        self.consumer.subscribe(topics)
        logger.info(f"Consumidor inscrito nos tópicos: {topics}")
    
    def close(self) -> None:
        """Fecha o consumidor e libera recursos."""
        self.consumer.close()
        logger.info("Consumidor Kafka fechado")


class MultiTopicConsumer:
    """Consumidor Kafka para múltiplos tópicos."""
    
    def __init__(self, topics: List[str], config_path: str = "config/kafka.yml", 
                group_id: Optional[str] = None):
        """
        Inicializa o consumidor multi-tópico.
        
        Args:
            topics: Lista de tópicos para consumir
            config_path: Caminho para o arquivo de configuração do Kafka
            group_id: ID do grupo de consumidores (se None, usa o ID padrão)
        """
        # Carregar configuração
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['kafka']
        
        # Usar group_id do parâmetro ou da configuração
        self.group_id = group_id if group_id else self.config['consumer']['group_id']
        
        # Inicializar consumidor
        self.topics = topics
        self.consumer = self._create_consumer(topics)
        
        logger.info(f"Consumidor multi-tópico inicializado para: {topics}")
    
    def _create_consumer(self, topics: List[str]) -> KafkaConsumer:
        """
        Cria e configura um consumidor Kafka.
        
        Args:
            topics: Lista de tópicos para inscrever
        
        Returns:
            Consumidor Kafka configurado
        """
        bootstrap_servers = self.config['bootstrap_servers']
        consumer_config = self.config['consumer']
        
        return KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=consumer_config['auto_offset_reset'],
            enable_auto_commit=consumer_config['enable_auto_commit'],
            auto_commit_interval_ms=consumer_config['auto_commit_interval_ms'],
            max_poll_records=consumer_config['max_poll_records'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def consume_with_handlers(self, handlers: Dict[str, Callable[[Dict[str, Any]], None]],
                             poll_interval: float = 1.0, batch_size: int = 100) -> None:
        """
        Consome mensagens de múltiplos tópicos com handlers específicos.
        
        Args:
            handlers: Dicionário de {topic: handler_function}
            poll_interval: Intervalo entre polls em segundos
            batch_size: Tamanho do lote para poll
        """
        try:
            while True:
                message_batch = self.consumer.poll(
                    timeout_ms=int(poll_interval * 1000),
                    max_records=batch_size
                )
                
                for tp, records in message_batch.items():
                    topic = tp.topic
                    
                    if topic in handlers:
                        handler = handlers[topic]
                        for record in records:
                            try:
                                handler(record.value)
                            except Exception as e:
                                logger.error(f"Erro ao processar mensagem do tópico {topic}: {e}")
                
                # Commit offsets
                self.consumer.commit()
                
        except KeyboardInterrupt:
            logger.info("Consumo interrompido pelo usuário")
        finally:
            self.close()
    
    def close(self) -> None:
        """Fecha o consumidor e libera recursos."""
        self.consumer.close()
        logger.info("Consumidor multi-tópico fechado")