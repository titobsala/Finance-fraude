import os
import yaml
import time
import random
import uuid
import ipaddress
import datetime
import multiprocessing
from faker import Faker
from typing import Dict, List, Tuple, Any
import numpy as np
import json
from kafka import KafkaProducer
from loguru import logger

from src.data_generator.transaction import (
    Transaction, 
    TransactionType, 
    MerchantCategory, 
    DeviceType,
    FraudType
)

class TransactionGenerator:
    """Gerador de transações simuladas para detecção de fraudes."""
    
    def __init__(self, config_path: str = "config/generator.yml", kafka_config_path: str = "config/kafka.yml"):
        """
        Inicializa o gerador de transações.
        
        Args:
            config_path: Caminho para o arquivo de configuração
            kafka_config_path: Caminho para configuração do Kafka
        """
        # Carrega configurações
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['generator']
        
        with open(kafka_config_path, 'r') as f:
            self.kafka_config = yaml.safe_load(f)['kafka']
        
        # Inicializa Faker para dados aleatórios
        self.faker = Faker()
        
        # Cache de usuários, dispositivos e comerciantes
        self.users = self._generate_users(10000)
        self.merchants = self._generate_merchants(1000)
        self.devices = self._generate_devices(5000)
        
        # Inicializar producer Kafka
        self.producer = self._create_kafka_producer()
        
        logger.info(f"Gerador de transações inicializado com {len(self.users)} usuários e {len(self.merchants)} comerciantes")
    
    def _create_kafka_producer(self) -> KafkaProducer:
        """Cria e configura um Kafka Producer."""
        bootstrap_servers = self.kafka_config['bootstrap_servers']
        
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=self.kafka_config['producer']['acks'],
            retries=self.kafka_config['producer']['retries'],
            batch_size=self.kafka_config['producer']['batch_size'],
            linger_ms=self.kafka_config['producer']['linger_ms'],
            buffer_memory=self.kafka_config['producer']['buffer_memory']
        )
    
    def _generate_users(self, num_users: int) -> List[Dict[str, Any]]:
        """Gera uma lista de usuários simulados."""
        users = []
        for _ in range(num_users):
            user_id = str(uuid.uuid4())
            
            # Configurações de gasto típico para este usuário
            avg_amount = random.uniform(50, 500)
            std_amount = avg_amount * 0.3
            
            # Padrões de localização para o usuário
            home_country = self.faker.country_code()
            home_city = self.faker.city()
            home_lat = float(self.faker.latitude())
            home_lng = float(self.faker.longitude())
            
            users.append({
                'id': user_id,
                'avg_amount': avg_amount,
                'std_amount': std_amount,
                'home_country': home_country,
                'home_city': home_city,
                'home_lat': home_lat,
                'home_lng': home_lng,
                'common_merchant_categories': random.sample(
                    list(MerchantCategory), k=random.randint(2, 5)
                ),
                'common_device_ids': []
            })
        
        return users
    
    def _generate_merchants(self, num_merchants: int) -> List[Dict[str, Any]]:
        """Gera uma lista de comerciantes simulados."""
        merchants = []
        for _ in range(num_merchants):
            merchant_id = str(uuid.uuid4())
            category = random.choice(list(MerchantCategory))
            
            merchants.append({
                'id': merchant_id,
                'name': self.faker.company(),
                'category': category,
                'country': self.faker.country_code(),
                'city': self.faker.city(),
                'lat': float(self.faker.latitude()),
                'lng': float(self.faker.longitude())
            })
        
        return merchants
    
    def _generate_devices(self, num_devices: int) -> List[Dict[str, Any]]:
        """Gera uma lista de dispositivos simulados."""
        devices = []
        for _ in range(num_devices):
            device_id = str(uuid.uuid4())
            device_type = random.choice(list(DeviceType))
            
            # Atribuir este dispositivo a alguns usuários
            user_ids = random.sample(
                [user['id'] for user in self.users],
                k=random.randint(1, 3)
            )
            
            # Adicionar este dispositivo aos usuários
            for user_id in user_ids:
                for user in self.users:
                    if user['id'] == user_id:
                        user['common_device_ids'].append(device_id)
            
            devices.append({
                'id': device_id,
                'type': device_type,
                'os': random.choice(['iOS', 'Android', 'Windows', 'MacOS', 'Linux']),
                'user_ids': user_ids
            })
        
        return devices
    
    def _get_transaction_amount(self, transaction_type: TransactionType, is_fraud: bool, user: Dict[str, Any]) -> float:
        """Gera um valor de transação com base no tipo e usuário."""
        # Obter configurações de valores para este tipo de transação
        amount_config = self.config['amounts'][transaction_type.value]
        
        if is_fraud and random.random() < 0.7:
            # Para transações fraudulentas, aumentar significativamente o valor
            multiplier = random.uniform(3, 10)
            amount = user['avg_amount'] * multiplier
        else:
            # Para transações legítimas, usar distribuição normal próxima da média do usuário
            mean = user['avg_amount']
            std_dev = user['std_amount']
            amount = max(amount_config['min'], min(
                amount_config['max'],
                np.random.normal(mean, std_dev)
            ))
        
        return round(amount, 2)
    
    def _get_transaction_location(self, user: Dict[str, Any], is_fraud: bool) -> Tuple[str, str, float, float]:
        """Gera uma localização para a transação."""
        if is_fraud and random.random() < 0.8:
            # Para transações fraudulentas, usar localização diferente da casa do usuário
            country = self.faker.country_code()
            city = self.faker.city()
            lat = float(self.faker.latitude())
            lng = float(self.faker.longitude())
        else:
            # Para transações legítimas, usar localização normal do usuário
            country = user['home_country']
            city = user['home_city']
            
            # Adicionar pequena variação na localização
            lat = user['home_lat'] + random.uniform(-0.1, 0.1)
            lng = user['home_lng'] + random.uniform(-0.1, 0.1)
        
        return country, city, lat, lng
    
    def _get_transaction_device(self, user: Dict[str, Any], is_fraud: bool) -> Dict[str, Any]:
        """Seleciona um dispositivo para a transação."""
        if is_fraud and random.random() < 0.7:
            # Para transações fraudulentas, usar um dispositivo não associado ao usuário
            non_user_devices = [d for d in self.devices if user['id'] not in d['user_ids']]
            if non_user_devices:
                return random.choice(non_user_devices)
        
        # Para transações legítimas ou alguns casos de fraude, usar dispositivo do usuário
        user_devices = [d for d in self.devices if d['id'] in user['common_device_ids']]
        if user_devices:
            return random.choice(user_devices)
        
        # Fallback: qualquer dispositivo
        return random.choice(self.devices)
    
    def _determine_fraud(self, user: Dict[str, Any]) -> Tuple[bool, FraudType]:
        """Determina se uma transação é fraudulenta e qual o tipo de fraude."""
        # Probabilidade base de fraude
        if random.random() < self.config['fraud']['probability']:
            is_fraud = True
            
            # Escolher tipo de fraude com base nas probabilidades configuradas
            fraud_patterns = self.config['fraud']['patterns']
            fraud_types = list(fraud_patterns.keys())
            fraud_weights = list(fraud_patterns.values())
            
            fraud_type_str = random.choices(fraud_types, weights=fraud_weights, k=1)[0]
            fraud_type = FraudType(fraud_type_str) if fraud_type_str != "legitimate" else FraudType.LEGITIMATE
            
            # Probabilidade de detecção (simulação)
            fraud_probability = random.uniform(0.6, 0.95)
        else:
            is_fraud = False
            fraud_type = FraudType.LEGITIMATE
            
            # Falsos positivos ocasionais
            fraud_probability = random.uniform(0, 0.1)
        
        return is_fraud, fraud_type, fraud_probability
    
    def generate_transaction(self) -> Transaction:
        """Gera uma única transação simulada."""
        # Selecionar usuário e comerciante aleatórios
        user = random.choice(self.users)
        merchant = random.choice(self.merchants)
        
        # Determinar o tipo de transação com base nas probabilidades configuradas
        tx_types = list(self.config['transaction_types'].keys())
        tx_weights = list(self.config['transaction_types'].values())
        tx_type_str = random.choices(tx_types, weights=tx_weights, k=1)[0]
        transaction_type = TransactionType(tx_type_str)
        
        # Determinar se é fraude
        is_fraud, fraud_type, fraud_probability = self._determine_fraud(user)
        
        # Obter detalhes da transação
        amount = self._get_transaction_amount(transaction_type, is_fraud, user)
        country, city, lat, lng = self._get_transaction_location(user, is_fraud)
        device = self._get_transaction_device(user, is_fraud)
        
        # Gerar timestamp (geralmente atual, com pequena variação)
        timestamp = datetime.datetime.now() - datetime.timedelta(
            seconds=random.randint(0, 60)
        )
        
        # Criar transação
        transaction = Transaction(
            user_id=user['id'],
            merchant_id=merchant['id'],
            timestamp=timestamp,
            amount=amount,
            transaction_type=transaction_type,
            merchant_category=merchant['category'],
            device_id=device['id'],
            device_type=device['type'],
            ip_address=str(self.faker.ipv4()),
            country=country,
            city=city,
            latitude=lat,
            longitude=lng,
            is_fraud=is_fraud,
            fraud_type=fraud_type,
            fraud_probability=fraud_probability,
        )
        
        # Configurar campos derivados
        transaction.set_derived_fields()
        
        return transaction
    
    def send_to_kafka(self, transaction: Transaction) -> None:
        """Envia uma transação para o Kafka."""
        topic = self.kafka_config['topics']['raw_transactions']
        self.producer.send(topic, transaction.to_dict())
    
    def generate_continuous(self, rate: int = None, process_id: int = 0) -> None:
        """
        Gera transações continuamente a uma determinada taxa.
        
        Args:
            rate: Transações por segundo (se None, usa configuração)
            process_id: ID do processo para logging
        """
        if rate is None:
            rate = self.config['rate']
        
        interval = 1.0 / rate
        
        logger.info(f"Processo {process_id}: Iniciando geração contínua a {rate} tx/segundo")
        
        try:
            while True:
                start_time = time.time()
                
                # Gerar e enviar transação
                transaction = self.generate_transaction()
                self.send_to_kafka(transaction)
                
                # Calcular tempo de processamento e aguardar
                processing_time = time.time() - start_time
                sleep_time = max(0, interval - processing_time)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            logger.info(f"Processo {process_id}: Geração interrompida")
        except Exception as e:
            logger.error(f"Processo {process_id}: Erro durante geração: {e}")
        finally:
            self.producer.flush()
            logger.info(f"Processo {process_id}: Finalizado")


def generate_worker(process_id: int, config_path: str, kafka_config_path: str, rate: int = None):
    """Função worker para geração de transações em paralelo."""
    generator = TransactionGenerator(config_path, kafka_config_path)
    generator.generate_continuous(rate=rate, process_id=process_id)


def main():
    """Função principal para iniciar a geração de transações."""
    logger.info("Iniciando gerador de transações")
    
    # Carregar configuração
    config_path = os.environ.get("CONFIG_PATH", "config/generator.yml")
    kafka_config_path = os.environ.get("KAFKA_CONFIG_PATH", "config/kafka.yml")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)['generator']
    
    # Determinar número de processos e taxa por processo
    num_processes = config.get('num_processes', 4)
    total_rate = config.get('rate', 100)
    rate_per_process = total_rate / num_processes
    
    logger.info(f"Iniciando {num_processes} processos geradores, taxa total: {total_rate} tx/segundo")
    
    # Iniciar processos
    processes = []
    try:
        for i in range(num_processes):
            p = multiprocessing.Process(
                target=generate_worker,
                args=(i, config_path, kafka_config_path, rate_per_process)
            )
            processes.append(p)
            p.start()
        
        # Aguardar todos os processos
        for p in processes:
            p.join()
    
    except KeyboardInterrupt:
        logger.info("Interrompendo gerador de transações")
        for p in processes:
            p.terminate()
    
    logger.info("Gerador de transações finalizado")


if __name__ == "__main__":
    main()