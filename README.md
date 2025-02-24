# Sistema de Detecção de Fraudes em Tempo Real

## Visão Geral
Sistema de detecção de fraudes em tempo real utilizando Apache Kafka e Spark para processamento de transações financeiras. O projeto implementa uma arquitetura completa de streaming de dados, desde a geração de dados simulados até a visualização em tempo real no Grafana.

## Arquitetura

O sistema é composto por cinco componentes principais:
1. Gerador de Dados - Simula transações financeiras com padrões normais e fraudulentos
2. Message Broker (Kafka) - Gerencia o fluxo de mensagens entre componentes
3. Processamento em Tempo Real (Spark) - Analisa transações em tempo real
4. Sistema de Detecção de Fraudes - Identifica transações suspeitas
5. Dashboard de Visualização (Grafana) - Monitora métricas e alertas

## Tecnologias Utilizadas
- Python 3.9+
- Apache Kafka
- Apache Spark
- Docker & Docker Compose
- MLlib (Spark ML)
- Grafana & InfluxDB
- Git (Controle de Versão)

## Estrutura do Projeto
```
fraud-detection/
├── docker/
│   ├── Dockerfile.generator
│   ├── Dockerfile.processor
│   ├── Dockerfile.spark
│   └── docker-compose.yml
├── src/
│   ├── data_generator/
│   │   ├── __init__.py
│   │   ├── generator.py
│   │   └── transaction.py
│   ├── kafka_utils/
│   │   ├── __init__.py
│   │   ├── producer.py
│   │   └── consumer.py
│   ├── spark_processor/
│   │   ├── __init__.py
│   │   └── streaming.py
│   ├── fraud_detector/
│   │   ├── __init__.py
│   │   ├── model.py
│   │   └── detector.py
│   └── dashboard/
│       ├── __init__.py
│       └── metrics.py
├── config/
│   ├── generator.yml
│   ├── kafka.yml
│   ├── spark.yml
│   └── grafana/
│       ├── dashboards/
│       └── datasources/
├── tests/
├── models/
└── docs/
```

## Roadmap de Desenvolvimento

### Fase 1: Setup Inicial e Geração de Dados
- [x] Configuração do ambiente Docker
- [x] Implementação do gerador de dados
- [x] Configuração inicial do Kafka

### Fase 2: Processamento de Dados
- [x] Implementação do consumer Kafka
- [x] Configuração do Spark Streaming
- [x] Desenvolvimento do pipeline de processamento

### Fase 3: Detecção de Fraudes
- [x] Implementação do modelo ML
- [x] Integração com Spark Streaming
- [x] Sistema de alertas

### Fase 4: Visualização
- [ ] Desenvolvimento do dashboard
- [ ] Implementação de métricas em tempo real
- [ ] Visualização de alertas

### Fase 5: Testes e Documentação
- [ ] Testes unitários
- [ ] Testes de integração
- [ ] Documentação completa

## Como Contribuir
1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## Licença
Este projeto está sob a licença MIT.