# Payment Processing System with Fraud Detection

Este projeto implementa um sistema de processamento de pagamentos em tempo real com detecção de fraudes, utilizando tecnologias modernas como Kafka, Spark e Grafana.

## Visão Geral

O sistema simula uma plataforma de processamento de pagamentos com os seguintes componentes:

- **Gerador de logs** - Simula transações de pagamento de várias fontes
- **Kafka** - Atua como hub central de mensagens
- **Spark Streaming** - Processa dados em tempo real
- **Modelo de detecção de fraudes** - Identifica transações suspeitas
- **Grafana** - Dashboard para visualização em tempo real

## Arquitetura

O fluxo de dados segue estas etapas:
1. Geração de dados simulados de diferentes fontes de pagamento
2. Ingestão de dados via Kafka
3. Processamento em tempo real com Spark Streaming
4. Detecção de fraudes com modelos de ML
5. Visualização e alertas via Grafana

## Requisitos

- Docker e Docker Compose
- Python 3.8+
- Apache Kafka
- Apache Spark
- Grafana
- TimescaleDB (para séries temporais)

## Estrutura do Projeto

```
payment-system/
├── docker-compose.yml
├── generator/
│   ├── transaction_generator.py
│   └── config.py
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── spark/
│   ├── streaming.py
│   └── batch_processing.py
├── models/
│   └── fraud_detection.py
├── dashboard/
│   └── grafana/
│       ├── dashboard.json
│       └── datasource.yml
└── tests/
    ├── test_generator.py
    └── test_processing.py
```

## Plano de Implementação

### Fase 1: Configuração da Infraestrutura
- [x] Criar estrutura do projeto
- [ ] Configurar Docker Compose com Kafka, ZooKeeper, Spark e Grafana
- [ ] Configurar TimescaleDB para armazenamento de séries temporais

### Fase 2: Geração de Dados
- [ ] Desenvolver gerador de transações com diferentes tipos de pagamento
- [ ] Implementar simulação de comportamentos normais e fraudulentos
- [ ] Criar produtor Kafka para enviar dados simulados

### Fase 3: Processamento em Tempo Real
- [ ] Desenvolver aplicação Spark Streaming para consumir tópicos Kafka
- [ ] Implementar transformações e agregações de dados
- [ ] Criar modelo básico de detecção de fraudes

### Fase 4: Dashboard e Visualização
- [ ] Configurar Grafana
- [ ] Desenvolver painéis para métricas-chave
- [ ] Implementar alertas para transações suspeitas

### Fase 5: Melhorias e Otimizações
- [ ] Refinar modelo de detecção de fraudes
- [ ] Otimizar performance do processamento
- [ ] Adicionar testes e documentação

## Como Executar

1. Clone o repositório
2. Execute `docker-compose up -d` para iniciar todos os serviços
3. Acesse o Grafana em `http://localhost:3000`
4. Use as credenciais padrão: admin/admin

## Próximos Passos

Após a implementação inicial, podemos expandir o projeto com:
- Processamento em lote para treinamento de modelos mais avançados
- Implementação de APIs REST para integração com outros sistemas
- Escalabilidade horizontal dos componentes