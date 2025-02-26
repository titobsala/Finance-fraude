#!/bin/bash

# Cores
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO] ${NC}$1"; }
error() { echo -e "${RED}[ERROR] ${NC}$1"; exit 1; }

check_dependencies() {
  command -v docker >/dev/null || error "Docker não instalado"
  command -v docker compose >/dev/null || error "Docker Compose não instalado"
}

create_dirs() {
  mkdir -p data/{raw,processed,influxdb} logs models
  # Create spark checkpoint directory in the local filesystem
  mkdir -p ./data/spark-checkpoint
  chmod -R 777 ./data/spark-checkpoint
  log "Diretórios criados"
}

main() {
  check_dependencies
  create_dirs
  
  log "Construindo containers..."
  docker compose -f docker/docker-compose.yml build || error "Build falhou"

  log "Iniciando serviços..."
  docker compose -f docker/docker-compose.yml up -d || error "Start falhou"

  log "Aguardando inicialização (30s)..."
  sleep 30

  log "Sistema pronto!"
  echo -e "• Spark UI:   http://localhost:8080"
  echo -e "• InfluxDB:   http://localhost:8086"
  echo -e "• Kafka:      localhost:9092"
}

main