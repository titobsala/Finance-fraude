#!/bin/bash

# Script para parar todos os componentes do sistema

# Cores para saída
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Função para exibir mensagens
log() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

# Parar todos os serviços
stop_services() {
  log "Parando todos os serviços..."
  
  cd docker
  docker-compose down
  
  if [ $? -ne 0 ]; then
    error "Falha ao parar alguns serviços."
    exit 1
  fi
  
  log "Todos os serviços parados com sucesso."
}

# Função principal
main() {
  log "Parando sistema de detecção de fraudes..."
  
  stop_services
  
  log "Sistema parado com sucesso."
}

# Execução do script
main