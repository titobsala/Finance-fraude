#!/bin/bash

# Script para iniciar todos os componentes do sistema

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

# Verificar dependências
check_dependencies() {
  log "Verificando dependências..."
  
  if ! command -v docker &> /dev/null; then
    error "Docker não encontrado. Por favor, instale o Docker."
    exit 1
  fi
  
  if ! command -v docker-compose &> /dev/null; then
    error "Docker Compose não encontrado. Por favor, instale o Docker Compose."
    exit 1
  fi
  
  log "Todas as dependências estão instaladas."
}

# Criar diretórios necessários
create_directories() {
  log "Criando diretórios necessários..."
  
  mkdir -p data/raw
  mkdir -p data/processed
  mkdir -p logs
  mkdir -p models
  
  log "Diretórios criados com sucesso."
}

# Construir imagens Docker
build_images() {
  log "Construindo imagens Docker..."
  
  cd docker
  DOCKER_BUILDKIT=1 docker-compose build --progress=plain
  
  if [ $? -ne 0 ]; then
    error "Falha ao construir imagens Docker."
    exit 1
  fi
  
  log "Imagens Docker construídas com sucesso."
}

# Iniciar os containers
start_containers() {
  log "Iniciando containers..."
  
   docker || { error "Diretório docker não encontrado!"; exit 1; }
  docker-compose up -d
  
  if [ $? -ne 0 ]; then
    error "Falha ao iniciar containers."
    exit 1
  fi
  
  log "Containers iniciados com sucesso."
  cd ..
}

# Verificar status dos containers
check_status() {
  log "Verificando status dos containers..."
  
  if [ -d "docker" ]; then
    cd docker
    docker-compose ps
    cd ..
  else
    # Se já estivermos no diretório docker
    docker-compose ps
  fi
  
  log "Verificação de status concluída."
}

# Função principal
main() {
  log "Iniciando sistema de detecção de fraudes..."
  
  check_dependencies
  create_directories
  build_images
  start_containers
  check_status
  
  log "Sistema iniciado e pronto para uso!"
  log "Dashboard Grafana disponível em: http://localhost:3000"
  log "Usuário: admin | Senha: admin"
}

# Execução do script
main