#!/bin/bash

# Script de inicialização rápida do ETL Receita Federal com Airflow
# Autor: Sistema automatizado
# Uso: ./quick-start.sh

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  ETL Receita Federal - Setup Rápido com Airflow & PostgreSQL  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Verificar se Docker está instalado
if ! command -v docker &> /dev/null; then
    echo "❌ Docker não está instalado!"
    echo "Instale o Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Verificar se Docker Compose está instalado
if ! command -v docker compose &> /dev/null; then
    echo "❌ Docker Compose não está instalado!"
    echo "Instale o Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✓ Docker e Docker Compose encontrados"
echo ""

# Criar diretórios
echo "📦 Criando diretórios..."
mkdir -p dags logs plugins data/downloads data/extracted config

# Criar arquivo .env
if [ ! -f .env ]; then
    echo "📝 Criando arquivo .env..."
    cat > .env << EOF
# Configurações do Docker Compose
AIRFLOW_UID=$(id -u)
AIRFLOW_PROJ_DIR=.

# Credenciais do Airflow Web UI
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Bibliotecas Python adicionais
_PIP_ADDITIONAL_REQUIREMENTS=beautifulsoup4>=4.9.3 bs4>=0.0.1 lxml>=4.6.3 numpy>=1.20.3 pandas>=1.2.4 psycopg2-binary>=2.9.1 python-dotenv==1.0.0 requests==2.30.0 SQLAlchemy>=1.4.18 wget>=3.2

# Configurações do ETL
OUTPUT_FILES_PATH=/opt/airflow/data/downloads
EXTRACTED_FILES_PATH=/opt/airflow/data/extracted

# Configurações do PostgreSQL - Dados RFB
DB_HOST=postgres-dados-rfb
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=Dados_RFB
EOF
    echo "✓ Arquivo .env criado"
else
    echo "⚠️  Arquivo .env já existe, pulando..."
fi

echo ""
echo "🚀 Inicializando Airflow..."
echo "   (Isso pode levar alguns minutos na primeira vez)"
echo ""

# Inicializar Airflow
docker compose up airflow-init

echo ""
echo "🚀 Iniciando todos os serviços..."
docker compose up -d

echo ""
echo "⏳ Aguardando serviços ficarem prontos..."
sleep 10

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    ✓ SETUP CONCLUÍDO!                         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "🌐 Interfaces disponíveis:"
echo ""
echo "  📊 Airflow Web UI:  http://localhost:8080"
echo "     Usuário: airflow"
echo "     Senha:   airflow"
echo ""
echo "  🐘 PgAdmin:         http://localhost:5050"
echo "     Email: admin@admin.com"
echo "     Senha: admin"
echo ""
echo "  🗄️  PostgreSQL:      localhost:5432"
echo "     Database: Dados_RFB"
echo "     User: postgres"
echo "     Password: postgres"
echo ""
echo "📋 Próximos passos:"
echo ""
echo "  1. Acesse o Airflow em http://localhost:8080"
echo "  2. Faça login com: airflow / airflow"
echo "  3. Procure a DAG 'etl_receita_federal'"
echo "  4. Ative a DAG (toggle à esquerda)"
echo "  5. Execute clicando no botão Play ▶️"
echo ""
echo "⏱️  ATENÇÃO: O ETL completo pode levar 4-8 horas!"
echo ""
echo "📚 Comandos úteis:"
echo ""
echo "  docker compose logs -f              # Ver logs"
echo "  docker compose ps                   # Status dos serviços"
echo "  docker compose down                 # Parar tudo"
echo "  docker compose restart              # Reiniciar"
echo ""
echo "📖 Para mais informações, consulte README-DOCKER.md"
echo ""

