#!/bin/bash

# Script para aplicar a correção da DAG
# Autor: AI Assistant
# Data: 2025-11-10

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         🔧 APLICANDO CORREÇÃO DA DAG DO AIRFLOW               ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Verificar se está no diretório correto
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Erro: docker-compose.yml não encontrado!"
    echo "Execute este script a partir do diretório:"
    echo "   Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master/"
    exit 1
fi

echo "🔍 Verificando Docker Compose..."
if ! command -v docker compose &> /dev/null; then
    echo "❌ Docker Compose não está instalado!"
    exit 1
fi
echo "   ✓ Docker Compose encontrado"

echo ""
echo "⏸️  Parando serviços do Airflow..."
docker compose down
echo "   ✓ Serviços parados"

echo ""
echo "🚀 Iniciando serviços novamente..."
docker compose up -d
echo "   ✓ Serviços iniciados"

echo ""
echo "⏳ Aguardando inicialização (15 segundos)..."
sleep 15

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    ✅ CORREÇÃO APLICADA!                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "🌐 Acesse o Airflow:"
echo "   URL: http://localhost:8080"
echo "   Usuário: airflow"
echo "   Senha: airflow"
echo ""
echo "📋 Próximos passos:"
echo "   1. Acesse http://localhost:8080"
echo "   2. Procure a DAG 'etl_receita_federal'"
echo "   3. Clique no botão ▶️ para executar"
echo "   4. Acompanhe os logs"
echo ""
echo "👀 Ver logs em tempo real:"
echo "   docker compose logs -f airflow-scheduler"
echo ""
echo "⏱️  Tempo estimado de execução: 4-8 horas"
echo ""

