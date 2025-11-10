#!/bin/bash

# Script para testar conectividade do Docker com o servidor da Receita Federal
# Autor: AI Assistant
# Data: 2025-11-10

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         🔍 TESTE DE CONECTIVIDADE - RECEITA FEDERAL          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

echo "1️⃣  Testando conectividade básica..."
echo ""

# Teste 1: Ping para Google (teste geral de internet)
echo "   📡 Testando conectividade com a internet (Google DNS)..."
if ping -c 2 8.8.8.8 > /dev/null 2>&1; then
    echo "   ✅ Internet OK"
else
    echo "   ❌ Sem conexão com a internet"
    exit 1
fi
echo ""

# Teste 2: Curl direto para o servidor
echo "   📡 Testando acesso direto ao servidor da Receita Federal..."
echo "   URL: http://200.152.38.155/CNPJ/"
echo ""
curl -I --connect-timeout 10 --max-time 30 http://200.152.38.155/CNPJ/ 2>&1 | head -5
CURL_EXIT=$?

if [ $CURL_EXIT -eq 0 ]; then
    echo ""
    echo "   ✅ Servidor acessível via curl"
else
    echo ""
    echo "   ⚠️  Curl falhou com código: $CURL_EXIT"
fi
echo ""

# Teste 3: Testar dentro do container do Airflow
echo "2️⃣  Testando conectividade DENTRO do container do Airflow..."
echo ""

if docker ps | grep -q "airflow-scheduler"; then
    echo "   Container encontrado. Testando..."
    echo ""
    
    # Teste de ping
    echo "   📡 Ping para 8.8.8.8 (dentro do container):"
    docker exec airflow-scheduler ping -c 2 8.8.8.8 2>&1 | grep "packets transmitted"
    
    echo ""
    echo "   📡 Acesso HTTP ao servidor da Receita Federal (dentro do container):"
    docker exec airflow-scheduler python3 -c "
import requests
import sys

try:
    print('   Tentando conectar...')
    response = requests.get('http://200.152.38.155/CNPJ/', timeout=30)
    print(f'   ✅ Status: {response.status_code}')
    print(f'   ✅ Tamanho da resposta: {len(response.content)} bytes')
    print(f'   ✅ Servidor ACESSÍVEL!')
    sys.exit(0)
except requests.exceptions.Timeout:
    print('   ❌ TIMEOUT - Servidor não respondeu em 30 segundos')
    print('   Possíveis causas:')
    print('      - Servidor da Receita Federal sobrecarregado')
    print('      - Firewall bloqueando conexões do Docker')
    print('      - Problemas de rede')
    sys.exit(1)
except requests.exceptions.ConnectionError as e:
    print(f'   ❌ ERRO DE CONEXÃO: {str(e)[:200]}')
    print('   Possíveis causas:')
    print('      - Container sem acesso à internet')
    print('      - DNS não resolvendo')
    print('      - Firewall bloqueando')
    sys.exit(1)
except Exception as e:
    print(f'   ❌ ERRO: {e}')
    sys.exit(1)
" 2>&1
    
    CONTAINER_TEST=$?
    
    echo ""
    if [ $CONTAINER_TEST -eq 0 ]; then
        echo "   ✅ Container tem acesso ao servidor!"
    else
        echo "   ❌ Container NÃO consegue acessar o servidor"
    fi
else
    echo "   ⚠️  Container airflow-scheduler não está rodando"
    echo "   Execute: docker compose up -d"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                     📋 DIAGNÓSTICO                             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

if [ $CONTAINER_TEST -eq 0 ]; then
    echo "✅ TUDO OK! O servidor está acessível."
    echo ""
    echo "Se o DAG ainda estiver falhando, tente:"
    echo "  1. Reiniciar o Airflow: docker compose restart"
    echo "  2. Aguardar alguns minutos (servidor pode estar lento)"
    echo "  3. Executar a DAG novamente"
else
    echo "⚠️  PROBLEMA IDENTIFICADO!"
    echo ""
    echo "🔧 Soluções possíveis:"
    echo ""
    echo "1. Verificar se o Docker tem acesso à internet:"
    echo "   docker run --rm alpine ping -c 2 google.com"
    echo ""
    echo "2. Reiniciar o Docker:"
    echo "   sudo systemctl restart docker"
    echo ""
    echo "3. Verificar configuração de rede do Docker Compose:"
    echo "   Adicionar 'network_mode: bridge' ao docker-compose.yml"
    echo ""
    echo "4. Tentar novamente mais tarde:"
    echo "   O servidor da Receita Federal pode estar temporariamente indisponível"
    echo ""
fi

