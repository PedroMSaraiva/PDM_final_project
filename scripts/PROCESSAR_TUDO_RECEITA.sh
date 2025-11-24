#!/bin/bash
# Script para processar TODOS os arquivos da Receita Federal
# Uso: ./PROCESSAR_TUDO_RECEITA.sh

set -e

echo "======================================================================"
echo "  PROCESSAR TODOS OS ARQUIVOS DA RECEITA FEDERAL"
echo "======================================================================"
echo ""
echo "Este script irá processar TODOS os arquivos de TODAS as pastas"
echo "disponíveis na Receita Federal, um arquivo por vez."
echo ""
echo "📊 Período: 2023-01 até 2025-12 (36 meses)"
echo "📦 Arquivos por mês: 10 (Estabelecimentos0-9)"
echo "📨 Total: ~360 arquivos"
echo ""
echo "⏱️  Tempo estimado: 5-7 dias (processamento sequencial)"
echo "💾 Espaço necessário: ~1-2 TB no bucket"
echo ""
read -p "Deseja continuar? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelado"
    exit 0
fi

echo ""
echo "🚀 Iniciando processamento automatizado..."
echo ""

# Executar script Python
python processar_receita_batch.py --all-folders --delay 10

echo ""
echo "======================================================================"
echo "  PROCESSAMENTO CONCLUÍDO!"
echo "======================================================================"
echo ""
echo "Verifique os arquivos no bucket:"
echo "  gsutil ls -r gs://dados-cnpjs/receita_federal/"
echo ""
echo "Ver logs completos:"
echo "  gcloud functions logs read crawler-receita-federal --gen2 --region=southamerica-east1 --limit=1000"
echo ""

