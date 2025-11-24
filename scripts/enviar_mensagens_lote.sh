#!/bin/bash
# Script para enviar TODAS as mensagens de uma vez (sem delay)
# ATENÇÃO: Isso enviará ~360 mensagens simultaneamente!
# Período: 2023-01 até 2025-12 (36 meses x 10 arquivos = 360 mensagens)
# As Cloud Functions irão processar em paralelo (se max-instances > 1)

echo "======================================================================"
echo "  ENVIAR TODAS AS MENSAGENS DE UMA VEZ"
echo "======================================================================"
echo ""
echo "⚠️  ATENÇÃO: Isso enviará ~360 mensagens simultaneamente!"
echo ""
echo "📊 Período: 2023-01 até 2025-12 (36 meses)"
echo "📦 Arquivos por mês: 10 (Estabelecimentos0-9)"
echo "📨 Total de mensagens: ~360"
echo ""
echo "Opções de processamento:"
echo "  1) max-instances=1  → Processa 1 arquivo por vez (sequencial)"
echo "  2) max-instances=10 → Processa até 10 arquivos em paralelo"
echo ""
echo "⏱️  Tempo estimado:"
echo "  - max-instances=1:  ~5-7 dias"
echo "  - max-instances=5:  ~1-2 dias"
echo "  - max-instances=10: ~12-24 horas"
echo ""
echo "Configure no deploy: --max-instances=N"
echo ""
read -p "Deseja continuar? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelado"
    exit 0
fi

echo ""
echo "🚀 Enviando mensagens..."
echo ""

TOPIC="receita-federal-download"
TOTAL=0

# Gerar todas as pastas de 2023 até 2025 (todos os meses)
FOLDERS=()
for YEAR in {2023..2025}; do
    for MONTH in {01..12}; do
        FOLDERS+=("${YEAR}-${MONTH}")
    done
done

echo "📋 Total de pastas a processar: ${#FOLDERS[@]}"
echo ""

for FOLDER in "${FOLDERS[@]}"; do
    echo "📁 Pasta: $FOLDER"
    
    # 10 arquivos por pasta
    #for i in {0..9}; do
    FILE="Estabelecimentos0.zip"
    MESSAGE="{\"folder\": \"$FOLDER\", \"file\": \"$FILE\"}"
        
    gcloud pubsub topics publish "$TOPIC" --message="$MESSAGE" &
        
    TOTAL=$((TOTAL + 1))
    echo "  ✅ $FILE (mensagem $TOTAL)"
    #done
    
    echo ""
done

# Aguardar todos os comandos em background
wait

echo ""
echo "======================================================================"
echo "  TODAS AS MENSAGENS ENVIADAS!"
echo "======================================================================"
echo ""
echo "Total de mensagens: $TOTAL"
echo ""
echo "⚠️  As Cloud Functions irão processar conforme a configuração:"
echo "  - Se max-instances=1: processará 1 por vez (sequencial)"
echo "  - Se max-instances=10: processará até 10 em paralelo"
echo ""
echo "Monitorar:"
echo "  gcloud functions logs tail crawler-receita-federal --gen2 --region=southamerica-east1"
echo ""
echo "Verificar status:"
echo "  gsutil ls -r gs://dados-cnpjs/receita_federal/ | grep .extracted"
echo ""

