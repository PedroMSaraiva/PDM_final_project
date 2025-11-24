#!/bin/bash
# =====================================================================
# Configuração dos Cloud Schedulers para Ingestão Automática de Dados
# =====================================================================

set -e  # Para na primeira falha

# Cores para output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# =====================================================================
# CONFIGURAÇÕES
# =====================================================================
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"trabalho-final-pdm-478021"}
REGION="southamerica-east1"
WORKFLOW_NAME="data-ingestion-workflow"
TIMEZONE="America/Sao_Paulo"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  📅 Configurando Cloud Schedulers - Ingestão Automática${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

# =====================================================================
# FUNÇÃO AUXILIAR: Criar ou atualizar scheduler
# =====================================================================
create_or_update_scheduler() {
    local JOB_NAME=$1
    local SCHEDULE=$2
    local DESCRIPTION=$3
    local WORKFLOW_ARGS=$4
    
    echo -e "${YELLOW}📌 Configurando: $JOB_NAME${NC}"
    echo "   Schedule: $SCHEDULE"
    echo "   Description: $DESCRIPTION"
    
    # Tentar deletar job existente (ignora erro se não existir)
    gcloud scheduler jobs delete $JOB_NAME \
        --location=$REGION \
        --project=$PROJECT_ID \
        --quiet 2>/dev/null || true
    
    # Criar novo job
    gcloud scheduler jobs create http $JOB_NAME \
        --location=$REGION \
        --schedule="$SCHEDULE" \
        --time-zone="$TIMEZONE" \
        --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions" \
        --message-body="$WORKFLOW_ARGS" \
        --oauth-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
        --description="$DESCRIPTION" \
        --project=$PROJECT_ID
    
    echo -e "${GREEN}✅ $JOB_NAME configurado${NC}"
    echo ""
}

# =====================================================================
# 1. RECEITA FEDERAL - MENSAL (Estabelecimentos + Empresas)
# Segunda semana de cada mês (dia 10)
# =====================================================================
echo -e "${BLUE}📊 1/5 - Receita Federal Mensal (Estabelecimentos + Empresas)${NC}"
create_or_update_scheduler \
    "receita-mensal-ingestion" \
    "0 2 10 * *" \
    "Coleta mensal de Estabelecimentos e Empresas da Receita Federal - Segunda semana do mês" \
    '{"argument": "{\"type\": \"receita_mensal\"}"}'

# =====================================================================
# 2. RECEITA FEDERAL - LUCROS (Anual)
# Dia 15 de fevereiro
# =====================================================================
echo -e "${BLUE}💰 2/5 - Receita Federal Lucros (Anual)${NC}"
create_or_update_scheduler \
    "receita-lucros-anual-ingestion" \
    "0 2 15 2 *" \
    "Coleta anual de Regime Tributário (Lucros) da Receita Federal - Fevereiro" \
    '{"argument": "{\"type\": \"receita_lucros_anual\"}"}'

# =====================================================================
# 3. FAZENDA NACIONAL - TRIMESTRAL
# Dia 15 do primeiro mês de cada trimestre (Jan, Abr, Jul, Out)
# =====================================================================
echo -e "${BLUE}🏛️ 3/5 - Fazenda Nacional (Trimestral)${NC}"
create_or_update_scheduler \
    "fazenda-trimestral-ingestion" \
    "0 2 15 1,4,7,10 *" \
    "Coleta trimestral da Fazenda Nacional - Primeiro mês de cada trimestre" \
    '{"argument": "{\"type\": \"fazenda_trimestral\"}"}'

# =====================================================================
# 4. BANCO CENTRAL - MENSAL
# Dia 5 de cada mês
# =====================================================================
echo -e "${BLUE}🏦 4/5 - Banco Central (Mensal)${NC}"
create_or_update_scheduler \
    "banco-central-mensal-ingestion" \
    "0 2 5 * *" \
    "Coleta mensal de indicadores econômicos do Banco Central" \
    '{"argument": "{\"type\": \"banco_central\"}"}'

# =====================================================================
# 5. INGESTÃO COMPLETA - SEMANAL (Opcional - para backup/validação)
# Todo domingo às 3h da manhã
# =====================================================================
echo -e "${BLUE}🔄 5/5 - Ingestão Completa Semanal (Backup)${NC}"
read -p "Deseja criar um job de backup semanal completo? (s/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[SsYy]$ ]]; then
    create_or_update_scheduler \
        "data-ingestion-full-backup" \
        "0 3 * * 0" \
        "Ingestão completa semanal (backup) - Todo domingo" \
        '{"argument": "{\"type\": \"full\"}"}'
else
    echo -e "${YELLOW}⏭️  Job de backup semanal não foi criado${NC}"
    echo ""
fi

# =====================================================================
# 6. LISTAR SCHEDULERS CRIADOS
# =====================================================================
echo -e "${BLUE}========================================================================${NC}"
echo -e "${GREEN}  ✅ Cloud Schedulers configurados com sucesso!${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""
echo -e "${YELLOW}📋 Schedulers ativos:${NC}"
echo ""

gcloud scheduler jobs list \
    --location=$REGION \
    --project=$PROJECT_ID

echo ""
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  📅 Calendário de Execuções${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""
echo "📊 Receita Federal (Estabelecimentos + Empresas)"
echo "   ➜ Dia 10 de cada mês às 02:00 (segunda semana)"
echo ""
echo "💰 Receita Federal (Lucros)"
echo "   ➜ Dia 15 de fevereiro às 02:00 (anual)"
echo ""
echo "🏛️ Fazenda Nacional"
echo "   ➜ Dia 15 de janeiro, abril, julho, outubro às 02:00 (trimestral)"
echo ""
echo "🏦 Banco Central"
echo "   ➜ Dia 5 de cada mês às 02:00 (mensal)"
echo ""
if [[ $REPLY =~ ^[SsYy]$ ]]; then
    echo "🔄 Backup Completo"
    echo "   ➜ Todo domingo às 03:00 (semanal)"
    echo ""
fi
echo -e "${BLUE}========================================================================${NC}"
echo ""
echo -e "${YELLOW}💡 Comandos úteis:${NC}"
echo ""
echo "# Pausar um scheduler:"
echo "gcloud scheduler jobs pause JOB_NAME --location=$REGION"
echo ""
echo "# Retomar um scheduler:"
echo "gcloud scheduler jobs resume JOB_NAME --location=$REGION"
echo ""
echo "# Executar manualmente (teste):"
echo "gcloud scheduler jobs run receita-mensal-ingestion --location=$REGION"
echo ""
echo "# Ver logs de execução:"
echo "gcloud workflows executions list $WORKFLOW_NAME --location=$REGION"
echo ""
echo "# Deletar um scheduler:"
echo "gcloud scheduler jobs delete JOB_NAME --location=$REGION"
echo ""

