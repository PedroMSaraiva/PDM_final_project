#!/bin/bash
# =====================================================================
# Deploy do GCP Workflow para Ingestão de Dados
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
REGION="us-central1"
WORKFLOW_NAME="data-ingestion-workflow"
WORKFLOW_FILE="data-ingestion-workflow.yaml"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  🚀 Deploy GCP Workflow - Ingestão de Dados${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

# =====================================================================
# 1. VERIFICAR CONFIGURAÇÕES
# =====================================================================
echo -e "${YELLOW}📋 Verificando configurações...${NC}"
echo "   Project ID: $PROJECT_ID"
echo "   Region: $REGION"
echo "   Workflow: $WORKFLOW_NAME"
echo ""

# =====================================================================
# 2. HABILITAR APIs NECESSÁRIAS
# =====================================================================
echo -e "${YELLOW}🔧 Habilitando APIs necessárias...${NC}"
gcloud services enable workflows.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudfunctions.googleapis.com \
  --project=$PROJECT_ID

echo -e "${GREEN}✅ APIs habilitadas${NC}"
echo ""

# =====================================================================
# 3. FAZER DEPLOY DO WORKFLOW
# =====================================================================
echo -e "${YELLOW}📦 Fazendo deploy do workflow...${NC}"

# Verificar se o arquivo existe
if [ ! -f "$WORKFLOW_FILE" ]; then
    echo -e "${RED}❌ Erro: Arquivo $WORKFLOW_FILE não encontrado!${NC}"
    exit 1
fi

# Deploy ou atualização do workflow
gcloud workflows deploy $WORKFLOW_NAME \
  --source=$WORKFLOW_FILE \
  --location=$REGION \
  --service-account=$SERVICE_ACCOUNT \
  --project=$PROJECT_ID

echo -e "${GREEN}✅ Workflow deployado com sucesso!${NC}"
echo ""

# =====================================================================
# 4. VERIFICAR WORKFLOW
# =====================================================================
echo -e "${YELLOW}🔍 Verificando workflow...${NC}"
gcloud workflows describe $WORKFLOW_NAME \
  --location=$REGION \
  --project=$PROJECT_ID

echo ""
echo -e "${GREEN}========================================================================${NC}"
echo -e "${GREEN}  ✅ Deploy concluído com sucesso!${NC}"
echo -e "${GREEN}========================================================================${NC}"
echo ""
echo -e "${BLUE}📝 Próximos passos:${NC}"
echo ""
echo "1. Configure os Cloud Schedulers:"
echo "   ./setup-schedulers.sh"
echo ""
echo "2. Teste o workflow manualmente:"
echo "   gcloud workflows execute $WORKFLOW_NAME \\"
echo "     --location=$REGION \\"
echo "     --data='{\"type\": \"receita_mensal\"}'"
echo ""
echo "3. Monitore as execuções:"
echo "   gcloud workflows executions list $WORKFLOW_NAME \\"
echo "     --location=$REGION"
echo ""

