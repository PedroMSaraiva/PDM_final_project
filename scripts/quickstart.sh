#!/bin/bash
# =====================================================================
# Quick Start - Deploy Completo do Sistema de Ingestão
# =====================================================================

set -e

# Cores
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# =====================================================================
# BANNER
# =====================================================================
clear
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}                                                                        ${NC}"
echo -e "${BLUE}  🚀 QUICK START - Sistema de Ingestão Automática de Dados           ${NC}"
echo -e "${BLUE}                                                                        ${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

# =====================================================================
# 1. VERIFICAR CONFIGURAÇÕES
# =====================================================================
echo -e "${YELLOW}📋 Passo 1/5: Verificando configurações...${NC}"
echo ""

# Verificar se gcloud está instalado
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ Erro: gcloud CLI não está instalado${NC}"
    echo "Instale em: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Obter projeto atual
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}❌ Erro: Nenhum projeto GCP configurado${NC}"
    echo ""
    echo "Configure com:"
    echo "  gcloud config set project SEU-PROJETO-ID"
    exit 1
fi

echo -e "${GREEN}✅ gcloud CLI instalado${NC}"
echo "   Project ID: $PROJECT_ID"
echo ""

# Confirmar com usuário
echo -e "${YELLOW}⚠️  Este script vai:${NC}"
echo "   1. Habilitar APIs necessárias"
echo "   2. Fazer deploy de 5 Cloud Functions"
echo "   3. Fazer deploy do GCP Workflow"
echo "   4. Configurar 4-5 Cloud Schedulers"
echo ""
read -p "Deseja continuar? (s/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[SsYy]$ ]]; then
    echo -e "${YELLOW}Operação cancelada pelo usuário${NC}"
    exit 0
fi

echo ""

# =====================================================================
# 2. HABILITAR APIS
# =====================================================================
echo -e "${YELLOW}📋 Passo 2/5: Habilitando APIs...${NC}"
echo ""

gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    storage.googleapis.com \
    pubsub.googleapis.com \
    workflows.googleapis.com \
    cloudscheduler.googleapis.com \
    --project=$PROJECT_ID

echo -e "${GREEN}✅ APIs habilitadas${NC}"
echo ""

# =====================================================================
# 3. CRIAR BUCKET
# =====================================================================
echo -e "${YELLOW}📋 Passo 3/5: Verificando bucket de dados...${NC}"
echo ""

BUCKET_NAME="dados-cnpjs"
if gsutil ls -b gs://$BUCKET_NAME &> /dev/null; then
    echo -e "${GREEN}✅ Bucket $BUCKET_NAME já existe${NC}"
else
    echo "Criando bucket $BUCKET_NAME..."
    gsutil mb -l southamerica-east1 gs://$BUCKET_NAME
    echo -e "${GREEN}✅ Bucket criado${NC}"
fi
echo ""

# =====================================================================
# 4. DEPLOY CLOUD FUNCTIONS
# =====================================================================
echo -e "${YELLOW}📋 Passo 4/5: Deploy das Cloud Functions...${NC}"
echo ""

cd ..  # Voltar para o diretório raiz

REGION="southamerica-east1"

# 4.1 Receita Estabelecimentos
echo -e "${BLUE}📦 1/5 - Receita Estabelecimentos...${NC}"
gcloud functions deploy crawler-receita-estabelecimentos \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./Receita_estabelecimentos_CF \
    --entry-point=crawler_receita_pubsub \
    --trigger-topic=receita-estabelecimentos-download \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars DESTINATION_BUCKET_NAME=$BUCKET_NAME,BASE_PATH=receita_federal/estabelecimentos \
    --quiet

echo -e "${GREEN}✅ Estabelecimentos deployado${NC}"
echo ""

# 4.2 Receita Empresas
echo -e "${BLUE}📦 2/5 - Receita Empresas...${NC}"
gcloud functions deploy crawler-receita-empresas \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./Receita_empresas_CF \
    --entry-point=crawler_receita_pubsub \
    --trigger-topic=receita-empresas-download \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars DESTINATION_BUCKET_NAME=$BUCKET_NAME,BASE_PATH=receita_federal/empresas \
    --quiet

echo -e "${GREEN}✅ Empresas deployado${NC}"
echo ""

# 4.3 Receita Lucros
echo -e "${BLUE}📦 3/5 - Receita Lucros...${NC}"
gcloud functions deploy crawler-receita-lucros \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./Receita_lucros_CF \
    --entry-point=crawler_receita_pubsub \
    --trigger-topic=receita-lucros-download \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars DESTINATION_BUCKET_NAME=$BUCKET_NAME,BASE_PATH=receita_federal/regime_tributario \
    --quiet

echo -e "${GREEN}✅ Lucros deployado${NC}"
echo ""

# 4.4 Fazenda Nacional
echo -e "${BLUE}📦 4/5 - Fazenda Nacional...${NC}"
gcloud functions deploy download-fazenda-nacional \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./Fazenda_CF \
    --entry-point=download_fazenda_pubsub \
    --trigger-topic=fazenda-download \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars DESTINATION_BUCKET_NAME=$BUCKET_NAME,BASE_PATH=fazenda_nacional \
    --quiet

echo -e "${GREEN}✅ Fazenda deployado${NC}"
echo ""

# 4.5 Banco Central
echo -e "${BLUE}📦 5/5 - Banco Central...${NC}"
gcloud functions deploy banco-central-indicadores \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./Banco_Central_CF \
    --entry-point=banco_central_pubsub \
    --trigger-topic=banco-central-indicadores \
    --timeout=300s \
    --memory=4Gi \
    --max-instances=1 \
    --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET_ID=main_database \
    --quiet

echo -e "${GREEN}✅ Banco Central deployado${NC}"
echo ""

# =====================================================================
# 5. DEPLOY WORKFLOW E SCHEDULERS
# =====================================================================
echo -e "${YELLOW}📋 Passo 5/5: Deploy do Workflow e Schedulers...${NC}"
echo ""

cd workflows/

# Deploy do workflow
echo -e "${BLUE}🔄 Deploy do Workflow...${NC}"
./deploy-workflow.sh

echo ""

# Setup schedulers
echo -e "${BLUE}📅 Configuração dos Schedulers...${NC}"
./setup-schedulers.sh

echo ""

# =====================================================================
# FINALIZAÇÃO
# =====================================================================
echo -e "${GREEN}========================================================================${NC}"
echo -e "${GREEN}                                                                        ${NC}"
echo -e "${GREEN}  ✅ DEPLOY COMPLETO - Sistema está pronto!                           ${NC}"
echo -e "${GREEN}                                                                        ${NC}"
echo -e "${GREEN}========================================================================${NC}"
echo ""
echo -e "${BLUE}📊 Resumo do Deployment:${NC}"
echo ""
echo "✅ 5 Cloud Functions deployadas"
echo "✅ 1 GCP Workflow configurado"
echo "✅ 4-5 Cloud Schedulers ativos"
echo "✅ 1 Bucket GCS criado/verificado"
echo ""
echo -e "${BLUE}📅 Agendamentos Ativos:${NC}"
echo ""
echo "  📊 Dia 05: Banco Central"
echo "  📊 Dia 10: Receita Federal (Estabelecimentos + Empresas)"
echo "  🏛️ Dia 15: Fazenda Nacional (Jan, Abr, Jul, Out)"
echo "  💰 15 Fev: Receita Lucros (Anual)"
echo ""
echo -e "${BLUE}💻 Próximos Passos:${NC}"
echo ""
echo "1. Teste manual do workflow:"
echo "   gcloud workflows execute data-ingestion-workflow \\"
echo "     --location=$REGION \\"
echo "     --data='{\"type\": \"receita_mensal\"}'"
echo ""
echo "2. Monitore as execuções:"
echo "   gcloud workflows executions list data-ingestion-workflow \\"
echo "     --location=$REGION"
echo ""
echo "3. Ver logs:"
echo "   gcloud functions logs read banco-central-indicadores \\"
echo "     --gen2 --region=$REGION --limit=50"
echo ""
echo "4. Consulte a documentação completa:"
echo "   cat workflows/README.md"
echo ""
echo -e "${GREEN}🎉 Tudo pronto! Seu sistema de ingestão está operacional!${NC}"
echo ""

