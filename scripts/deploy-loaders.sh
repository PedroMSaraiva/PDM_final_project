#!/bin/bash
# Script para deploy automatizado dos loaders do BigQuery como Cloud Functions

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configurações
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-trabalho-final-pdm-478021}"
REGION="${REGION:-us-central1}"
DATASET_ID="${DATASET_ID:-main_database}"
BUCKET_NAME="${BUCKET_NAME:-dados-cnpjs}"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  Deploy dos Loaders BigQuery como Cloud Functions                    ${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

# Verificar se está autenticado
echo -e "${BLUE}Verificando autenticação...${NC}"
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo -e "${RED}Erro: Não há contas autenticadas. Execute: gcloud auth login${NC}"
    exit 1
fi

# Configurar projeto
echo -e "${BLUE}Configurando projeto: ${PROJECT_ID}${NC}"
gcloud config set project "$PROJECT_ID"

# Habilitar APIs necessárias
echo -e "${BLUE}Habilitando APIs...${NC}"
gcloud services enable cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    --quiet

# Criar tópicos Pub/Sub
echo -e "${BLUE}Criando tópicos Pub/Sub...${NC}"

# Tópico para loader Fazenda
if ! gcloud pubsub topics describe bigquery-loader-fazenda --project="$PROJECT_ID" &>/dev/null; then
    gcloud pubsub topics create bigquery-loader-fazenda --project="$PROJECT_ID"
    echo -e "${GREEN}✅ Tópico 'bigquery-loader-fazenda' criado${NC}"
else
    echo -e "${YELLOW}ℹ️  Tópico 'bigquery-loader-fazenda' já existe${NC}"
fi

# Tópico para loader Receita
if ! gcloud pubsub topics describe bigquery-loader-receita --project="$PROJECT_ID" &>/dev/null; then
    gcloud pubsub topics create bigquery-loader-receita --project="$PROJECT_ID"
    echo -e "${GREEN}✅ Tópico 'bigquery-loader-receita' criado${NC}"
else
    echo -e "${YELLOW}ℹ️  Tópico 'bigquery-loader-receita' já existe${NC}"
fi

echo ""

# Voltar para diretório raiz do projeto
cd "$(dirname "$0")/.."

# =====================================================================
# 1. DEPLOY LOADER FAZENDA
# =====================================================================
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  1/2 - Deploy Loader Fazenda Nacional (BigQuery)                    ${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

gcloud functions deploy bigquery-loader-fazenda \
    --gen2 \
    --runtime=python311 \
    --region="$REGION" \
    --source=./BigQuery_loader_fazenda_CF \
    --entry-point=load_fazenda_bigquery \
    --trigger-topic=bigquery-loader-fazenda \
    --timeout=3600s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="$DATASET_ID",BUCKET_NAME="$BUCKET_NAME",BASE_PATH=fazenda_nacional \
    --quiet

echo -e "${GREEN}✅ Loader Fazenda deployado${NC}"
echo ""

# =====================================================================
# 2. DEPLOY LOADER RECEITA
# =====================================================================
echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  2/2 - Deploy Loader Receita Federal (BigQuery)                      ${NC}"
echo -e "${BLUE}========================================================================${NC}"
echo ""

gcloud functions deploy bigquery-loader-receita \
    --gen2 \
    --runtime=python311 \
    --region="$REGION" \
    --source=./BigQuery_loader_receita_CF \
    --entry-point=load_receita_bigquery \
    --trigger-topic=bigquery-loader-receita \
    --timeout=3600s \
    --memory=4Gi \
    --max-instances=1 \
    --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="$DATASET_ID",TABLE_NAME=receita_estabelecimentos,BUCKET_NAME="$BUCKET_NAME",BASE_PATH=receita_federal \
    --quiet

echo -e "${GREEN}✅ Loader Receita deployado${NC}"
echo ""

# =====================================================================
# FINALIZAÇÃO
# =====================================================================
echo -e "${GREEN}========================================================================${NC}"
echo -e "${GREEN}                                                                        ${NC}"
echo -e "${GREEN}  ✅ DEPLOY COMPLETO - Loaders BigQuery prontos!                      ${NC}"
echo -e "${GREEN}                                                                        ${NC}"
echo -e "${GREEN}========================================================================${NC}"
echo ""

echo -e "${BLUE}📋 Resumo:${NC}"
echo -e "  • Loader Fazenda: bigquery-loader-fazenda"
echo -e "  • Loader Receita: bigquery-loader-receita"
echo -e "  • Região: $REGION"
echo -e "  • Projeto: $PROJECT_ID"
echo ""

echo -e "${BLUE}🚀 Para testar:${NC}"
echo -e "  # Loader Fazenda (carrega todos os dados)"
echo -e "  gcloud pubsub topics publish bigquery-loader-fazenda --message='{}'"
echo ""
echo -e "  # Loader Receita (carrega todos os períodos)"
echo -e "  gcloud pubsub topics publish bigquery-loader-receita --message='{}'"
echo ""
echo -e "  # Loader Receita (carrega período específico)"
echo -e "  gcloud pubsub topics publish bigquery-loader-receita --message='{\"period\": \"2024-03\"}'"
echo ""

