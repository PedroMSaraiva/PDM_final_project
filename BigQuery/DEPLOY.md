# 🚀 Deploy Automatizado dos Loaders BigQuery

Guia completo para deploy dos loaders do BigQuery como Cloud Functions na GCP.

---

## 📋 Pré-requisitos

```bash
# 1. Autenticar e configurar projeto
gcloud auth login
gcloud config set project SEU-PROJETO-ID

# 2. Habilitar APIs (feito automaticamente pelo script)
gcloud services enable cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com
```

---

## ⚡ Deploy Rápido (Recomendado)

```bash
cd BigQuery/
chmod +x deploy-loaders.sh
./deploy-loaders.sh
```

Este script vai:

- ✅ Verificar autenticação
- ✅ Habilitar APIs necessárias
- ✅ Criar tópicos Pub/Sub
- ✅ Fazer deploy das 2 Cloud Functions
- ✅ Configurar variáveis de ambiente

**Tempo estimado:** 5-10 minutos

---

## 📦 Cloud Functions Criadas

### 1. **bigquery-loader-fazenda**

Carrega dados da Fazenda Nacional (PGFN) do GCS para o BigQuery.

**Tópico Pub/Sub:** `bigquery-loader-fazenda`**Tabelas criadas:**

- `pgfn_nao_previdenciario`
- `pgfn_fgts`
- `pgfn_previdenciario`

**Configurações:**

- Runtime: Python 3.11
- Timeout: 3600s (1 hora)
- Memory: 2Gi
- Max instances: 1

### 2. **bigquery-loader-receita**

Carrega dados da Receita Federal (Estabelecimentos) do GCS para o BigQuery.

**Tópico Pub/Sub:** `bigquery-loader-receita`**Tabela criada:**

- `receita_estabelecimentos`

**Configurações:**

- Runtime: Python 3.11
- Timeout: 3600s (1 hora)
- Memory: 4Gi
- Max instances: 1

---

## 🎯 Como Usar

### **Loader Fazenda Nacional**

#### Carregar todos os dados (WRITE_TRUNCATE):

```bash
gcloud pubsub topics publish bigquery-loader-fazenda --message='{}'
```

#### Adicionar dados sem apagar existentes (WRITE_APPEND):

```bash
gcloud pubsub topics publish bigquery-loader-fazenda \
  --message='{"mode": "WRITE_APPEND"}'
```

#### Substituir dados existentes (WRITE_TRUNCATE):

```bash
gcloud pubsub topics publish bigquery-loader-fazenda \
  --message='{"mode": "WRITE_TRUNCATE"}'
```

---

### **Loader Receita Federal**

#### Carregar todos os períodos disponíveis:

```bash
gcloud pubsub topics publish bigquery-loader-receita --message='{}'
```

**Comportamento:**

- Primeiro período: `WRITE_TRUNCATE` (substitui tabela)
- Demais períodos: `WRITE_APPEND` (adiciona dados)

#### Carregar período específico (WRITE_APPEND):

```bash
gcloud pubsub topics publish bigquery-loader-receita \
  --message='{"period": "2024-03"}'
```

#### Carregar período específico (WRITE_TRUNCATE):

```bash
gcloud pubsub topics publish bigquery-loader-receita \
  --message='{"period": "2024-03", "mode": "WRITE_TRUNCATE"}'
```

---

## 📅 Agendar Execução Automática

### **Loader Fazenda (Trimestral - após ingestão de dados)**

```bash
gcloud scheduler jobs create pubsub bigquery-loader-fazenda-quarterly \
  --location=us-central1 \
  --schedule="0 4 1 1,4,7,10 *" \
  --time-zone="America/Sao_Paulo" \
  --topic=bigquery-loader-fazenda \
  --message-body='{"mode": "WRITE_TRUNCATE"}'
```

**Cron:** Todo dia 1 às 04:00 nos meses 1, 4, 7, 10 (trimestral)

---

### **Loader Receita (Mensal - após ingestão de dados)**

```bash
gcloud scheduler jobs create pubsub bigquery-loader-receita-monthly \
  --location=us-central1 \
  --schedule="0 5 10 * *" \
  --time-zone="America/Sao_Paulo" \
  --topic=bigquery-loader-receita \
  --message-body='{}'
```

**Cron:** Todo dia 10 às 05:00 (segunda semana do mês)

---

### **Loader Receita (Período Específico - Incremental)**

```bash
# Carregar apenas o último mês disponível
gcloud scheduler jobs create pubsub bigquery-loader-receita-incremental \
  --location=us-central1 \
  --schedule="0 5 10 * *" \
  --time-zone="America/Sao_Paulo" \
  --topic=bigquery-loader-receita \
  --message-body='{"period": "2024-11"}'  # Atualizar manualmente ou usar workflow
```

---

## 🔄 Deploy Manual

Se preferir fazer deploy manualmente:

### **1. Loader Fazenda**

```bash
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal

gcloud functions deploy bigquery-loader-fazenda \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./BigQuery_loader_fazenda_CF \
  --entry-point=load_fazenda_bigquery \
  --trigger-topic=bigquery-loader-fazenda \
  --timeout=540s \
  --memory=4Gi \
  --max-instances=1 \
  --set-env-vars PROJECT_ID=trabalho-final-pdm-478021,DATASET_ID=main_database,BUCKET_NAME=dados-cnpjs,BASE_PATH=fazenda_nacional
```

### **2. Loader Receita**

```bash
gcloud functions deploy bigquery-loader-receita \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./BigQuery_loader_receita_CF \
  --entry-point=load_receita_bigquery \
  --trigger-topic=bigquery-loader-receita \
  --timeout=3600s \
  --memory=4Gi \
  --max-instances=1 \
  --set-env-vars PROJECT_ID=trabalho-final-pdm-478021,DATASET_ID=main_database,TABLE_NAME=receita_estabelecimentos,BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal
```

---

## 📊 Monitoramento

### **Ver logs do Loader Fazenda:**

```bash
gcloud functions logs read bigquery-loader-fazenda \
  --gen2 \
  --region=us-central1 \
  --limit=50
```

### **Ver logs do Loader Receita:**

```bash
gcloud functions logs read bigquery-loader-receita \
  --gen2 \
  --region=us-central1 \
  --limit=50
```

### **Seguir logs em tempo real:**

```bash
gcloud functions logs tail bigquery-loader-fazenda \
  --gen2 \
  --region=us-central1
```

---

## 🔧 Configurações Avançadas

### **Variáveis de Ambiente**

#### Loader Fazenda:

- `PROJECT_ID`: ID do projeto GCP
- `DATASET_ID`: Dataset no BigQuery (padrão: `main_database`)
- `BUCKET_NAME`: Nome do bucket GCS (padrão: `dados-cnpjs`)
- `BASE_PATH`: Caminho base no bucket (padrão: `fazenda_nacional`)

#### Loader Receita:

- `PROJECT_ID`: ID do projeto GCP
- `DATASET_ID`: Dataset no BigQuery (padrão: `main_database`)
- `TABLE_NAME`: Nome da tabela (padrão: `receita_estabelecimentos`)
- `BUCKET_NAME`: Nome do bucket GCS (padrão: `dados-cnpjs`)
- `BASE_PATH`: Caminho base no bucket (padrão: `receita_federal`)

### **Atualizar Configurações:**

```bash
gcloud functions deploy bigquery-loader-fazenda \
  --gen2 \
  --region=us-central1 \
  --update-env-vars DATASET_ID=novo_dataset
```

---

## 🗑️ Remover Funções

```bash
# Deletar Cloud Functions
gcloud functions delete bigquery-loader-fazenda --gen2 --region=us-central1
gcloud functions delete bigquery-loader-receita --gen2 --region=us-central1

# Deletar tópicos Pub/Sub
gcloud pubsub topics delete bigquery-loader-fazenda
gcloud pubsub topics delete bigquery-loader-receita

# Deletar schedulers (se existirem)
gcloud scheduler jobs delete bigquery-loader-fazenda-quarterly --location=us-central1
gcloud scheduler jobs delete bigquery-loader-receita-monthly --location=us-central1
```

---

## ⚠️ Troubleshooting

### **Erro: "Dataset not found"**

```bash
bq mk --dataset SEU-PROJETO-ID:main_database
```

### **Erro: "Permission denied"**

Verifique se a service account da Cloud Function tem permissões:

- `roles/bigquery.dataEditor`
- `roles/bigquery.jobUser`
- `roles/storage.objectViewer`

```bash
# Dar permissões
PROJECT_ID="seu-projeto-id"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectViewer"
```

### **Erro: "No files matched"**

Verifique se os dados foram baixados no bucket:

```bash
# Fazenda
gsutil ls gs://dados-cnpjs/fazenda_nacional/2024/1trimestre/

# Receita
gsutil ls gs://dados-cnpjs/receita_federal/2024-03/
```

### **Timeout na execução**

Aumente o timeout:

```bash
gcloud functions deploy bigquery-loader-receita \
  --gen2 \
  --region=us-central1 \
  --timeout=5400s  # 1.5 horas
```

---

## 📚 Integração com Workflows

Os loaders podem ser chamados via GCP Workflows após a ingestão de dados:

```yaml
- call_loader_fazenda:
    call: googleapis.pubsub.v1.projects.topics.publish
    args:
      topic: ${"projects/" + project_id + "/topics/bigquery-loader-fazenda"}
      body:
        messages:
          - data: ${base64.encode(json.encode({"mode": "WRITE_TRUNCATE"}))}
```

---

## 🎉 Pronto!

Seus loaders estão deployados e prontos para carregar dados do GCS para o BigQuery! 🚀

