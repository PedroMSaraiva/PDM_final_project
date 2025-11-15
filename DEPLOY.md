# 🚀 Deploy Cloud Functions via Pub/Sub

Guia único para deploy das Cloud Functions usando **exclusivamente Pub/Sub triggers**.

## 📋 Pré-requisitos

```bash
# Autenticar e configurar projeto
gcloud auth login
gcloud config set project SEU-PROJETO-ID

# Habilitar APIs
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com storage.googleapis.com pubsub.googleapis.com

# Criar bucket
gsutil mb gs://dados-cnpjs
```

## 🎯 Deploy das Funções

### 1️⃣ Crawler Receita Federal

**Criar tópico Pub/Sub:**

```bash
gcloud pubsub topics create receita-federal-download
```

**Deploy:**

```bash
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal

gcloud functions deploy crawler-receita-federal \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Receita_Federal_CF \
  --entry-point=crawler_receita_pubsub \
  --trigger-topic=receita-federal-download \
  --timeout=540s \
  --memory=2Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal,START_YEAR_MONTH=2023-01,END_YEAR_MONTH=2025-11
```

**Invocar:**

```bash
# 1. Listar pastas disponíveis
gcloud pubsub topics publish receita-federal-download --message='{}'

# 2. Listar arquivos de uma pasta específica
gcloud pubsub topics publish receita-federal-download \
  --message='{"folder": "2024-03", "list_files": true}'

# 3. Processar UM arquivo específico (RECOMENDADO - evita timeout)
gcloud pubsub topics publish receita-federal-download \
  --message='{"folder": "2024-03", "file": "Estabelecimentos0.zip"}'

# 4. Processar pasta completa (CUIDADO: pode dar timeout!)
gcloud pubsub topics publish receita-federal-download \
  --message='{"folder": "2024-03"}'
```

**⚠️ IMPORTANTE:** Para arquivos grandes (Estabelecimentos), use SEMPRE o modo de arquivo individual (#3) para evitar timeout.

---

### 2️⃣ Download PGFN (Fazenda Nacional)

**Criar tópico Pub/Sub:**

```bash
gcloud pubsub topics create fazenda-download
```

**Deploy:**

```bash
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal

gcloud functions deploy download-fazenda-nacional \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Fazenda_CF \
  --entry-point=download_fazenda_pubsub \
  --trigger-topic=fazenda-download \
  --timeout=540s \
  --memory=2Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=fazenda_nacional,START_YEAR=2020,END_YEAR=2025
```

**Invocar:**

```bash
# Processar todos os arquivos configurados
gcloud pubsub topics publish fazenda-download --message='{}'

# Processar arquivo específico
gcloud pubsub topics publish fazenda-download --message='{"year": 2024, "quarter": 3, "data_type": "Dados_abertos_FGTS"}'
```

---

## 📊 Configurações

### Receita Federal

| Variável                   | Descrição       | Valor               |
| --------------------------- | ----------------- | ------------------- |
| `DESTINATION_BUCKET_NAME` | Bucket GCS        | `dados-cnpjs`     |
| `BASE_PATH`               | Caminho no bucket | `receita_federal` |
| `START_YEAR_MONTH`        | Início (YYYY-MM) | `2023-01`         |
| `END_YEAR_MONTH`          | Fim (YYYY-MM)     | `2025-12`         |
| `ALLOWED_MONTHS`          | Meses (ex: 03,09) | `03,09`           |

### Fazenda Nacional

| Variável                   | Descrição       | Valor                |
| --------------------------- | ----------------- | -------------------- |
| `DESTINATION_BUCKET_NAME` | Bucket GCS        | `dados-cnpjs`      |
| `BASE_PATH`               | Caminho no bucket | `fazenda_nacional` |
| `START_YEAR`              | Ano inicial       | `2020`             |
| `END_YEAR`                | Ano final         | `2025`             |
| `END_QUARTER`             | Último trimestre | `3`                |

---

## 🔄 Atualizar Funções

```bash
# Após modificar código
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal

# Receita Federal
gcloud functions deploy crawler-receita-federal \
  --gen2 \
  --region=southamerica-east1 \
  --source=./Receita_Federal_CF \
  --entry-point=crawler_receita_pubsub

# Fazenda Nacional
gcloud functions deploy download-fazenda-nacional \
  --gen2 \
  --region=southamerica-east1 \
  --source=./Fazenda_CF \
  --entry-point=download_fazenda_pubsub
```

---

## 📝 Logs e Monitoramento

```bash
# Ver logs Receita Federal
gcloud functions logs read crawler-receita-federal \
  --gen2 \
  --region=southamerica-east1 \
  --limit=100

# Ver logs Fazenda
gcloud functions logs read download-fazenda-nacional \
  --gen2 \
  --region=southamerica-east1 \
  --limit=100

# Seguir logs em tempo real
gcloud functions logs tail crawler-receita-federal \
  --gen2 \
  --region=southamerica-east1
```

---

## 📅 Agendar Execução Automática

### Receita Federal (Mensal - Todo dia 1)

```bash
gcloud scheduler jobs create pubsub receita-monthly \
  --location=southamerica-east1 \
  --schedule="0 2 1 * *" \
  --time-zone="America/Sao_Paulo" \
  --topic=receita-federal-download \
  --message-body='{}'
```

### Fazenda Nacional (Trimestral)

```bash
gcloud scheduler jobs create pubsub fazenda-quarterly \
  --location=southamerica-east1 \
  --schedule="0 2 1 1,4,7,10 *" \
  --time-zone="America/Sao_Paulo" \
  --topic=fazenda-download \
  --message-body='{}'
```

---

## 🗑️ Remover Funções

```bash
# Receita Federal
gcloud functions delete crawler-receita-federal --gen2 --region=southamerica-east1
gcloud pubsub topics delete receita-federal-download
gcloud scheduler jobs delete receita-monthly --location=southamerica-east1

# Fazenda Nacional
gcloud functions delete download-fazenda-nacional --gen2 --region=southamerica-east1
gcloud pubsub topics delete fazenda-download
gcloud scheduler jobs delete fazenda-quarterly --location=southamerica-east1
```

---

## 🛠️ Script Automatizado

Use o script `deploy.sh` para deploy rápido:

```bash
./deploy.sh
```

---

## 📁 Estrutura dos Dados no Bucket

```
gs://dados-cnpjs/
├── receita_federal/
│   ├── 2023-03/
│   │   ├── arquivo1.csv
│   │   └── .Estabelecimentos0.extracted
│   └── 2023-09/
└── fazenda_nacional/
    └── 2020/
        └── 1trimestre/
            ├── Nao_Previdenciario/
            ├── FGTS/
            └── Previdenciario/
```
