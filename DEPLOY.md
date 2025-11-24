# 🚀 Deploy das Cloud Functions e Loaders

Documentação única para subir, atualizar e operar todos os componentes de ingestão e carregamento BigQuery deste repositório.

---

## 1. Pré-requisitos

```bash
gcloud auth login
gcloud config set project <SEU_PROJETO>

gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  workflows.googleapis.com \
  cloudscheduler.googleapis.com

gsutil mb -l southamerica-east1 gs://dados-cnpjs
```

> Ajuste bucket/região conforme necessidade.

---

## 2. Mapa das funções

| Função | Diretório (`Cloud_Functions/...`) | Trigger | Destino |
| --- | --- | --- | --- |
| `crawler-receita-estabelecimentos` | `Receita_estabelecimentos_CF` | Pub/Sub `receita-estabelecimentos-download` | GCS `receita_federal/estabelecimentos/<ano-mes>` |
| `crawler-receita-empresas` | `Receita_empresas_CF` | Pub/Sub `receita-empresas-download` | GCS `receita_federal/empresas/<ano-mes>` |
| `crawler-receita-lucros` | `Receita_lucros_CF` | Pub/Sub `receita-lucros-download` | GCS `receita_federal/regime_tributario/<arquivo>` |
| `download-fazenda-nacional` | `Fazenda_CF` | Pub/Sub `fazenda-download` | GCS `fazenda_nacional/<ano>/<trimestre>/<tipo>` |
| `banco-central-indicadores` | `Banco_Central_CF` | Pub/Sub `banco-central-download` | GCS `banco_central/<ano_mes>` + BigQuery |
| `bigquery-loader-fazenda` | `BigQuery_loader_fazenda_CF` | Pub/Sub `bigquery-loader-fazenda` | BigQuery dataset PGFN |
| `bigquery-loader-receita` | `BigQuery_loader_receita_CF` | Pub/Sub `bigquery-loader-receita` ou script local | BigQuery dataset Receita (estabelecimentos + empresas) |

---

## 3. Deploy dos crawlers (download → GCS)

Execute sempre a partir da raiz do projeto.

### Receita – Estabelecimentos

```bash
gcloud pubsub topics create receita-estabelecimentos-download

gcloud functions deploy crawler-receita-estabelecimentos \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Cloud_Functions/Receita_estabelecimentos_CF \
  --entry-point=crawler_receita_pubsub \
  --trigger-topic=receita-estabelecimentos-download \
  --timeout=540s \
  --memory=8Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal/estabelecimentos,START_YEAR_MONTH=2023-01,END_YEAR_MONTH=2025-12
```

### Receita – Empresas

```bash
gcloud pubsub topics create receita-empresas-download

gcloud functions deploy crawler-receita-empresas \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Cloud_Functions/Receita_empresas_CF \
  --entry-point=crawler_receita_pubsub \
  --trigger-topic=receita-empresas-download \
  --timeout=540s \
  --memory=8Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal/empresas,START_YEAR_MONTH=2023-01,END_YEAR_MONTH=2025-12
```

### Receita – Regime Tributário (Lucros)

```bash
gcloud pubsub topics create receita-lucros-download

gcloud functions deploy crawler-receita-lucros \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Cloud_Functions/Receita_lucros_CF \
  --entry-point=crawler_receita_pubsub \
  --trigger-topic=receita-lucros-download \
  --timeout=540s \
  --memory=4Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal/regime_tributario
```

### PGFN (Fazenda Nacional)

```bash
gcloud pubsub topics create fazenda-download

gcloud functions deploy download-fazenda-nacional \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Cloud_Functions/Fazenda_CF \
  --entry-point=download_fazenda_pubsub \
  --trigger-topic=fazenda-download \
  --timeout=540s \
  --memory=2Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=fazenda_nacional,START_YEAR=2020,END_YEAR=2025
```

### Banco Central

```bash
gcloud pubsub topics create banco-central-download

gcloud functions deploy banco-central-indicadores \
  --gen2 \
  --runtime=python311 \
  --region=us-east1 \
  --source=./Cloud_Functions/Banco_Central_CF \
  --entry-point=collect_indicadores_pubsub \
  --trigger-topic=banco-central-download \
  --timeout=540s \
  --memory=2Gi \
  --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=banco_central
```

---

## 4. Deploy dos loaders BigQuery

> Para automatizar o processo (criação dos tópicos + deploy das duas funções), rode `./scripts/deploy-loaders.sh`.

### Loader PGFN → BigQuery

```bash
gcloud pubsub topics create bigquery-loader-fazenda

gcloud functions deploy bigquery-loader-fazenda \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./Cloud_Functions/BigQuery_loader_fazenda_CF \
  --entry-point=load_fazenda_bigquery \
  --trigger-topic=bigquery-loader-fazenda \
  --timeout=3600s \
  --memory=2Gi \
  --max-instances=1 \
  --set-env-vars PROJECT_ID=<SEU_PROJETO>,DATASET_ID=main_database,BUCKET_NAME=dados-cnpjs,BASE_PATH=fazenda_nacional
```

### Loader Receita → BigQuery (Estabelecimentos + Empresas)

```bash
gcloud pubsub topics create bigquery-loader-receita

gcloud functions deploy bigquery-loader-receita \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./Cloud_Functions/BigQuery_loader_receita_CF \
  --entry-point=load_receita_bigquery \
  --trigger-topic=bigquery-loader-receita \
  --timeout=3600s \
  --memory=4Gi \
  --max-instances=1 \
  --set-env-vars PROJECT_ID=<SEU_PROJETO>,DATASET_ID=main_database,TABLE_NAME_ESTABELECIMENTOS=receita_estabelecimentos,TABLE_NAME_EMPRESAS=receita_empresas,BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal
```

#### Runner local (evitar timeout)

```bash
cd Cloud_Functions/BigQuery_loader_receita_CF
python run_loader_empresas_local.py --data-type empresas --period 2024-03
# ou
python run_loader_empresas_local.py --mode all --data-type all
```

---

## 5. Payloads e invocação

### Downloads (GCS)

```bash
# Receita Estabelecimentos – arquivo único (recomendado)
gcloud pubsub topics publish receita-estabelecimentos-download \
  --message='{"folder":"2024-03","file":"Estabelecimentos0.zip"}'

# Receita Empresas – listar arquivos
gcloud pubsub topics publish receita-empresas-download --message='{"folder":"2024-03","list_files":true}'

# Receita Lucros – arquivo específico
gcloud pubsub topics publish receita-lucros-download --message='{"file":"Lucro Real.zip"}'

# PGFN – trimestre/Data type
gcloud pubsub topics publish fazenda-download \
  --message='{"year":2024,"quarter":3,"data_type":"Dados_abertos_FGTS"}'

# Banco Central – período
gcloud pubsub topics publish banco-central-download --message='{"ano":2024,"mes":10}'
```

### Loaders BigQuery

```bash
# Receita (todos os períodos configurados)
gcloud pubsub topics publish bigquery-loader-receita --message='{}'

# Receita – período + tipo
gcloud pubsub topics publish bigquery-loader-receita \
  --message='{"period":"2024-03","data_type":"empresas","write_mode":"WRITE_APPEND"}'

# PGFN
gcloud pubsub topics publish bigquery-loader-fazenda --message='{}'
```

Payload do loader Receita:

```json
{
  "period": "2024-03",          // opcional, processa somente o período informado
  "data_type": "empresas",      // "estabelecimentos" | "empresas" | "all"
  "write_mode": "WRITE_APPEND"  // default WRITE_TRUNCATE para primeira carga
}
```

---

## 6. Variáveis de ambiente úteis

| Função | Variáveis chave |
| --- | --- |
| Crawlers Receita | `DESTINATION_BUCKET_NAME`, `BASE_PATH`, `START_YEAR_MONTH`, `END_YEAR_MONTH`, `ALLOWED_MONTHS` |
| Crawlers Fazenda | `DESTINATION_BUCKET_NAME`, `BASE_PATH`, `START_YEAR`, `END_YEAR`, `END_QUARTER` |
| Banco Central | `DESTINATION_BUCKET_NAME`, `BASE_PATH`, `PROJECT_ID`, `DATASET_ID` |
| Loader Receita | `PROJECT_ID`, `DATASET_ID`, `TABLE_NAME_ESTABELECIMENTOS`, `TABLE_NAME_EMPRESAS`, `BUCKET_NAME`, `BASE_PATH` |
| Loader Fazenda | `PROJECT_ID`, `DATASET_ID`, `BUCKET_NAME`, `BASE_PATH` |

---

## 7. Atualizar funções (redeploy rápido)

```bash
gcloud functions deploy crawler-receita-empresas \
  --gen2 --region=us-east1 \
  --source=./Cloud_Functions/Receita_empresas_CF \
  --entry-point=crawler_receita_pubsub
# Repita trocando diretório/entry-point para as demais funções
```

---

## 8. Logs & monitoramento

```bash
gcloud functions logs read crawler-receita-estabelecimentos --gen2 --region=us-east1 --limit=100
gcloud functions logs read download-fazenda-nacional --gen2 --region=us-east1 --limit=100
gcloud functions logs read bigquery-loader-receita --gen2 --region=us-central1 --limit=100
gcloud functions logs tail crawler-receita-empresas --gen2 --region=us-east1
```

Para Workflows/Schedulers veja `scripts/README.md`.

---

## 9. Schedulers sugeridos

```bash
# Receita Estabelecimentos / Empresas – dia 10
gcloud scheduler jobs create pubsub receita-estabelecimentos-monthly \
  --location=us-east1 \
  --schedule="0 2 10 * *" \
  --topic=receita-estabelecimentos-download \
  --message-body='{}'

gcloud scheduler jobs create pubsub receita-empresas-monthly \
  --location=us-east1 \
  --schedule="0 3 10 * *" \
  --topic=receita-empresas-download \
  --message-body='{}'

# Fazenda – trimestral (Jan/Apr/Jul/Oct)
gcloud scheduler jobs create pubsub fazenda-quarterly \
  --location=us-east1 \
  --schedule="0 4 15 1,4,7,10 *" \
  --topic=fazenda-download \
  --message-body='{}'

# Receita Lucros – fevereiro e agosto
gcloud scheduler jobs create pubsub receita-lucros-semestral \
  --location=us-east1 \
  --schedule="0 5 1 2,8 *" \
  --topic=receita-lucros-download \
  --message-body='{}'
```

---

## 10. Remoção

```bash
gcloud functions delete crawler-receita-empresas --gen2 --region=us-east1
gcloud pubsub topics delete receita-empresas-download
gcloud scheduler jobs delete receita-empresas-monthly --location=us-east1
# Repita para cada função/topic/job conforme necessidade
```

---

## 11. Estrutura esperada no GCS

```
gs://dados-cnpjs/
├── receita_federal/
│   ├── estabelecimentos/2024-03/*.csv
│   ├── empresas/2024-03/*.csv
│   └── regime_tributario/<Regime>/*.csv
├── fazenda_nacional/2024/3trimestre/<FGTS|Nao_Previdenciario|Previdenciario>/*.csv
└── banco_central/2024-10/*.json
```

Marcadores `.arquivo.extracted` evitam reprocessamentos.

---

## 12. Dicas rápidas

- Estabelecimentos e Empresas geram arquivos grandes → publique mensagens por arquivo e monitore tempo restante.  
- Para cargas BigQuery extensas, use o runner local e/ou defina `period` para processar em blocos.  
- Particione/clusterize as tabelas no BigQuery para reduzir custo (`ano_mes` já é carregado dos arquivos).  
- Scripts úteis: `scripts/enviar_mensagens_lote.sh` (envio em massa), `scripts/setup-schedulers.sh` (agendamentos).

---

Com estes passos você consegue subir toda a pipeline (download → GCS → BigQuery) e manter os jobs agendados/monitorados em produção. Dúvidas adicionais? Veja `ARCHITECTURE.md` ou `scripts/README.md`. 🚀
