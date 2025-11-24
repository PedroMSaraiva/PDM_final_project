# Cloud Data Ingestion & Analytics Platform

Automação completa para coletar dados públicos brasileiros (Receita Federal, PGFN/Fazenda, Banco Central), armazenar em Google Cloud Storage, carregar no BigQuery e treinar modelos em notebooks Jupyter.

---

## 🔎 Visão Geral

- **Ingestão:** Cloud Functions Gen2 acionadas por Pub/Sub fazem download, extração e upload organizado dos dados.
- **Orquestração:** Workflows + Cloud Scheduler permitem execuções mensais, trimestrais ou sob demanda.
- **Curadoria:** Loaders dedicados (`BigQuery_loader_*`) movem os CSV/JSON do GCS para datasets particionados no BigQuery.
- **Analytics/ML:** Notebooks em `models/` usam o dataset *silver* para análises exploratórias e predição de `situacao_cadastral`, com versões Pandas e Spark.

Consulte `ARCHITECTURE.md` para o fluxo ponta a ponta e `DEPLOY.md` para os comandos oficiais de implantação.

---

## 📁 Estrutura Principal

| Caminho | Conteúdo |
| --- | --- |
| `Cloud_Functions/` | Crawlers (Receita, Fazenda, Banco Central) e loaders BigQuery prontos para Pub/Sub |
| `BigQuery/` | Scripts standalone (`loader.py`, `loader_receita.py`) e documentação complementar |
| `models/` | Notebooks `ml_model_prediction_silver.ipynb`, versão Spark e datasets CSV |
| `scripts/` | Automação: deploy dos loaders, workflow, schedulers, quickstart e envio em lote |
| `docs/` | Metadados oficiais (`cnpj-metadados.pdf`, dicionários de campos) |
| `ARCHITECTURE.md` | Detalhes da arquitetura e fluxos |
| `DEPLOY.md` | Guia completo de deploy/update das Cloud Functions e loaders |
| `QUICKSTART.md` | Passo a passo em 5 minutos para subir o essencial |

---

## ⚙️ Fluxo de Dados Resumido

1. **Cloud Scheduler** dispara o **Workflow** com um payload (`type`) ou você publica manualmente no Pub/Sub.
2. **Workflow** invoca a Cloud Function adequada (Receita/Fazenda/Banco Central); cada função:
   - baixa o ZIP/JSON,
   - extrai/normaliza,
   - grava no bucket `gs://dados-cnpjs/<fonte>/<período>/`,
   - cria marcadores `.extracted` para evitar reprocessos.
3. **Loaders** (`BigQuery_loader_fazenda_CF`, `BigQuery_loader_receita_CF`) movem os dados para tabelas no dataset `main_database`.
4. **Notebooks** consomem as camadas *silver* (CSV ou BigQuery) para análises e modelos.

---

## 🚀 Guia Rápido

```bash
# Autenticação e projeto
gcloud auth login
gcloud config set project <SEU_PROJETO>

# APIs fundamentais
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com \
  pubsub.googleapis.com storage.googleapis.com bigquery.googleapis.com \
  workflows.googleapis.com cloudscheduler.googleapis.com

# Bucket padrão
gsutil mb -l southamerica-east1 gs://dados-cnpjs

# Deploy dos loaders BigQuery (Opcional: usa defaults do script)
./scripts/deploy-loaders.sh

# Deploy manual de um crawler (exemplo: Receita empresas)
gcloud functions deploy crawler-receita-empresas \
  --gen2 --runtime=python311 --region=us-east1 \
  --source=./Cloud_Functions/Receita_empresas_CF \
  --entry-point=crawler_receita_pubsub \
  --trigger-topic=receita-empresas-download \
  --timeout=540s --memory=8Gi --max-instances=1 \
  --set-env-vars DESTINATION_BUCKET_NAME=dados-cnpjs,BASE_PATH=receita_federal/empresas
```

Para comandos completos (incluindo Banco Central e o runner local dos loaders) veja `DEPLOY.md`. Se preferir um setup guiado, execute `./scripts/quickstart.sh`.

---

## 📬 Operações Cotidianas

- **Executar ingestão manual:** publique no Pub/Sub correspondente (`receita-estabelecimentos-download`, `fazenda-download`, etc.).
- **Carregar no BigQuery localmente:** `python Cloud_Functions/BigQuery_loader_receita_CF/run_loader_empresas_local.py --data-type empresas --period 2024-03`.
- **Agendar execuções:** utilize `scripts/setup-schedulers.sh` ou os comandos da seção Schedulers em `DEPLOY.md`.
- **Monitorar:** `gcloud functions logs read <nome> --gen2 --region <região> --limit 100` e dashboards de workflow descritos em `ARCHITECTURE.md`.

---

## 🤖 Machine Learning

- `ml_model_prediction_silver.ipynb`: fluxo completo com Pandas/Sklearn (split 70/20/10) para prever `situacao_cadastral`.
- `ml_model_prediction_silver_spark.ipynb`: feature engineering e splitting em PySpark, treinamento em Sklearn e salvamento em `models_pickle/`.
- Datasets: `dataset_metrics_silver.csv`, `dataset_silver.csv`.
- Rodar notebooks via VS Code/Jupyter local ou ambiente Dataproc/Spark, apontando para os CSVs no diretório `models/` ou BigQuery.

---

## 📚 Documentação Complementar

- `ARCHITECTURE.md` – diagramas e detalhes de segurança, custo, ciclo de vida.
- `DEPLOY.md` – lista completa de comandos gcloud e payloads.
- `BigQuery/README.md` – instruções para uso dos scripts offline e schemas.
- `docs/` – layouts oficiais (ex.: `cnpj-metadados.pdf` para o schema de empresas).

---

## ✅ Checklist Antes do Deploy

- [ ] `gcloud config get-value project` mostra o projeto correto
- [ ] Bucket GCS criado e acessível (`gs://dados-cnpjs` ou equivalente)
- [ ] APIs habilitadas
- [ ] Variáveis de ambiente ajustadas nos comandos de deploy (bucket, base path, dataset)
- [ ] Se usar Workflows, arquivo `scripts/data-ingestion-workflow.yaml` disponível

# Cloud Data Platform – Ingestão + BigQuery + ML

Pipelines completos para coletar, organizar e analisar dados públicos brasileiros com Cloud Functions, BigQuery loaders e notebooks de Machine Learning.

## Fast Track

1. **Configurar GCP** – `gcloud auth login`, `gcloud config set project <id>`  
2. **Provisionar infraestrutura** – buckets + APIs (ver `DEPLOY.md`)  
3. **Deploy** – use os comandos da seção *Cloud Functions* abaixo ou o script `scripts/deploy-loaders.sh` para os loaders do BigQuery  
4. **Acionar** – publique mensagens nas filas Pub/Sub indicadas ou execute `Cloud_Functions/BigQuery_loader_receita_CF/run_loader_empresas_local.py` para rodar localmente  
5. **Analisar** – rode os notebooks em `models/` para explorar e treinar modelos

> Precisa de um passo a passo guiado? Consulte `QUICKSTART.md`.

## Repositório em um olhar

| Diretório/Arquivo | Descrição |
| --- | --- |
| `Cloud_Functions/` | Funções de ingestão (Receita, Fazenda, Banco Central) e loaders BigQuery |
| `BigQuery/` | Scripts standalone e instruções para cargas diretas a partir do GCS |
| `models/` | Notebooks e scripts de modelagem (`ml_model_prediction_silver.ipynb`, versão Spark, etc.) |
| `scripts/` | Automação de deploy, schedulers e envio em lote para Pub/Sub |
| `docs/` | Metadados oficiais (schemas Receita, dicionários) |
| `ARCHITECTURE.md` | Visão ponta a ponta (Scheduler → Workflow → CF → GCS → BigQuery) |
| `DEPLOY.md` | Guia único de deploy e operação |
| `QUICKSTART.md` | Deploy resumido (5 minutos) |

## Cloud Functions & Loaders

| Fonte | Função (`Cloud_Functions/<dir>`) | Trigger/Script | Observações |
| --- | --- | --- | --- |
| Receita – Estabelecimentos | `Receita_estabelecimentos_CF` | Pub/Sub `receita-estabelecimentos-download` | Processa ZIP → CSV no GCS |
| Receita – Empresas | `Receita_empresas_CF` | Pub/Sub `receita-empresas-download` | Mesmo fluxo com arquivos `EMPRECSV` |
| Receita – Lucros | `Receita_lucros_CF` | Pub/Sub `receita-lucros-download` | Mantém os 4 regimes separados |
| PGFN (Fazenda) | `Fazenda_CF` | Pub/Sub `fazenda-download` | Baixa os 3 blocos (FGTS, Previd., Não Prev.) |
| Banco Central | `Banco_Central_CF` | Pub/Sub `banco-central-download` | Agrega indicadores macro |
| Loader PGFN → BigQuery | `BigQuery_loader_fazenda_CF` | Pub/Sub `bigquery-loader-fazenda` ou `scripts/deploy-loaders.sh` | Escreve em `pgfn_*` (bronze/silver) |
| Loader Receita → BigQuery | `BigQuery_loader_receita_CF` | Pub/Sub `bigquery-loader-receita` ou runner local | Carrega Estabelecimentos + Empresas; suporta `data_type` e `period` no payload |

### Executar loaders localmente (evitar timeout do Cloud Run)

```bash
cd Cloud_Functions/BigQuery_loader_receita_CF
python run_loader_empresas_local.py --data-type empresas --period 2024-03
# ou para todos os períodos
python run_loader_empresas_local.py --data-type all --mode all
```

### Payload padrão Pub/Sub (loader Receita → BigQuery)

```json
{
  "period": "2024-03",          // opcional
  "data_type": "empresas",      // "estabelecimentos" | "empresas" | "all"
  "write_mode": "WRITE_APPEND"  // ou WRITE_TRUNCATE
}
```

Detalhes sobre schemas, modos de escrita e estratégias de custo: veja `BigQuery/README.md`.

## Automação & Orquestração

- **`scripts/deploy-loaders.sh`** – Deploy end-to-end dos loaders BigQuery (cria tópicos Pub/Sub, habilita APIs, sobe as funções Gen2).  
- **`scripts/enviar_mensagens_lote.sh`** – Publica mensagens para recuperar múltiplos períodos de uma vez.  
- **`scripts/setup-schedulers.sh`** – Cria Cloud Schedulers alinhados ao calendário definido em `scripts/README.md`.  
- **Workflows/Schedulers** – Toda a lógica (tipo de execução, horários, troubleshooting) está documentada em `scripts/README.md`.

## Machine Learning (models/)

- `ml_model_prediction_silver.ipynb` – Modelo tradicional em Pandas/Sklearn (treino/val/test 70/20/10).  
- `ml_model_prediction_silver_spark.ipynb` – Versão Spark-first: feature engineering em PySpark, split temporal em Spark, conversão controlada para Pandas apenas no momento do treinamento. Salva modelo + mapeamentos em `models_pickle/`.  
- `ml_model_analysis.ipynb` – Notebook base exploratório.  
- `dataset_metrics_silver.csv` – Base consolidada (Silver) usada nos notebooks.

## Principais comandos de operação

```bash
# Receita (download) - processar arquivo específico
gcloud pubsub topics publish receita-estabelecimentos-download \
  --message='{"folder": "2024-03", "file": "Estabelecimentos0.zip"}'

# Loader Receita → BigQuery - período único
gcloud pubsub topics publish bigquery-loader-receita \
  --message='{"period": "2024-03", "data_type": "empresas", "write_mode": "WRITE_APPEND"}'

# PGFN download completo
gcloud pubsub topics publish fazenda-download --message='{}'
```

Para logs, agendamentos e remoção de recursos, siga `DEPLOY.md` (cobre comandos `gcloud functions logs read`, schedulers e limpeza).

## Documentos complementares

- `DEPLOY.md` – comandos completos de deploy/atualização, payloads e configuração de variáveis de ambiente.  
- `ARCHITECTURE.md` – diagrama + fluxo detalhado Scheduler → Workflow → Cloud Functions → GCS → BigQuery → notebooks.  
- `BigQuery/README.md` – instruções para cargas offline, schemas e troubleshooting.  
- `scripts/README.md` – orquestração via Workflows + Cloud Scheduler.  
- `docs/*.pdf|xlsx` – metadados oficiais (ex.: `cnpj-metadados.pdf` para o schema de empresas).

---

Com isso você tem ingestão automatizada por Pub/Sub, cargas confiáveis para BigQuery e notebooks prontos para modelagem. Bons experimentos! 🚀

