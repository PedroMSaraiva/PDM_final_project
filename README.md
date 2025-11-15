# ☁️ Cloud Functions - Dados Públicos

Cloud Functions para baixar dados públicos brasileiros diretamente para Google Cloud Storage via **Pub/Sub**.

## 📦 Funções Disponíveis

### 1. Crawler Receita Federal
Baixa dados de Estabelecimentos CNPJ da Receita Federal.

**Localização:** `Receita_Federal_CF/`  
**Tópico Pub/Sub:** `receita-federal-download`  
**Handler:** `crawler_receita_pubsub`

### 2. Download PGFN (Fazenda Nacional)
Baixa dados da Procuradoria-Geral da Fazenda Nacional (Não Previdenciário, FGTS, Previdenciário).

**Localização:** `Fazenda_CF/`  
**Tópico Pub/Sub:** `fazenda-download`  
**Handler:** `download_fazenda_pubsub`

---

## 🚀 Deploy Rápido

```bash
# 1. Configurar projeto
gcloud config set project SEU-PROJETO-ID

# 2. Habilitar APIs
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com storage.googleapis.com pubsub.googleapis.com

# 3. Criar bucket
gsutil mb -l southamerica-east1 gs://dados-cnpjs

# 4. Usar script automatizado
./deploy.sh
```

Ou consulte [DEPLOY.md](./DEPLOY.md) para comandos manuais detalhados.

---

## 📊 Estrutura do Projeto

```
TrabalhoFinal/
├── Receita_Federal_CF/          # Cloud Function - Receita Federal
│   ├── crawler_receita_cf.py    # Código principal
│   ├── main.py                   # Entry point
│   └── requirements_cloud_functions.txt
│
├── Fazenda_CF/                   # Cloud Function - Fazenda Nacional
│   ├── download_fazenda_cf.py   # Código principal
│   ├── main.py                   # Entry point
│   └── requirements_cloud_functions.txt
│
├── DEPLOY.md                     # Guia de deploy completo
├── README.md                     # Este arquivo
└── deploy.sh                     # Script de deploy automatizado
```

---

## 🎯 Como Funciona

1. **Publicar mensagem no Pub/Sub** → Aciona a Cloud Function
2. **Cloud Function baixa dados** → Faz download e extrai arquivos
3. **Salva diretamente no GCS** → Organiza em estrutura de pastas
4. **ZIPs deletados automaticamente** → Economia de espaço
5. **Marcadores previnem reprocessamento** → Eficiência

---

## 💡 Invocar Funções

### Receita Federal

```bash
# Listar pastas disponíveis
gcloud pubsub topics publish receita-federal-download --message='{}'

# Listar arquivos de uma pasta
gcloud pubsub topics publish receita-federal-download \
  --message='{"folder": "2024-03", "list_files": true}'

# Processar UM arquivo específico (RECOMENDADO)
gcloud pubsub topics publish receita-federal-download \
  --message='{"folder": "2024-03", "file": "Estabelecimentos0.zip"}'
```

### Fazenda Nacional

```bash
# Processar todos
gcloud pubsub topics publish fazenda-download --message='{}'

# Processar específico
gcloud pubsub topics publish fazenda-download --message='{"year": 2024, "quarter": 3, "data_type": "Dados_abertos_FGTS"}'
```

---

## 📅 Agendamento Automático

```bash
# Receita Federal (mensal)
gcloud scheduler jobs create pubsub receita-monthly \
  --location=southamerica-east1 \
  --schedule="0 2 1 * *" \
  --topic=receita-federal-download \
  --message-body='{}'

# Fazenda Nacional (trimestral)
gcloud scheduler jobs create pubsub fazenda-quarterly \
  --location=southamerica-east1 \
  --schedule="0 2 1 1,4,7,10 *" \
  --topic=fazenda-download \
  --message-body='{}'
```

---

## 📝 Ver Logs

```bash
# Receita Federal
gcloud functions logs read crawler-receita-federal --gen2 --region=southamerica-east1 --limit=100

# Fazenda Nacional
gcloud functions logs read download-fazenda-nacional --gen2 --region=southamerica-east1 --limit=100
```

---

## 📁 Dados no Bucket

Os dados são organizados automaticamente:

```
gs://dados-cnpjs/
├── receita_federal/
│   ├── 2023-03/
│   │   ├── estabelecimento1.csv
│   │   ├── estabelecimento2.csv
│   │   └── .Estabelecimentos0.extracted (marcador)
│   └── 2023-09/
│       └── ...
└── fazenda_nacional/
    ├── 2020/
    │   ├── 1trimestre/
    │   │   ├── Nao_Previdenciario/
    │   │   ├── FGTS/
    │   │   └── Previdenciario/
    │   └── 2trimestre/
    └── 2021/
```

---

## ⚙️ Características

- ✅ **Pub/Sub exclusivo** - Arquitetura event-driven
- ✅ **Processamento em memória** - ZIPs não salvos em disco
- ✅ **Upload direto para GCS** - Sem armazenamento local
- ✅ **Marcadores inteligentes** - Evita reprocessamento
- ✅ **Timeout otimizado** - 3600s para arquivos grandes
- ✅ **Retry automático** - Resiliência em caso de falhas
- ✅ **Estrutura mantida** - Organização de pastas preservada

---

## 🔧 Configurações

Ajuste as variáveis de ambiente no comando de deploy (ver [DEPLOY.md](./DEPLOY.md)):

**Receita Federal:**
- `DESTINATION_BUCKET_NAME`, `BASE_PATH`, `START_YEAR_MONTH`, `END_YEAR_MONTH`, `ALLOWED_MONTHS`

**Fazenda Nacional:**
- `DESTINATION_BUCKET_NAME`, `BASE_PATH`, `START_YEAR`, `END_YEAR`, `END_QUARTER`

---

## 📚 Documentação

- **[DEPLOY.md](./DEPLOY.md)** - Guia completo de deploy e configuração
- **[deploy.sh](./deploy.sh)** - Script automatizado de deploy

---

## 🎉 Pronto!

Suas Cloud Functions estão prontas para baixar dados públicos automaticamente via Pub/Sub! 🚀

