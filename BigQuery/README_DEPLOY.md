# 📦 Deploy Automatizado dos Loaders BigQuery

## 🚀 Quick Start

```bash
cd BigQuery/
./deploy-loaders.sh
```

Isso vai fazer o deploy de 2 Cloud Functions:
1. **bigquery-loader-fazenda** - Carrega dados da Fazenda Nacional
2. **bigquery-loader-receita** - Carrega dados da Receita Federal

---

## 📋 O que foi criado

### **Estrutura de Arquivos:**
```
BigQuery/
├── deploy-loaders.sh          # Script de deploy automatizado
├── DEPLOY.md                  # Documentação completa
├── README_DEPLOY.md           # Este arquivo (resumo)
│
BigQuery_loader_fazenda_CF/
├── main.py                    # Cloud Function - Loader Fazenda
└── requirements.txt           # Dependências
│
BigQuery_loader_receita_CF/
├── main.py                    # Cloud Function - Loader Receita
└── requirements.txt           # Dependências
```

---

## 🎯 Como Usar

### **1. Deploy (uma vez)**
```bash
cd BigQuery/
./deploy-loaders.sh
```

### **2. Executar Loaders**

#### Fazenda Nacional:
```bash
# Carregar todos os dados
gcloud pubsub topics publish bigquery-loader-fazenda --message='{}'
```

#### Receita Federal:
```bash
# Carregar todos os períodos
gcloud pubsub topics publish bigquery-loader-receita --message='{}'

# Carregar período específico
gcloud pubsub topics publish bigquery-loader-receita \
  --message='{"period": "2024-03"}'
```

---

## 📅 Agendar Execução

### **Fazenda (Trimestral):**
```bash
gcloud scheduler jobs create pubsub bigquery-loader-fazenda-quarterly \
  --location=us-central1 \
  --schedule="0 4 1 1,4,7,10 *" \
  --time-zone="America/Sao_Paulo" \
  --topic=bigquery-loader-fazenda \
  --message-body='{"mode": "WRITE_TRUNCATE"}'
```

### **Receita (Mensal):**
```bash
gcloud scheduler jobs create pubsub bigquery-loader-receita-monthly \
  --location=us-central1 \
  --schedule="0 5 10 * *" \
  --time-zone="America/Sao_Paulo" \
  --topic=bigquery-loader-receita \
  --message-body='{}'
```

---

## 📊 Tabelas Criadas no BigQuery

### **Fazenda Nacional:**
- `pgfn_nao_previdenciario`
- `pgfn_fgts`
- `pgfn_previdenciario`

### **Receita Federal:**
- `receita_estabelecimentos`

---

## 🔍 Monitoramento

```bash
# Ver logs Fazenda
gcloud functions logs read bigquery-loader-fazenda --gen2 --region=us-central1

# Ver logs Receita
gcloud functions logs read bigquery-loader-receita --gen2 --region=us-central1
```

---

## 📚 Documentação Completa

Consulte `DEPLOY.md` para:
- Deploy manual
- Configurações avançadas
- Troubleshooting
- Integração com Workflows

---

## ✅ Pronto!

Os loaders estão prontos para carregar dados do GCS para o BigQuery automaticamente! 🎉


