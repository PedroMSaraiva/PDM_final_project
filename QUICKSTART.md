# ⚡ Guia Rápido - 5 Minutos

Deploy das Cloud Functions em 5 passos simples.

## 1️⃣ Autenticar

```bash
gcloud auth login
gcloud config set project SEU-PROJETO-ID
```

## 2️⃣ Habilitar APIs

```bash
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  storage.googleapis.com \
  pubsub.googleapis.com
```

## 3️⃣ Criar Bucket

```bash
gsutil mb -l southamerica-east1 gs://dados-cnpjs
```

## 4️⃣ Deploy

```bash
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal
./deploy.sh
```

Escolha no menu:
- `1` para Receita Federal
- `2` para Fazenda Nacional
- `3` para ambas

## 5️⃣ Invocar

```bash
# Receita Federal
gcloud pubsub topics publish receita-federal-download --message='{}'

# Fazenda Nacional
gcloud pubsub topics publish fazenda-download --message='{}'
```

## ✅ Pronto!

Suas funções estão rodando! Veja os logs:

```bash
# Receita Federal
gcloud functions logs read crawler-receita-federal --gen2 --region=southamerica-east1 --limit=50

# Fazenda Nacional
gcloud functions logs read download-fazenda-nacional --gen2 --region=southamerica-east1 --limit=50
```

## 📅 Agendar (Opcional)

```bash
# Receita Federal - Mensal (todo dia 1)
gcloud scheduler jobs create pubsub receita-monthly \
  --location=southamerica-east1 \
  --schedule="0 2 1 * *" \
  --topic=receita-federal-download \
  --message-body='{}'

# Fazenda Nacional - Trimestral (jan, abr, jul, out)
gcloud scheduler jobs create pubsub fazenda-quarterly \
  --location=southamerica-east1 \
  --schedule="0 2 1 1,4,7,10 *" \
  --topic=fazenda-download \
  --message-body='{}'
```

---

Para mais detalhes, consulte [DEPLOY.md](./DEPLOY.md)
