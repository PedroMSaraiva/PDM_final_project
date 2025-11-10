# 🔧 Como Aplicar a Correção da DAG

## ✅ O que foi corrigido?

O script `ETL_coletar_dados_e_gravar_BD.py` não funcionava no Docker porque tentava fazer um `input()` interativo. Agora ele usa diretamente as variáveis de ambiente do Docker Compose.

## 🚀 Passos para Aplicar (RÁPIDO)

### 1. Reiniciar o Docker Compose

```bash
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal/dados/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master

# Parar tudo
docker compose down

# Iniciar novamente
docker compose up -d

# Ver logs
docker compose logs -f airflow-scheduler
```

### 2. Testar a DAG

1. Acesse: http://localhost:8080
2. Login: `airflow` / `airflow`
3. Procure a DAG: `etl_receita_federal`
4. Clique no botão ▶️ (Play) → "Trigger DAG"
5. Acompanhe os logs

## ✅ Sucesso Esperado

Você verá nos logs:

```
✓ Arquivo .env não encontrado. Usando variáveis de ambiente do sistema.
✓ Diretórios definidos:
✓ output_files: /opt/airflow/data/downloads
✓ extracted_files: /opt/airflow/data/extracted
✓ Arquivos que serão baixados:
  1 - EMPRESA...
  2 - ESTABELE...
```

## ⏱️ Tempo de Execução

- **Download**: 30-90 minutos (~17GB)
- **ETL Total**: 4-8 horas

## 📚 Mais Informações

- Detalhes completos: `CORRECAO-DAG-ERROR.md`
- Documentação: `README-DOCKER.md`
- Comandos úteis: `QUICK-REFERENCE.md`

---

**🎯 Ação Imediata**: Execute os comandos da seção "Passos para Aplicar" acima!

