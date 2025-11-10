.PHONY: help setup init up down restart logs clean clean-all status

help: ## Mostra esta mensagem de ajuda
	@echo "ETL Receita Federal - Comandos disponíveis:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Configura o ambiente (cria diretórios e .env)
	@echo "📦 Criando diretórios necessários..."
	@mkdir -p dags logs plugins data/downloads data/extracted config
	@echo "✓ Diretórios criados"
	@if [ ! -f .env ]; then \
		echo "📝 Criando arquivo .env..."; \
		echo "# Configurações do Docker Compose" > .env; \
		echo "AIRFLOW_UID=$$(id -u)" >> .env; \
		echo "AIRFLOW_PROJ_DIR=." >> .env; \
		echo "" >> .env; \
		echo "# Credenciais do Airflow Web UI" >> .env; \
		echo "_AIRFLOW_WWW_USER_USERNAME=airflow" >> .env; \
		echo "_AIRFLOW_WWW_USER_PASSWORD=airflow" >> .env; \
		echo "" >> .env; \
		echo "# Bibliotecas Python adicionais" >> .env; \
		echo "_PIP_ADDITIONAL_REQUIREMENTS=beautifulsoup4>=4.9.3 bs4>=0.0.1 lxml>=4.6.3 numpy>=1.20.3 pandas>=1.2.4 psycopg2-binary>=2.9.1 python-dotenv==1.0.0 requests==2.30.0 SQLAlchemy>=1.4.18 wget>=3.2" >> .env; \
		echo "" >> .env; \
		echo "# Configurações do ETL" >> .env; \
		echo "OUTPUT_FILES_PATH=/opt/airflow/data/downloads" >> .env; \
		echo "EXTRACTED_FILES_PATH=/opt/airflow/data/extracted" >> .env; \
		echo "" >> .env; \
		echo "# Configurações do PostgreSQL - Dados RFB" >> .env; \
		echo "DB_HOST=postgres-dados-rfb" >> .env; \
		echo "DB_PORT=5432" >> .env; \
		echo "DB_USER=postgres" >> .env; \
		echo "DB_PASSWORD=postgres" >> .env; \
		echo "DB_NAME=Dados_RFB" >> .env; \
		echo "✓ Arquivo .env criado"; \
	else \
		echo "⚠️  Arquivo .env já existe, pulando..."; \
	fi
	@echo ""
	@echo "✓ Setup completo! Agora execute: make init"

init: ## Inicializa o banco de dados do Airflow
	@echo "🚀 Inicializando Airflow..."
	@docker-compose up airflow-init
	@echo ""
	@echo "✓ Airflow inicializado! Agora execute: make up"

up: ## Inicia todos os serviços
	@echo "🚀 Iniciando serviços..."
	@docker-compose up -d
	@echo ""
	@echo "✓ Serviços iniciados!"
	@echo ""
	@echo "Acesse as interfaces:"
	@echo "  🌐 Airflow Web UI: http://localhost:8080 (airflow/airflow)"
	@echo "  🐘 PgAdmin:        http://localhost:5050 (admin@admin.com/admin)"
	@echo ""
	@echo "Aguarde alguns minutos para os serviços ficarem prontos."
	@echo "Monitore com: make logs"

down: ## Para todos os serviços
	@echo "🛑 Parando serviços..."
	@docker-compose down
	@echo "✓ Serviços parados"

restart: ## Reinicia todos os serviços
	@echo "🔄 Reiniciando serviços..."
	@docker-compose restart
	@echo "✓ Serviços reiniciados"

logs: ## Mostra logs de todos os serviços
	@docker-compose logs -f

logs-scheduler: ## Mostra logs do scheduler
	@docker-compose logs -f airflow-scheduler

logs-webserver: ## Mostra logs do webserver
	@docker-compose logs -f airflow-webserver

logs-postgres: ## Mostra logs do PostgreSQL
	@docker-compose logs -f postgres-dados-rfb

status: ## Mostra status dos serviços
	@docker-compose ps

shell-airflow: ## Abre shell no container do Airflow
	@docker exec -it airflow-scheduler bash

shell-postgres: ## Abre psql no PostgreSQL
	@docker exec -it postgres-dados-rfb psql -U postgres -d Dados_RFB

clean: ## Remove containers e networks (mantém volumes)
	@echo "🧹 Limpando containers e networks..."
	@docker-compose down
	@echo "✓ Limpeza concluída (volumes preservados)"

clean-all: ## Remove TUDO (containers, networks, volumes) ⚠️ CUIDADO!
	@echo "⚠️  ATENÇÃO: Isso vai apagar TODOS os dados!"
	@read -p "Tem certeza? Digite 'sim' para confirmar: " confirm; \
	if [ "$$confirm" = "sim" ]; then \
		echo "🧹 Removendo tudo..."; \
		docker-compose down -v; \
		sudo rm -rf logs/* data/downloads/* data/extracted/*; \
		echo "✓ Tudo removido"; \
	else \
		echo "❌ Operação cancelada"; \
	fi

test-connection: ## Testa conexão com o PostgreSQL
	@echo "🔍 Testando conexão com PostgreSQL..."
	@docker exec postgres-dados-rfb pg_isready -U postgres
	@docker exec postgres-dados-rfb psql -U postgres -d Dados_RFB -c "SELECT version();" || echo "Banco ainda não está pronto"

backup-db: ## Cria backup do banco de dados
	@echo "💾 Criando backup do banco de dados..."
	@mkdir -p backups
	@docker exec postgres-dados-rfb pg_dump -U postgres Dados_RFB | gzip > backups/backup_$$(date +%Y%m%d_%H%M%S).sql.gz
	@echo "✓ Backup criado em backups/"

stats: ## Mostra estatísticas de uso do Docker
	@echo "📊 Estatísticas do Docker:"
	@docker system df

first-run: setup init up ## Configuração completa do zero (setup + init + up)
	@echo ""
	@echo "🎉 Tudo pronto!"
	@echo ""
	@echo "Próximos passos:"
	@echo "  1. Acesse http://localhost:8080"
	@echo "  2. Faça login com: airflow / airflow"
	@echo "  3. Ative a DAG 'etl_receita_federal'"
	@echo "  4. Execute a DAG clicando no botão Play"
	@echo ""
	@echo "⏱️  O processo completo pode levar várias horas!"

