# ETL Receita Federal - Dockerizado com Airflow

Este projeto foi dockerizado para executar o ETL dos dados públicos de CNPJ da Receita Federal utilizando Apache Airflow e PostgreSQL.

## 🏗️ Arquitetura

A solução utiliza Docker Compose com os seguintes serviços:

- **postgres-dados-rfb**: PostgreSQL 14 para armazenar os dados da Receita Federal
- **postgres-airflow**: PostgreSQL 14 para metadados do Airflow
- **airflow-webserver**: Interface web do Airflow (porta 8080)
- **airflow-scheduler**: Orquestrador de tarefas do Airflow
- **pgadmin**: Interface web para gerenciar os bancos PostgreSQL (porta 5050)

## 📋 Pré-requisitos

- Docker (versão 20.10 ou superior)
- Docker Compose (versão 2.0 ou superior)
- Pelo menos 8GB de RAM livre
- Pelo menos 50GB de espaço em disco livre (os dados da Receita Federal são grandes!)

## 🚀 Como usar

### 1. Configuração inicial

Primeiro, certifique-se de estar no diretório correto:

```bash
cd /home/saraiva/Documents/BIA/6p/PDM/TrabalhoFinal/dados/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master
```

### 2. Criar arquivo .env

Copie o conteúdo abaixo e crie um arquivo `.env` na raiz do projeto:

```bash
cat > .env << 'EOF'
# Configurações do Docker Compose
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# Credenciais do Airflow Web UI
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Bibliotecas Python adicionais
_PIP_ADDITIONAL_REQUIREMENTS=beautifulsoup4>=4.9.3 bs4>=0.0.1 lxml>=4.6.3 numpy>=1.20.3 pandas>=1.2.4 psycopg2-binary>=2.9.1 python-dotenv==1.0.0 requests==2.30.0 SQLAlchemy>=1.4.18 wget>=3.2

# Configurações do ETL
OUTPUT_FILES_PATH=/opt/airflow/data/downloads
EXTRACTED_FILES_PATH=/opt/airflow/data/extracted

# Configurações do PostgreSQL - Dados RFB
DB_HOST=postgres-dados-rfb
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=Dados_RFB
EOF
```

### 3. Criar diretórios necessários

```bash
mkdir -p dags logs plugins data/downloads data/extracted
```

### 4. Inicializar o Airflow

```bash
# Inicializar banco de dados e criar usuário admin
docker-compose up airflow-init

# Iniciar todos os serviços
docker-compose up -d
```

### 5. Acessar as interfaces

Aguarde alguns minutos para os serviços iniciarem completamente. Então acesse:

- **Airflow Web UI**: http://localhost:8080
  - Usuário: `airflow`
  - Senha: `airflow`

- **PgAdmin**: http://localhost:5050
  - Email: `admin@admin.com`
  - Senha: `admin`

### 6. Executar o ETL

1. Acesse o Airflow Web UI em http://localhost:8080
2. Faça login com as credenciais acima
3. Localize a DAG `etl_receita_federal`
4. Ative a DAG (toggle no lado esquerdo)
5. Clique no botão "Play" para executar manualmente

## 📊 Monitoramento

### Logs do Airflow

```bash
# Ver logs de todos os serviços
docker-compose logs -f

# Ver logs apenas do scheduler
docker-compose logs -f airflow-scheduler

# Ver logs apenas do webserver
docker-compose logs -f airflow-webserver
```

### Conectar ao PostgreSQL

#### Via PgAdmin (Interface Web)

1. Acesse http://localhost:5050
2. Faça login
3. Clique com botão direito em "Servers" → "Register" → "Server"
4. Na aba "General": Nome = "Dados RFB"
5. Na aba "Connection":
   - Host: `postgres-dados-rfb`
   - Port: `5432`
   - Database: `Dados_RFB`
   - Username: `postgres`
   - Password: `postgres`

#### Via linha de comando

```bash
# Conectar ao banco de dados da Receita Federal
docker exec -it postgres-dados-rfb psql -U postgres -d Dados_RFB

# Exemplos de consultas
\dt  # Listar tabelas
SELECT COUNT(*) FROM empresa;
SELECT * FROM empresa LIMIT 10;
```

## 🔧 Comandos úteis

```bash
# Parar todos os serviços
docker-compose down

# Parar e remover volumes (CUIDADO: apaga os dados!)
docker-compose down -v

# Reiniciar um serviço específico
docker-compose restart airflow-scheduler

# Ver status dos serviços
docker-compose ps

# Entrar no container do Airflow
docker exec -it airflow-scheduler bash

# Ver espaço em disco usado
docker system df
```

## 📁 Estrutura de diretórios

```
.
├── docker-compose.yml          # Configuração dos containers
├── init-db.sql                 # Script de inicialização do PostgreSQL
├── README-DOCKER.md            # Este arquivo
├── requirements-airflow.txt    # Dependências Python
├── dags/                       # DAGs do Airflow
│   └── etl_receita_federal_dag.py
├── code/                       # Scripts ETL
│   └── etl_receita_federal.py
├── logs/                       # Logs do Airflow (criado automaticamente)
├── plugins/                    # Plugins customizados (opcional)
└── data/                       # Dados baixados e extraídos
    ├── downloads/              # Arquivos ZIP baixados
    └── extracted/              # Arquivos CSV extraídos
```

## 🎯 Fluxo do ETL

A DAG `etl_receita_federal` executa uma única tarefa:

1. **executar_etl_receita_federal**: Executa o script completo `ETL_coletar_dados_e_gravar_BD.py`
   - Baixa todos os arquivos ZIP da Receita Federal
   - Extrai os dados
   - Carrega todas as tabelas (empresa, estabelecimento, sócios, simples, etc.)
   - Cria índices automaticamente

### Tempo estimado

⏱️ **ATENÇÃO**: O processo completo pode levar **várias horas** (4-8 horas), dependendo da velocidade da internet e do hardware.

## 🛑 Troubleshooting

### Erro "Bind for 0.0.0.0:5432 failed: port is already allocated"

Você já tem um PostgreSQL rodando na porta 5432. Opções:
1. Parar o PostgreSQL local: `sudo systemctl stop postgresql`
2. Ou alterar a porta no `docker-compose.yml`

### Erro "No space left on device"

Os dados da Receita Federal são grandes (dezenas de GB). Libere espaço em disco:

```bash
# Ver uso de disco do Docker
docker system df

# Limpar imagens não utilizadas
docker system prune -a
```

### DAG não aparece no Airflow

1. Verifique se o arquivo está em `dags/etl_receita_federal_dag.py`
2. Verifique os logs: `docker-compose logs airflow-scheduler`
3. Reinicie o scheduler: `docker-compose restart airflow-scheduler`

### Erro de conexão com PostgreSQL

Aguarde alguns minutos para o PostgreSQL inicializar completamente. Verifique:

```bash
docker-compose ps
docker-compose logs postgres-dados-rfb
```

## 📝 Tabelas geradas

Após a execução completa, as seguintes tabelas estarão disponíveis:

- **empresa**: Dados cadastrais das empresas (matriz)
- **estabelecimento**: Dados por estabelecimento/filial
- **socios**: Dados dos sócios
- **simples**: Dados de MEI e Simples Nacional
- **cnae**: Códigos e descrições de CNAE
- **moti**: Motivos de situação cadastral
- **munic**: Municípios
- **natju**: Naturezas jurídicas
- **pais**: Países
- **quals**: Qualificação de sócios

## 🔒 Segurança

⚠️ **IMPORTANTE**: Esta configuração é para desenvolvimento/teste. Para produção:

- Altere todas as senhas padrão
- Use secrets do Docker para credenciais
- Configure HTTPS
- Restrinja acesso às portas
- Configure backups automáticos

## 📚 Referências

- [Dados Públicos CNPJ - Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- [Documentação Apache Airflow](https://airflow.apache.org/docs/)
- [PostgreSQL Docker Hub](https://hub.docker.com/_/postgres)

## 👤 Créditos

- **Script ETL original**: Aphonso Henrique do Amaral Rafael
- **Dockerização e Airflow**: Adaptado para uso em containers
- **Repositório original**: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ

## 📄 Licença

Consulte o arquivo LICENSE no repositório original.

