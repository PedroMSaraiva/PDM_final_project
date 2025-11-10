# Arquitetura do Sistema - ETL Receita Federal

## 📐 Visão Geral

Este documento descreve a arquitetura dockerizada do sistema de ETL para dados públicos de CNPJ da Receita Federal do Brasil.

## 🏗️ Diagrama de Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE                            │
│                                                                   │
│  ┌───────────────────┐          ┌──────────────────┐            │
│  │  Airflow Webserver│          │ Airflow Scheduler│            │
│  │  (porta 8080)     │◄────────►│                  │            │
│  │                   │          │  - Orquestra ETL │            │
│  │  - Interface Web  │          │  - Executa DAGs  │            │
│  │  - Monitoramento  │          │  - Task Manager  │            │
│  └─────────┬─────────┘          └────────┬─────────┘            │
│            │                              │                      │
│            │                              │                      │
│            ▼                              ▼                      │
│  ┌─────────────────────────────────────────────────┐            │
│  │     PostgreSQL - Airflow Metadata               │            │
│  │     (porta 5433)                                 │            │
│  │     - Armazena metadados do Airflow              │            │
│  │     - Histórico de execuções                     │            │
│  │     - Status das tasks                           │            │
│  └──────────────────────────────────────────────────┘            │
│                                                                   │
│                              │                                    │
│                              │ ETL Process                        │
│                              ▼                                    │
│  ┌─────────────────────────────────────────────────┐            │
│  │     PostgreSQL - Dados RFB                      │            │
│  │     (porta 5432)                                 │            │
│  │                                                  │            │
│  │  Tabelas:                                        │            │
│  │  ├─ empresa (45M+ registros)                    │            │
│  │  ├─ estabelecimento (48M+ registros)            │            │
│  │  ├─ socios (20M+ registros)                     │            │
│  │  ├─ simples (27M+ registros)                    │            │
│  │  └─ Tabelas auxiliares (cnae, moti, etc.)       │            │
│  └──────────────────────────────────────────────────┘            │
│                              ▲                                    │
│                              │                                    │
│  ┌───────────────────────────┴──────────────────┐                │
│  │            PgAdmin                            │                │
│  │            (porta 5050)                       │                │
│  │            - Gerenciamento visual             │                │
│  │            - Queries e consultas              │                │
│  └───────────────────────────────────────────────┘                │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Download via HTTP
                              ▼
                ┌──────────────────────────────┐
                │   Receita Federal do Brasil  │
                │   http://200.152.38.155/CNPJ/│
                │                              │
                │   - Arquivos ZIP (~17GB)     │
                │   - Dados de CNPJ            │
                └──────────────────────────────┘
```

## 🔄 Fluxo de Dados (ETL Pipeline)

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAG: etl_receita_federal                      │
│                                                                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │  Task: executar_etl_receita_federal                    │     │
│  │                                                          │     │
│  │  Executa: ETL_coletar_dados_e_gravar_BD.py             │     │
│  │                                                          │     │
│  │  1️⃣  EXTRACT (Download & Extração)                     │     │
│  │     - Baixa arquivos ZIP da Receita Federal            │     │
│  │     - Extrai arquivos CSV                              │     │
│  │                                                          │     │
│  │  2️⃣  TRANSFORM & LOAD                                  │     │
│  │     - load_empresa (45M+ registros)                    │     │
│  │     - load_estabelecimento (48M+ registros)            │     │
│  │     - load_socios (20M+ registros)                     │     │
│  │     - load_simples (27M+ registros)                    │     │
│  │     - Tabelas auxiliares (cnae, moti, munic, etc.)     │     │
│  │                                                          │     │
│  │  3️⃣  INDEXAÇÃO (Otimização)                            │     │
│  │     - Cria índices em cnpj_basico                      │     │
│  │                                                          │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Download via HTTP
                              ▼
                ┌──────────────────────────────┐
                │   Receita Federal do Brasil  │
                │   http://200.152.38.155/CNPJ/│
                │                              │
                │   - Arquivos ZIP (~17GB)     │
                │   - Dados de CNPJ            │
                └──────────────────────────────┘
```

## 🗂️ Estrutura de Dados

### Volumes Docker

```
/opt/airflow/                          (Container)
├── dags/                              → etl_receita_federal_dag.py
├── logs/                              → Logs do Airflow
├── plugins/                           → Plugins customizados
├── data/
│   ├── downloads/                     → Arquivos ZIP (~17GB)
│   └── extracted/                     → Arquivos CSV (~60GB)
└── etl_scripts/                       → etl_receita_federal.py
```

### Banco de Dados - Modelo ER

```
┌─────────────┐         ┌───────────────────┐
│   empresa   │         │ estabelecimento   │
├─────────────┤         ├───────────────────┤
│cnpj_basico●─┼────────►│cnpj_basico       │
│razao_social │         │cnpj_ordem         │
│...          │         │nome_fantasia      │
└─────────────┘         │endereco           │
                        │telefone           │
      │                 │...                │
      │                 └───────────────────┘
      │
      │                 ┌───────────────────┐
      └────────────────►│     socios        │
                        ├───────────────────┤
                        │cnpj_basico        │
                        │nome_socio         │
                        │cpf_cnpj_socio     │
                        │...                │
                        └───────────────────┘
      │
      │                 ┌───────────────────┐
      └────────────────►│     simples       │
                        ├───────────────────┤
                        │cnpj_basico        │
                        │opcao_simples      │
                        │opcao_mei          │
                        └───────────────────┘

Tabelas Auxiliares:
┌──────┐  ┌──────┐  ┌───────┐  ┌───────┐  ┌──────┐  ┌───────┐
│ cnae │  │ moti │  │ munic │  │ natju │  │ pais │  │ quals │
└──────┘  └──────┘  └───────┘  └───────┘  └──────┘  └───────┘
```

## ⚙️ Tecnologias Utilizadas

| Componente | Tecnologia | Versão | Função |
|------------|-----------|---------|---------|
| Orquestração | Apache Airflow | 2.8.1 | Gerenciamento do pipeline ETL |
| Banco de Dados | PostgreSQL | 14 | Armazenamento dos dados |
| Containerização | Docker | 20.10+ | Isolamento de ambientes |
| Gerenciamento | Docker Compose | 2.0+ | Orquestração de containers |
| Processamento | Python + Pandas | 3.11 | ETL e transformação de dados |
| Interface DB | PgAdmin | Latest | Gerenciamento visual do PostgreSQL |

## 🔐 Segurança

### Portas Expostas

| Porta | Serviço | Acesso |
|-------|---------|---------|
| 8080 | Airflow Web UI | http://localhost:8080 |
| 5050 | PgAdmin | http://localhost:5050 |
| 5432 | PostgreSQL (Dados RFB) | localhost:5432 |
| 5433 | PostgreSQL (Airflow) | localhost:5433 |

### Credenciais Padrão

⚠️ **IMPORTANTE**: Alterar em produção!

```yaml
Airflow Web UI:
  Usuário: airflow
  Senha: airflow

PgAdmin:
  Email: admin@admin.com
  Senha: admin

PostgreSQL (Dados RFB):
  Usuário: postgres
  Senha: postgres
  Database: Dados_RFB

PostgreSQL (Airflow):
  Usuário: airflow
  Senha: airflow
  Database: airflow
```

## 📊 Métricas de Performance

### Recursos Recomendados

| Recurso | Mínimo | Recomendado | Ideal |
|---------|--------|-------------|-------|
| CPU | 4 cores | 8 cores | 16+ cores |
| RAM | 8 GB | 16 GB | 32+ GB |
| Disco | 50 GB | 100 GB | 200+ GB |
| Internet | 10 Mbps | 50 Mbps | 100+ Mbps |

### Tempo de Execução Estimado

| Etapa | Tempo Estimado | Tamanho |
|-------|----------------|---------|
| Download | 30-90 min | ~17 GB |
| Extração | 10-20 min | ~60 GB |
| Load Empresa | 30-60 min | 45M registros |
| Load Estabelecimento | 60-120 min | 48M registros |
| Load Sócios | 30-60 min | 20M registros |
| Load Simples | 45-90 min | 27M registros |
| Tabelas Auxiliares | 5-10 min | ~10K registros |
| Criação de Índices | 20-40 min | - |
| **TOTAL** | **4-8 horas** | **~140M registros** |

## 🔍 Monitoramento

### Logs Importantes

```bash
# Ver todos os logs
docker-compose logs -f

# Logs específicos do scheduler (onde o ETL roda)
docker-compose logs -f airflow-scheduler

# Logs do PostgreSQL
docker-compose logs -f postgres-dados-rfb

# Entrar no container para debug
docker exec -it airflow-scheduler bash
```

### Queries de Monitoramento

```sql
-- Conectar: docker exec -it postgres-dados-rfb psql -U postgres -d Dados_RFB

-- Ver todas as tabelas
\dt

-- Contar registros
SELECT 'empresa' as tabela, COUNT(*) FROM empresa
UNION ALL
SELECT 'estabelecimento', COUNT(*) FROM estabelecimento
UNION ALL
SELECT 'socios', COUNT(*) FROM socios
UNION ALL
SELECT 'simples', COUNT(*) FROM simples;

-- Verificar índices
\di

-- Tamanho das tabelas
SELECT
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
```

## 🚀 Escalabilidade

### Opções para Melhorar Performance

1. **Aumentar recursos do container**
   ```yaml
   # No docker-compose.yml
   deploy:
     resources:
       limits:
         cpus: '8'
         memory: 16G
   ```

2. **Paralelização no Airflow**
   ```python
   # Na DAG, ajustar max_active_tasks
   default_args = {
       'max_active_tasks': 4
   }
   ```

3. **Otimizações do PostgreSQL**
   ```sql
   -- Ajustar configurações para bulk insert
   SET maintenance_work_mem = '2GB';
   SET max_wal_size = '4GB';
   ```

## 📚 Referências

- [Apache Airflow Documentation](https://airflow.apache.org/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [Dados Abertos CNPJ - Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)

## 📝 Notas de Versão

- **v1.0.0** (2024-01): Dockerização inicial com Airflow
  - Migração do script standalone para DAG do Airflow
  - Configuração de PostgreSQL dual (Airflow + Dados)
  - Adição de PgAdmin para gerenciamento
  - Scripts automatizados de setup

