# 🔧 Correção do Erro da DAG

## ❌ Problema Identificado

O script `ETL_coletar_dados_e_gravar_BD.py` estava tentando fazer um `input()` interativo para pedir o caminho do arquivo `.env`, mas isso **não funciona** em ambientes Docker/Airflow (não-interativos).

### Erro Original:
```
EOFError: EOF when reading a line
local_env = input()
```

## ✅ Solução Implementada

### 1. **Modificação no Script** (`ETL_coletar_dados_e_gravar_BD.py`)

**Antes** (linhas 73-81):
```python
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, '.env')
if not os.path.isfile(dotenv_path):
    print('Especifique o local do seu arquivo de configuração ".env"...')
    local_env = input()  # ❌ ERRO: não funciona no Docker!
    dotenv_path = os.path.join(local_env, '.env')
print(dotenv_path)
load_dotenv(dotenv_path=dotenv_path)
```

**Depois** (linhas 73-92):
```python
# Procurar arquivo .env em locais possíveis (Docker-friendly)
possible_paths = [
    os.path.join(pathlib.Path().resolve(), '.env'),  # Diretório atual
    os.path.join('/opt/airflow/etl_scripts', '.env'),  # Docker: etl_scripts
    os.path.join('/opt/airflow', '.env'),  # Docker: raiz do airflow
]

dotenv_path = None
for path in possible_paths:
    if os.path.isfile(path):
        dotenv_path = path
        break

if dotenv_path:
    print(f'Arquivo .env encontrado: {dotenv_path}')
    load_dotenv(dotenv_path=dotenv_path)
else:
    print('Arquivo .env não encontrado. Usando variáveis de ambiente do sistema.')
    load_dotenv()  # ✅ Usa variáveis de ambiente diretas (Docker)
```

### 2. **Variáveis de Ambiente no Docker Compose**

As variáveis já estão configuradas no `docker-compose.yml`:

```yaml
environment:
  # Variáveis para o ETL
  DB_HOST: postgres-dados-rfb
  DB_PORT: 5432
  DB_USER: postgres
  DB_PASSWORD: postgres
  DB_NAME: Dados_RFB
  OUTPUT_FILES_PATH: /opt/airflow/data/downloads
  EXTRACTED_FILES_PATH: /opt/airflow/data/extracted
```

## 🚀 Como Aplicar a Correção

### Opção 1: Reiniciar os Serviços (Recomendado)

```bash
cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master

# Parar todos os serviços
docker compose down

# Iniciar novamente
docker compose up -d

# Verificar logs
docker compose logs -f airflow-scheduler
```

### Opção 2: Reiniciar Apenas o Scheduler

```bash
# Reiniciar apenas o scheduler (onde a DAG roda)
docker compose restart airflow-scheduler

# Ver logs em tempo real
docker compose logs -f airflow-scheduler
```

### Opção 3: Usando Make

```bash
make restart
make logs
```

## 🧪 Testando a Correção

### 1. Acesse o Airflow Web UI
```
http://localhost:8080
Usuário: airflow
Senha: airflow
```

### 2. Encontre a DAG `etl_receita_federal`
- Vá para a lista de DAGs
- Procure por `etl_receita_federal`

### 3. Execute Manualmente
- Clique no botão ▶️ (Play) na DAG
- Selecione "Trigger DAG"
- Aguarde alguns segundos

### 4. Verifique os Logs
- Clique na DAG executada
- Clique na task `executar_etl_receita_federal`
- Clique em "Log"

### ✅ **Sucesso esperado:**
```
[INFO] Arquivo .env não encontrado. Usando variáveis de ambiente do sistema.
[INFO] Diretórios definidos:
[INFO] output_files: /opt/airflow/data/downloads
[INFO] extracted_files: /opt/airflow/data/extracted
[INFO] Arquivos que serão baixados:
[INFO] 1 - EMPRESA...
[INFO] 2 - ESTABELE...
...
```

### ❌ **Se ainda der erro:**
```bash
# Ver logs detalhados
docker compose logs airflow-scheduler | grep -A 20 "ETL_coletar"

# Verificar se as variáveis estão disponíveis
docker exec -it airflow-scheduler env | grep DB_
docker exec -it airflow-scheduler env | grep FILES_PATH

# Verificar se o script está no lugar certo
docker exec -it airflow-scheduler ls -la /opt/airflow/etl_scripts/
```

## 📊 Monitoramento Durante a Execução

### Ver Progresso em Tempo Real
```bash
# Logs do scheduler (onde o ETL roda)
docker compose logs -f airflow-scheduler

# Status dos containers
docker compose ps

# Recursos usados
docker stats
```

### Verificar Banco de Dados
```bash
# Entrar no PostgreSQL
docker exec -it postgres-dados-rfb psql -U postgres -d Dados_RFB

# Listar tabelas criadas
\dt

# Contar registros (enquanto está rodando)
SELECT 'empresa' as tabela, COUNT(*) FROM empresa;
SELECT 'estabelecimento' as tabela, COUNT(*) FROM estabelecimento;

# Sair
\q
```

## ⏱️ Tempo Estimado de Execução

A execução completa do ETL pode levar **4 a 8 horas**, dependendo de:
- Velocidade da internet (download dos ~17GB)
- CPU e RAM disponíveis
- Velocidade do disco

### Progresso Esperado:
```
[00:00] Iniciando download dos arquivos ZIP...
[00:30] Baixando arquivos... (~17GB)
[01:30] Extraindo arquivos... (~60GB descompactados)
[02:00] Carregando tabela: empresa (45M registros)
[03:00] Carregando tabela: estabelecimento (48M registros)
[04:30] Carregando tabela: socios (20M registros)
[05:30] Carregando tabela: simples (27M registros)
[06:00] Carregando tabelas auxiliares
[06:30] Criando índices
[07:00] ✅ Processo finalizado!
```

## 🆘 Troubleshooting

### Problema: "Conexão com o banco recusada"
```bash
# Verificar se o PostgreSQL está rodando
docker compose ps postgres-dados-rfb

# Verificar logs do PostgreSQL
docker compose logs postgres-dados-rfb

# Reiniciar PostgreSQL
docker compose restart postgres-dados-rfb
```

### Problema: "Disco cheio"
```bash
# Verificar espaço em disco
df -h

# Limpar volumes não usados
docker system prune -a --volumes
```

### Problema: "Memória insuficiente"
```yaml
# Adicionar ao docker-compose.yml (serviço airflow-scheduler)
deploy:
  resources:
    limits:
      memory: 8G
    reservations:
      memory: 4G
```

### Problema: "Task timeout"
No `docker-compose.yml`, adicionar:
```yaml
environment:
  AIRFLOW__CORE__TASK_EXECUTION_TIMEOUT: 28800  # 8 horas
```

## 📝 Notas Importantes

1. **Primeira Execução**: A primeira execução sempre demora mais (download + extração)
2. **Execuções Subsequentes**: Se os arquivos já estiverem baixados, pulam o download
3. **Espaço em Disco**: Reserve pelo menos **100GB livres** (~17GB ZIP + ~60GB CSV + ~30GB BD)
4. **Memória RAM**: Recomendado **16GB** para processamento eficiente
5. **Internet**: Conexão estável é essencial para download dos 17GB

## 🎯 Resultado Final Esperado

Ao final da execução bem-sucedida:

✅ Tabelas criadas:
- `empresa` (45M+ registros)
- `estabelecimento` (48M+ registros)
- `socios` (20M+ registros)
- `simples` (27M+ registros)
- Tabelas auxiliares (cnae, moti, munic, natju, pais, quals)

✅ Índices criados em `cnpj_basico` para todas as tabelas principais

✅ Dados prontos para consultas e análises!

## 📚 Referências

- [README-DOCKER.md](./README-DOCKER.md) - Documentação completa
- [QUICK-REFERENCE.md](./QUICK-REFERENCE.md) - Guia rápido de comandos
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Arquitetura do sistema

---

**Status**: ✅ Correção Aplicada  
**Data**: 2025-11-10  
**Tipo**: Adaptação para ambiente Docker não-interativo

