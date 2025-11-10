# 🚀 Melhorias Implementadas no ETL da Receita Federal

## 📅 Data: 2025-11-10

## ✨ Resumo das Melhorias

O script `ETL_coletar_dados_e_gravar_BD.py` foi completamente refatorado para torná-lo **robusto, modular e inteligente**.

---

## 🎯 Principais Melhorias

### 1. **Auto-Detecção de Pastas de Dados** 🔍

**Antes:**
```python
dados_rf = 'http://200.152.38.155/CNPJ/'  # URL fixa e desatualizada
```

**Depois:**
```python
# Auto-detecta a pasta mais recente disponível
dados_rf = select_data_folder(BASE_URL_RFB, PREFERRED_FOLDER)
# Exemplo resultado: http://200.152.38.155/CNPJ/dados_abertos_cnpj/2024-11/
```

**Benefícios:**
- ✅ **Detecta automaticamente** as pastas disponíveis (2024-08/, 2024-09/, etc)
- ✅ **Seleciona a mais recente** automaticamente
- ✅ **Fallback inteligente** se não conseguir detectar
- ✅ **Configurável** via variável de ambiente `RECEITA_FOLDER`

---

### 2. **Sistema de Retry Robusto** 🔄

**Nova função:**
```python
def make_request_with_retry(url, max_retries=5, timeout=60):
    # Retry automático com tratamento de erros específicos
    # - Timeout
    # - ConnectionError
    # - Outros erros HTTP
```

**Benefícios:**
- ✅ Retry automático em caso de falha
- ✅ Tratamento específico para diferentes tipos de erro
- ✅ Delays progressivos entre tentativas
- ✅ Mensagens claras de erro

---

### 3. **Listagem Inteligente de Pastas** 📁

**Nova função:**
```python
def get_available_folders(base_url):
    # Lista todas as pastas disponíveis (ano-mes)
    # Retorna: ['2024-11/', '2024-10/', '2024-09/', ...]
```

**Como funciona:**
1. Acessa a URL base
2. Procura por padrões YYYY-MM/
3. Ordena por data (mais recente primeiro)
4. Fallback para métodos alternativos se necessário

---

### 4. **Seleção Automática da Pasta Mais Recente** 🎯

**Nova função:**
```python
def select_data_folder(base_url, preferred_folder=None):
    # 1. Se preferred_folder especificada → usa ela
    # 2. Senão → detecta a mais recente
    # 3. Fallback → tenta meses recentes
    # 4. Último fallback → mês atual
```

**Comportamento:**
```
🔍 Buscando pastas disponíveis em: http://200.152.38.155/CNPJ/dados_abertos_cnpj/
✓ Encontradas 8 pastas: 2024-11/, 2024-10/, 2024-09/, 2024-08/, 2024-07/...
✓ Selecionada pasta mais recente: 2024-11/
```

---

### 5. **Melhor Tratamento de Erros** ⚠️

**Antes:**
```python
try:
    # código
except:
    pass  # Silencia todos os erros
```

**Depois:**
```python
except ValueError as e:
    print(f'❌ Erro na definição dos diretórios: {e}')
    print('Verifique o arquivo ".env"')
    sys.exit(1)
except requests.exceptions.Timeout:
    print('❌ ERRO: Timeout - Servidor não respondeu')
    print('   Possíveis causas:...')
    sys.exit(1)
```

**Benefícios:**
- ✅ Erros específicos com mensagens claras
- ✅ Sugestões de solução
- ✅ Exit codes apropriados
- ✅ Nunca silencia erros importantes

---

### 6. **Listagem Robusta de Arquivos ZIP** 📋

**Melhorias:**
- ✅ Busca por tags `<a>` no HTML (mais robusto)
- ✅ Fallback para método antigo se necessário
- ✅ Remove duplicatas
- ✅ Ordena alfabeticamente
- ✅ Valida que encontrou arquivos

**Output melhorado:**
```
📋 Listando arquivos ZIP disponíveis...
✓ Encontrados 23 arquivos para download:
    1. EMPRESA.zip
    2. ESTABELE.zip
    3. SOCIO.zip
    ...
```

---

### 7. **Download com Rastreamento** 📥

**Melhorias:**
- ✅ Contador de progresso (x/total)
- ✅ Estatísticas: baixados, pulados, falhos
- ✅ Tempo de execução
- ✅ Lista de falhas ao final
- ✅ Não re-baixa arquivos existentes

**Output melhorado:**
```
================================================================================
INICIANDO DOWNLOAD DOS ARQUIVOS
================================================================================
Total de arquivos: 23
Destino: /opt/airflow/data/downloads

[1/23] EMPRESA.zip
   ⬇  Baixando de: http://...
   Progresso: 45% [1024 / 2048] bytes
   ✅ Download concluído

[2/23] ESTABELE.zip
   ✓  Arquivo já existe e está atualizado

...

================================================================================
RESUMO DO DOWNLOAD
================================================================================
✓ Baixados:  15
○ Pulados:   8 (já existiam)
✗ Falhos:    0
⏱  Tempo:     450.5s (7.5 min)
```

---

### 8. **Extração com Verificação de Integridade** 📦

**Melhorias:**
- ✅ Testa integridade do ZIP antes de extrair
- ✅ Verifica se já foi extraído
- ✅ Detecta e reporta arquivos corrompidos
- ✅ Estatísticas de extração
- ✅ Não re-extrai arquivos existentes

**Output melhorado:**
```
================================================================================
EXTRAINDO ARQUIVOS
================================================================================
Destino: /opt/airflow/data/extracted

[1/23] EMPRESA.zip
   📦 Extraindo...
   ✅ Extraído com sucesso

[2/23] ESTABELE.zip
   ✓  Já extraído

[3/23] CORRUPTED.zip
   ❌ Arquivo ZIP corrompido: file CRC mismatch

...

================================================================================
RESUMO DA EXTRAÇÃO
================================================================================
✓ Extraídos: 18
✗ Falhos:    0
⏱  Tempo:    120.3s (2.0 min)
```

---

### 9. **Configuração Simplificada** ⚙️

**Nova variável de ambiente opcional:**
```bash
# No .env (OPCIONAL)
RECEITA_FOLDER=2024-11/
```

**Se não especificada:**
- Script detecta automaticamente a pasta mais recente
- Fallback inteligente para meses recentes
- Sempre tenta funcionar mesmo sem configuração manual

---

### 10. **Output Organizado e Informativo** 📊

**Antes:**
```
Baixando arquivo:
1 - EMPRESA.zip
```

**Depois:**
```
================================================================================
CONFIGURAÇÃO DO ETL - RECEITA FEDERAL
================================================================================

✓ Diretórios definidos:
  📂 Downloads: /opt/airflow/data/downloads
  📂 Extraídos: /opt/airflow/data/extracted

🔗 Conectando ao servidor da Receita Federal...
   URL base: http://200.152.38.155/CNPJ/dados_abertos_cnpj/

🔍 Buscando pastas disponíveis...
✓ Encontradas 8 pastas: 2024-11/, 2024-10/, ...
✓ Selecionada pasta mais recente: 2024-11/

📡 Testando conexão com: http://200.152.38.155/CNPJ/dados_abertos_cnpj/2024-11/
✅ Conexão estabelecida com sucesso!
```

---

## 📝 Arquivos Criados/Modificados

| Arquivo | Status | Descrição |
|---------|--------|-----------|
| `ETL_coletar_dados_e_gravar_BD.py` | ✏️ Modificado | Script principal refatorado |
| `.env_docker_template` | ✨ Novo | Template de configuração para Docker |
| `MELHORIAS-ETL.md` | ✨ Novo | Esta documentação |

---

## 🎓 Como Usar

### 1. **Configuração Básica** (Auto-Detecção)

```bash
# Criar .env com configurações mínimas
cd code/
cp .env_docker_template .env

# Editar apenas as variáveis obrigatórias:
# - OUTPUT_FILES_PATH
# - EXTRACTED_FILES_PATH
# - DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

# Executar (vai detectar pasta automaticamente)
python3 ETL_coletar_dados_e_gravar_BD.py
```

### 2. **Configuração com Pasta Específica**

```bash
# No .env, adicionar:
RECEITA_FOLDER=2024-08/

# Executar
python3 ETL_coletar_dados_e_gravar_BD.py
```

### 3. **Execução no Docker/Airflow**

```bash
# As variáveis já estão configuradas no docker-compose.yml
# Basta executar a DAG no Airflow

# Ou executar manualmente dentro do container:
docker exec -it airflow-scheduler bash
cd /opt/airflow/etl_scripts
python ETL_coletar_dados_e_gravar_BD.py
```

---

## 🔧 Funções Principais Adicionadas

### `make_request_with_retry(url, max_retries=5, timeout=60)`
Faz requisição HTTP com retry automático

### `get_available_folders(base_url)`
Lista pastas disponíveis (ano-mes) no servidor

### `select_data_folder(base_url, preferred_folder=None)`
Seleciona a pasta de dados inteligentemente

---

## ✅ Benefícios Gerais

| Aspecto | Antes | Depois |
|---------|-------|--------|
| **URL** | Fixa e desatualizada | Auto-detecta a mais recente |
| **Errors** | Silenciados (`pass`) | Tratados com mensagens claras |
| **Retry** | Nenhum | 5 tentativas automáticas |
| **Progress** | Básico | Detalhado com estatísticas |
| **Validation** | Mínima | Integridade completa |
| **Output** | Confuso | Organizado em seções |
| **Config** | Difícil | Simples e opcional |
| **Robustez** | ⭐⭐ | ⭐⭐⭐⭐⭐ |

---

## 🎯 Próximos Passos Sugeridos

1. ✅ **Concluído**: Auto-detecção de pastas
2. ✅ **Concluído**: Sistema de retry robusto
3. ✅ **Concluído**: Melhor tratamento de erros
4. ✅ **Concluído**: Output organizado
5. 🔜 **Futuro**: Paralelização de downloads
6. 🔜 **Futuro**: Cache de metadados
7. 🔜 **Futuro**: Validação de dados antes de inserir no BD

---

## 📞 Suporte

Para problemas ou dúvidas:
1. Verifique os logs detalhados do script
2. Confirme que o `.env` está configurado corretamente
3. Teste a conectividade: `curl -I http://200.152.38.155/CNPJ/dados_abertos_cnpj/`

---

**Status**: ✅ Refatoração Completa  
**Versão**: 2.0  
**Data**: 2025-11-10

