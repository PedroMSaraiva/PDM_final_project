#!/usr/bin/env python3
"""
Loader para carregar dados da Receita Federal (Estabelecimentos CNPJ) do GCS para o BigQuery

Estrutura dos dados no bucket:
gs://bucket/receita_federal/
├── 2023-01/
│   └── *.ESTABELE
├── 2023-02/
│   └── *.ESTABELE
└── ...

Cria 1 tabela consolidada no BigQuery com:
- Todos os campos como STRING
- Coluna adicional: ano_mes (formato YYYY-MM)
- Delimitador: ; (ponto e vírgula)
"""

from google.cloud import bigquery
from google.cloud import storage
from typing import List, Dict
import time
import re


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PROJECT_ID = "trabalho-final-pdm-478021"
DATASET_ID = "main_database"
TABLE_NAME = "receita_estabelecimentos"
BUCKET_NAME = "dados-cnpjs"
BASE_PATH = "receita_federal"

# Schema da Receita Federal (Estabelecimentos) - Todos como STRING
# Baseado no layout oficial da Receita Federal
ESTABELECIMENTOS_SCHEMA = [
    bigquery.SchemaField("cnpj_basico", "STRING"),
    bigquery.SchemaField("cnpj_ordem", "STRING"),
    bigquery.SchemaField("cnpj_dv", "STRING"),
    bigquery.SchemaField("identificador_matriz_filial", "STRING"),
    bigquery.SchemaField("nome_fantasia", "STRING"),
    bigquery.SchemaField("situacao_cadastral", "STRING"),
    bigquery.SchemaField("data_situacao_cadastral", "STRING"),
    bigquery.SchemaField("motivo_situacao_cadastral", "STRING"),
    bigquery.SchemaField("nome_cidade_exterior", "STRING"),
    bigquery.SchemaField("pais", "STRING"),
    bigquery.SchemaField("data_inicio_atividade", "STRING"),
    bigquery.SchemaField("cnae_fiscal_principal", "STRING"),
    bigquery.SchemaField("cnae_fiscal_secundaria", "STRING"),
    bigquery.SchemaField("tipo_logradouro", "STRING"),
    bigquery.SchemaField("logradouro", "STRING"),
    bigquery.SchemaField("numero", "STRING"),
    bigquery.SchemaField("complemento", "STRING"),
    bigquery.SchemaField("bairro", "STRING"),
    bigquery.SchemaField("cep", "STRING"),
    bigquery.SchemaField("uf", "STRING"),
    bigquery.SchemaField("municipio", "STRING"),
    bigquery.SchemaField("ddd_1", "STRING"),
    bigquery.SchemaField("telefone_1", "STRING"),
    bigquery.SchemaField("ddd_2", "STRING"),
    bigquery.SchemaField("telefone_2", "STRING"),
    bigquery.SchemaField("ddd_fax", "STRING"),
    bigquery.SchemaField("fax", "STRING"),
    bigquery.SchemaField("correio_eletronico", "STRING"),
    bigquery.SchemaField("situacao_especial", "STRING"),
    bigquery.SchemaField("data_situacao_especial", "STRING"),
    bigquery.SchemaField("ano_mes", "STRING"),  # ← Coluna adicional com período (YYYY-MM)
]


# =============================================================================
# FUNÇÕES
# =============================================================================

def get_available_periods() -> List[str]:
    """
    Lista todos os períodos (ano-mês) disponíveis no bucket GCS
    
    Returns:
        Lista de períodos no formato YYYY-MM
    """
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Listar blobs dentro de receita_federal/
    blobs = bucket.list_blobs(prefix=f"{BASE_PATH}/", delimiter="/")
    
    # Forçar iteração dos blobs para popular prefixes
    _ = list(blobs)
    
    periods = set()
    
    # Método 1: Extrair de prefixes
    if blobs.prefixes:
        for prefix in blobs.prefixes:
            # Extrair ano-mes do prefixo: receita_federal/2023-05/ -> 2023-05
            match = re.search(r'(\d{4}-\d{2})/?$', prefix)
            if match:
                periods.add(match.group(1))
    
    # Método 2 (fallback): Listar todos os blobs e extrair períodos dos paths
    if not periods:
        print("   ⚠️  Usando método alternativo para listar períodos...")
        all_blobs = bucket.list_blobs(prefix=f"{BASE_PATH}/")
        for blob in all_blobs:
            # Extrair ano-mes do path: receita_federal/2023-05/arquivo.csv -> 2023-05
            match = re.search(rf'{BASE_PATH}/(\d{{4}}-\d{{2}})/', blob.name)
            if match:
                periods.add(match.group(1))
    
    return sorted(list(periods))


def create_load_job_config_temp(write_mode: str = "WRITE_TRUNCATE") -> bigquery.LoadJobConfig:
    """
    Cria configuração para o job de carga temporária (sem coluna ano_mes)
    
    Args:
        write_mode: WRITE_TRUNCATE (substitui) ou WRITE_APPEND (adiciona)
    """
    # Schema sem a coluna ano_mes (será adicionada depois)
    schema_without_ano_mes = [field for field in ESTABELECIMENTOS_SCHEMA if field.name != "ano_mes"]
    
    return bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",  # ← Delimitador ponto e vírgula
        skip_leading_rows=0,  # Arquivos da Receita não têm cabeçalho
        write_disposition=write_mode,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10000,
        encoding='ISO-8859-1',  # Encoding dos arquivos da Receita Federal
        schema=schema_without_ano_mes,
    )


def create_final_table(client: bigquery.Client):
    """
    Cria a tabela final com o schema completo (incluindo ano_mes)
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    
    try:
        table = bigquery.Table(table_ref, schema=ESTABELECIMENTOS_SCHEMA)
        table = client.create_table(table)
        print(f"✅ Tabela {TABLE_NAME} criada com sucesso!")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"ℹ️  Tabela {TABLE_NAME} já existe")
        else:
            print(f"⚠️  Aviso ao criar tabela: {e}")


def load_period_to_temp(client: bigquery.Client, ano_mes: str) -> Dict:
    """
    Carrega dados de um período específico para uma tabela temporária
    
    Args:
        client: Cliente do BigQuery
        ano_mes: Período no formato YYYY-MM
        
    Returns:
        Dicionário com status e estatísticas
    """
    temp_table = f"{DATASET_ID}.temp_receita_{ano_mes.replace('-', '_')}"
    
    # URI para arquivos *.ESTABELE do período
    uri = f"gs://{BUCKET_NAME}/{BASE_PATH}/{ano_mes}/*.ESTABELE"
    
    print(f"   📥 Carregando {ano_mes} para tabela temporária...")
    print(f"      URI: {uri}")
    
    # Configurar e iniciar job de carga
    job_config = create_load_job_config_temp("WRITE_TRUNCATE")
    load_job = client.load_table_from_uri(uri, temp_table, job_config=job_config)
    
    try:
        load_job.result()
        rows = load_job.output_rows or 0
        print(f"   ✅ {ano_mes}: {rows:,} linhas carregadas na tabela temporária")
        return {'status': 'success', 'rows': rows}
    except Exception as e:
        print(f"   ❌ {ano_mes}: Erro - {str(e)[:100]}")
        return {'status': 'error', 'error': str(e)}


def insert_from_temp_to_final(client: bigquery.Client, ano_mes: str, is_first: bool) -> Dict:
    """
    Insere dados da tabela temporária na tabela final, adicionando coluna ano_mes
    
    Args:
        client: Cliente do BigQuery
        ano_mes: Período no formato YYYY-MM
        is_first: Se True, usa WRITE_TRUNCATE; se False, usa WRITE_APPEND
        
    Returns:
        Dicionário com status e estatísticas
    """
    temp_table = f"{DATASET_ID}.temp_receita_{ano_mes.replace('-', '_')}"
    final_table = f"{DATASET_ID}.{TABLE_NAME}"
    
    print(f"   📤 Inserindo {ano_mes} na tabela final...")
    
    # Query para inserir dados adicionando a coluna ano_mes
    write_mode = "WRITE_TRUNCATE" if is_first else "WRITE_APPEND"
    
    query = f"""
    SELECT
        *,
        '{ano_mes}' AS ano_mes
    FROM `{PROJECT_ID}.{temp_table}`
    """
    
    job_config = bigquery.QueryJobConfig(
        destination=f"{PROJECT_ID}.{final_table}",
        write_disposition=write_mode,
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        
        rows = query_job.num_dml_affected_rows or 0
        print(f"   ✅ {ano_mes}: {rows:,} linhas inseridas na tabela final")
        
        # Deletar tabela temporária
        client.delete_table(f"{PROJECT_ID}.{temp_table}", not_found_ok=True)
        print(f"   🗑️  {ano_mes}: Tabela temporária removida")
        
        return {'status': 'success', 'rows': rows}
    except Exception as e:
        print(f"   ❌ {ano_mes}: Erro na inserção - {str(e)[:100]}")
        return {'status': 'error', 'error': str(e)}


def load_receita_data() -> Dict:
    """
    Carrega todos os dados da Receita Federal para o BigQuery
    
    Processo:
    1. Lista todos os períodos disponíveis no GCS
    2. Para cada período:
       a. Carrega arquivos *.ESTABELE em tabela temporária
       b. Insere na tabela final adicionando coluna ano_mes
       c. Remove tabela temporária
    
    Returns:
        Dicionário com estatísticas do processo
    """
    print("=" * 80)
    print("CARREGAR DADOS RECEITA FEDERAL (ESTABELECIMENTOS) PARA BIGQUERY")
    print("=" * 80)
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Tabela: {TABLE_NAME}")
    print(f"Bucket: gs://{BUCKET_NAME}/{BASE_PATH}")
    print()
    
    start_time = time.time()
    
    # Inicializar cliente
    client = bigquery.Client(project=PROJECT_ID)
    
    # Criar tabela final (se não existir)
    create_final_table(client)
    print()
    
    # Listar períodos disponíveis
    print("📂 Listando períodos disponíveis...")
    periods = get_available_periods()
    print(f"   Encontrados: {len(periods)} períodos")
    print(f"   Períodos: {', '.join(periods)}")
    print()
    
    if not periods:
        print("❌ Nenhum período encontrado!")
        return {'status': 'error', 'error': 'Nenhum período encontrado'}
    
    # Processar cada período
    print("📊 Processando períodos...")
    print()
    
    results = []
    total_rows = 0
    
    for idx, ano_mes in enumerate(periods):
        print(f"[{idx + 1}/{len(periods)}] Processando {ano_mes}...")
        
        # 1. Carregar para tabela temporária
        load_result = load_period_to_temp(client, ano_mes)
        
        if load_result['status'] == 'success':
            # 2. Inserir na tabela final
            is_first = (idx == 0)
            insert_result = insert_from_temp_to_final(client, ano_mes, is_first)
            
            if insert_result['status'] == 'success':
                total_rows += insert_result['rows']
                results.append({'period': ano_mes, 'status': 'success', 'rows': insert_result['rows']})
            else:
                results.append({'period': ano_mes, 'status': 'error', 'error': insert_result['error']})
        else:
            results.append({'period': ano_mes, 'status': 'error', 'error': load_result['error']})
        
        print()
    
    elapsed_time = time.time() - start_time
    
    # Resumo final
    print("=" * 80)
    print("RESUMO")
    print("=" * 80)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    error_count = len(results) - success_count
    
    print(f"✅ Períodos processados com sucesso: {success_count}/{len(results)}")
    print(f"❌ Períodos com erro: {error_count}")
    print(f"📊 Total de linhas carregadas: {total_rows:,}")
    print(f"⏱️  Tempo total: {elapsed_time:.1f}s ({elapsed_time/60:.1f} min)")
    print()
    
    # Informações da tabela final
    try:
        table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}")
        print("📋 Informações da Tabela Final:")
        print(f"   Total de linhas: {table.num_rows:,}")
        print(f"   Tamanho: {table.num_bytes / 1024 / 1024 / 1024:.2f} GB")
        print(f"   Colunas: {len(table.schema)}")
    except Exception as e:
        print(f"⚠️  Não foi possível obter informações da tabela: {e}")
    
    print()
    print("🎉 Processo concluído!")
    
    return {
        'status': 'success' if error_count == 0 else 'partial',
        'total_rows': total_rows,
        'periods_processed': success_count,
        'periods_failed': error_count,
        'elapsed_time': elapsed_time,
        'results': results
    }


def load_receita_by_period(ano_mes: str, append: bool = True) -> Dict:
    """
    Carrega dados de um período específico (ano-mês)
    
    Args:
        ano_mes: Período no formato YYYY-MM (ex: "2024-03")
        append: Se True, adiciona dados (WRITE_APPEND); se False, substitui (WRITE_TRUNCATE)
    
    Example:
        load_receita_by_period("2024-03", append=True)
    """
    print("=" * 80)
    print(f"CARREGAR DADOS RECEITA FEDERAL - PERÍODO {ano_mes}")
    print("=" * 80)
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Tabela: {TABLE_NAME}")
    print()
    
    start_time = time.time()
    
    # Inicializar cliente
    client = bigquery.Client(project=PROJECT_ID)
    
    # Criar tabela final (se não existir)
    create_final_table(client)
    print()
    
    # 1. Carregar para tabela temporária
    print(f"📊 Processando {ano_mes}...")
    load_result = load_period_to_temp(client, ano_mes)
    
    if load_result['status'] != 'success':
        return load_result
    
    # 2. Inserir na tabela final
    is_first = not append
    insert_result = insert_from_temp_to_final(client, ano_mes, is_first)
    
    elapsed_time = time.time() - start_time
    
    print()
    print("=" * 80)
    print("RESUMO")
    print("=" * 80)
    
    if insert_result['status'] == 'success':
        print(f"✅ Sucesso!")
        print(f"📊 Linhas carregadas: {insert_result['rows']:,}")
        print(f"⏱️  Tempo: {elapsed_time:.1f}s")
    else:
        print(f"❌ Erro: {insert_result['error']}")
    
    print()
    
    return insert_result


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Função principal - Carrega TODOS os dados automaticamente"""
    
    # Executar carga completa
    result = load_receita_data()
    
    print()
    print("=" * 80)
    if result['status'] == 'success':
        print("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
    elif result['status'] == 'partial':
        print("⚠️  PROCESSO CONCLUÍDO COM ALGUNS ERROS")
    else:
        print("❌ PROCESSO FALHOU")
    print("=" * 80)


if __name__ == "__main__":
    main()

