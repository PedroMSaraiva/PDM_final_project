#!/usr/bin/env python3
"""
Cloud Function para carregar dados da Receita Federal (Estabelecimentos e Empresas) do GCS para o BigQuery

Trigger: Pub/Sub
Mensagem esperada:
  - {} (vazio) - Carrega todos os períodos disponíveis (WRITE_TRUNCATE para primeiro, APPEND para demais)
  - {"period": "2024-03"} - Carrega período específico (WRITE_APPEND)
  - {"period": "2024-03", "mode": "WRITE_TRUNCATE"} - Substitui dados do período
  - {"mode": "WRITE_TRUNCATE"} - Substitui TODOS os dados (cuidado!)
  - {"data_type": "empresas"} - Carrega apenas empresas
  - {"data_type": "estabelecimentos"} - Carrega apenas estabelecimentos
  - {"data_type": "all"} ou omitido - Carrega ambos
"""

import os
import json
import base64
import re
from typing import List, Dict, Optional

from google.cloud import bigquery
from google.cloud import storage
import functions_framework


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PROJECT_ID = os.environ.get('PROJECT_ID', 'trabalho-final-pdm-478021')
DATASET_ID = os.environ.get('DATASET_ID', 'main_database')
TABLE_NAME_ESTABELECIMENTOS = os.environ.get('TABLE_NAME_ESTABELECIMENTOS', 'receita_estabelecimentos')
TABLE_NAME_EMPRESAS = os.environ.get('TABLE_NAME_EMPRESAS', 'receita_empresas')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'dados-cnpjs')
BASE_PATH = os.environ.get('BASE_PATH', 'receita_federal')

# Schema da Receita Federal (Estabelecimentos) - Todos como STRING
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
    bigquery.SchemaField("ano_mes", "STRING"),  # Coluna adicional com período
]

# Schema da Receita Federal (Empresas) - Todos como STRING
EMPRESAS_SCHEMA = [
    bigquery.SchemaField("cnpj_basico", "STRING"),
    bigquery.SchemaField("razao_social", "STRING"),
    bigquery.SchemaField("natureza_juridica", "STRING"),
    bigquery.SchemaField("qualificacao_responsavel", "STRING"),
    bigquery.SchemaField("capital_social", "STRING"),
    bigquery.SchemaField("porte_empresa", "STRING"),
    bigquery.SchemaField("ente_federativo_responsavel", "STRING"),
    bigquery.SchemaField("ano_mes", "STRING"),  # Coluna adicional com período
]

# Configuração de tipos de dados
DATA_TYPES_CONFIG = {
    "estabelecimentos": {
        "schema": ESTABELECIMENTOS_SCHEMA,
        "table_name": TABLE_NAME_ESTABELECIMENTOS,
        "file_pattern": "*.ESTABELE",
        "description": "Estabelecimentos"
    },
    "empresas": {
        "schema": EMPRESAS_SCHEMA,
        "table_name": TABLE_NAME_EMPRESAS,
        "file_pattern": "*.EMPRECSV",
        "description": "Empresas"
    }
}


# =============================================================================
# FUNÇÕES
# =============================================================================

def get_available_periods() -> List[str]:
    """
    Lista todos os períodos (ano-mês) disponíveis no bucket GCS
    """
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    blobs = bucket.list_blobs(prefix=f"{BASE_PATH}/", delimiter="/")
    _ = list(blobs)
    
    periods = set()
    
    if blobs.prefixes:
        for prefix in blobs.prefixes:
            match = re.search(r'(\d{4}-\d{2})/?$', prefix)
            if match:
                periods.add(match.group(1))
    
    if not periods:
        all_blobs = bucket.list_blobs(prefix=f"{BASE_PATH}/")
        for blob in all_blobs:
            match = re.search(rf'{BASE_PATH}/(\d{{4}}-\d{{2}})/', blob.name)
            if match:
                periods.add(match.group(1))
    
    return sorted(list(periods))


def create_load_job_config_temp(schema: List[bigquery.SchemaField]) -> bigquery.LoadJobConfig:
    """
    Cria configuração para o job de carga temporária (sem coluna ano_mes)
    
    Args:
        schema: Schema completo incluindo ano_mes
    """
    schema_without_ano_mes = [field for field in schema if field.name != "ano_mes"]
    
    return bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        skip_leading_rows=0,
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10000,
        encoding='ISO-8859-1',
        schema=schema_without_ano_mes,
    )


def create_final_table(client: bigquery.Client, data_type: str):
    """
    Cria a tabela final com o schema completo (incluindo ano_mes)
    
    Args:
        client: Cliente do BigQuery
        data_type: Tipo de dado ('estabelecimentos' ou 'empresas')
    """
    config = DATA_TYPES_CONFIG[data_type]
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{config['table_name']}"
    
    try:
        table = bigquery.Table(table_ref, schema=config['schema'])
        table = client.create_table(table)
        print(f"Tabela {config['table_name']} criada com sucesso!")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Tabela {config['table_name']} já existe")
        else:
            print(f"Aviso ao criar tabela: {e}")


def load_period_to_temp(client: bigquery.Client, ano_mes: str, data_type: str) -> Dict:
    """
    Carrega dados de um período específico para uma tabela temporária
    
    Args:
        client: Cliente do BigQuery
        ano_mes: Período no formato YYYY-MM
        data_type: Tipo de dado ('estabelecimentos' ou 'empresas')
    """
    config = DATA_TYPES_CONFIG[data_type]
    temp_table = f"{DATASET_ID}.temp_receita_{data_type}_{ano_mes.replace('-', '_')}"
    uri = f"gs://{BUCKET_NAME}/{BASE_PATH}/{ano_mes}/{config['file_pattern']}"
    
    print(f"Carregando {data_type} - {ano_mes} para tabela temporária...")
    
    job_config = create_load_job_config_temp(config['schema'])
    load_job = client.load_table_from_uri(uri, temp_table, job_config=job_config)
    
    try:
        load_job.result()
        rows = load_job.output_rows or 0
        print(f"{data_type} - {ano_mes}: {rows:,} linhas carregadas na tabela temporária")
        return {'status': 'success', 'rows': rows}
    except Exception as e:
        print(f"{data_type} - {ano_mes}: Erro - {str(e)[:100]}")
        return {'status': 'error', 'error': str(e)}


def insert_from_temp_to_final(client: bigquery.Client, ano_mes: str, data_type: str, is_first: bool) -> Dict:
    """
    Insere dados da tabela temporária na tabela final, adicionando coluna ano_mes
    
    Args:
        client: Cliente do BigQuery
        ano_mes: Período no formato YYYY-MM
        data_type: Tipo de dado ('estabelecimentos' ou 'empresas')
        is_first: Se True, usa WRITE_TRUNCATE; caso contrário, WRITE_APPEND
    """
    config = DATA_TYPES_CONFIG[data_type]
    temp_table = f"{DATASET_ID}.temp_receita_{data_type}_{ano_mes.replace('-', '_')}"
    final_table = f"{DATASET_ID}.{config['table_name']}"
    
    print(f"Inserindo {data_type} - {ano_mes} na tabela final...")
    
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
        print(f"{data_type} - {ano_mes}: {rows:,} linhas inseridas na tabela final")
        
        # Deletar tabela temporária
        client.delete_table(f"{PROJECT_ID}.{temp_table}", not_found_ok=True)
        print(f"{data_type} - {ano_mes}: Tabela temporária removida")
        
        return {'status': 'success', 'rows': rows}
    except Exception as e:
        print(f"{data_type} - {ano_mes}: Erro na inserção - {str(e)[:100]}")
        return {'status': 'error', 'error': str(e)}


def load_receita_data(data_types: Optional[List[str]] = None) -> Dict:
    """
    Carrega todos os dados da Receita Federal para o BigQuery
    
    Args:
        data_types: Lista de tipos de dados para carregar. Se None, carrega todos.
    """
    if data_types is None:
        data_types = list(DATA_TYPES_CONFIG.keys())
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Criar tabelas finais para cada tipo
    for data_type in data_types:
        create_final_table(client, data_type)
    
    periods = get_available_periods()
    
    if not periods:
        return {'status': 'error', 'error': 'Nenhum período encontrado'}
    
    all_results = {}
    total_rows_all = {}
    
    for data_type in data_types:
        print(f"\n{'=' * 80}")
        print(f"Processando {DATA_TYPES_CONFIG[data_type]['description']}")
        print(f"{'=' * 80}")
        
        results = []
        total_rows = 0
        
        for idx, ano_mes in enumerate(periods):
            print(f"[{idx + 1}/{len(periods)}] Processando {data_type} - {ano_mes}...")
            
            load_result = load_period_to_temp(client, ano_mes, data_type)
            
            if load_result['status'] == 'success':
                is_first = (idx == 0)
                insert_result = insert_from_temp_to_final(client, ano_mes, data_type, is_first)
                
                if insert_result['status'] == 'success':
                    total_rows += insert_result['rows']
                    results.append({
                        'period': ano_mes,
                        'status': 'success',
                        'rows': insert_result['rows']
                    })
                else:
                    results.append({
                        'period': ano_mes,
                        'status': 'error',
                        'error': insert_result['error']
                    })
            else:
                results.append({
                    'period': ano_mes,
                    'status': 'error',
                    'error': load_result['error']
                })
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        error_count = len(results) - success_count
        
        all_results[data_type] = {
            'status': 'success' if error_count == 0 else 'partial',
            'total_rows': total_rows,
            'periods_processed': success_count,
            'periods_failed': error_count,
            'results': results
        }
        total_rows_all[data_type] = total_rows
    
    # Status geral
    overall_status = 'success'
    for result in all_results.values():
        if result['status'] != 'success':
            overall_status = 'partial'
            break
    
    return {
        'status': overall_status,
        'data_types': all_results,
        'total_rows': total_rows_all
    }


def load_receita_by_period(ano_mes: str, data_types: Optional[List[str]] = None, append: bool = True) -> Dict:
    """
    Carrega dados de um período específico
    
    Args:
        ano_mes: Período no formato YYYY-MM
        data_types: Lista de tipos de dados para carregar. Se None, carrega todos.
        append: Se True, adiciona dados; se False, substitui
    """
    if data_types is None:
        data_types = list(DATA_TYPES_CONFIG.keys())
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Criar tabelas finais para cada tipo
    for data_type in data_types:
        create_final_table(client, data_type)
    
    results = {}
    
    for data_type in data_types:
        print(f"\nProcessando {DATA_TYPES_CONFIG[data_type]['description']} - {ano_mes}...")
        
        load_result = load_period_to_temp(client, ano_mes, data_type)
        
        if load_result['status'] != 'success':
            results[data_type] = load_result
            continue
        
        is_first = not append
        insert_result = insert_from_temp_to_final(client, ano_mes, data_type, is_first)
        results[data_type] = insert_result
    
    # Se apenas um tipo, retorna resultado direto; caso contrário, retorna dict
    if len(data_types) == 1:
        return results[data_types[0]]
    else:
        return results


# =============================================================================
# CLOUD FUNCTION HANDLER
# =============================================================================

@functions_framework.cloud_event
def load_receita_bigquery(cloud_event):
    """
    Handler da Cloud Function - Carrega dados da Receita Federal para BigQuery
    
    Suporta:
    - Estabelecimentos (arquivos *.ESTABELE)
    - Empresas (arquivos *.EMPRECSV)
    """
    try:
        # Decodificar mensagem do Pub/Sub
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        message_data = json.loads(message_data_str) if message_data_str else {}
        
        print(f"Iniciando carga de dados da Receita Federal para BigQuery")
        print(f"Projeto: {PROJECT_ID}")
        print(f"Dataset: {DATASET_ID}")
        print(f"Bucket: gs://{BUCKET_NAME}/{BASE_PATH}")
        
        # Determinar quais tipos de dados carregar
        data_type_param = message_data.get('data_type', 'all')
        if data_type_param == 'all':
            data_types = list(DATA_TYPES_CONFIG.keys())
        elif data_type_param in DATA_TYPES_CONFIG:
            data_types = [data_type_param]
        else:
            raise ValueError(f"Tipo de dado inválido: {data_type_param}. Use 'all', 'estabelecimentos' ou 'empresas'")
        
        print(f"Tipos de dados a carregar: {', '.join([DATA_TYPES_CONFIG[dt]['description'] for dt in data_types])}")
        
        # Verificar se é carga de período específico ou todos
        period = message_data.get('period')
        mode = message_data.get('mode', 'WRITE_APPEND')
        
        if period:
            # Carregar período específico
            print(f"Carregando período específico: {period}")
            append = (mode != 'WRITE_TRUNCATE')
            result = load_receita_by_period(period, data_types=data_types, append=append)
            
            # Formatar resposta
            if len(data_types) == 1:
                # Resposta simples para um único tipo
                response = {
                    'status': result.get('status', 'success'),
                    'data_type': data_types[0],
                    'period': period,
                    'mode': mode,
                    'rows': result.get('rows', 0),
                    'error': result.get('error')
                }
            else:
                # Resposta detalhada para múltiplos tipos
                response = {
                    'status': 'success' if all(r.get('status') == 'success' for r in result.values()) else 'partial',
                    'period': period,
                    'mode': mode,
                    'data_types': {
                        dt: {
                            'status': result[dt].get('status', 'success'),
                            'rows': result[dt].get('rows', 0),
                            'error': result[dt].get('error')
                        }
                        for dt in data_types
                    }
                }
        else:
            # Carregar todos os períodos
            print("Carregando todos os períodos disponíveis")
            result = load_receita_data(data_types=data_types)
            
            response = {
                'status': result.get('status', 'success'),
                'mode': 'WRITE_TRUNCATE (primeiro) + WRITE_APPEND (demais)',
                'data_types': result.get('data_types', {}),
                'total_rows': result.get('total_rows', {})
            }
        
        print(f"Carga concluída: {response.get('status', 'unknown')}")
        
        return response
        
    except Exception as e:
        error_response = {
            'status': 'error',
            'error': str(e)
        }
        print(f"Erro na carga: {e}")
        raise Exception(json.dumps(error_response))

