#!/usr/bin/env python3
"""
Cloud Function para carregar dados da Fazenda Nacional (PGFN) do GCS para o BigQuery

Trigger: Pub/Sub
Mensagem esperada:
  - {} (vazio) - Carrega todos os dados (WRITE_TRUNCATE)
  - {"mode": "WRITE_APPEND"} - Adiciona dados sem apagar existentes
  - {"mode": "WRITE_TRUNCATE"} - Substitui dados existentes
"""

import os
import json
import base64
from typing import Dict, List

from google.cloud import bigquery
import functions_framework


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PROJECT_ID = os.environ.get('PROJECT_ID', 'trabalho-final-pdm-478021')
DATASET_ID = os.environ.get('DATASET_ID', 'main_database')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'dados-cnpjs')
BASE_PATH = os.environ.get('BASE_PATH', 'fazenda_nacional')

# Mapeamento: tipo de dado → nome da tabela no BigQuery
DATA_TYPES = {
    "Nao_Previdenciario": "pgfn_nao_previdenciario",
    "FGTS": "pgfn_fgts",
    "Previdenciario": "pgfn_previdenciario"
}


# =============================================================================
# FUNÇÕES
# =============================================================================

def create_load_job_config(write_mode: str = "WRITE_TRUNCATE") -> bigquery.LoadJobConfig:
    """
    Cria configuração para o job de carga
    """
    return bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        skip_leading_rows=1,
        write_disposition=write_mode,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=1000,
        schema=[
            bigquery.SchemaField("cpf_cnpj", "STRING"),
            bigquery.SchemaField("tipo_pessoa", "STRING"),
            bigquery.SchemaField("tipo_devedor", "STRING"),
            bigquery.SchemaField("nome_devedor", "STRING"),
            bigquery.SchemaField("uf_devedor", "STRING"),
            bigquery.SchemaField("unidade_responsavel", "STRING"),
            bigquery.SchemaField("numero_inscricao", "STRING"),
            bigquery.SchemaField("tipo_situacao_inscricao", "STRING"),
            bigquery.SchemaField("situacao_inscricao", "STRING"),
            bigquery.SchemaField("receita_principal", "STRING"),
            bigquery.SchemaField("data_inscricao", "STRING"),
            bigquery.SchemaField("indicador_ajuizado", "STRING"),
            bigquery.SchemaField("valor_consolidado", "STRING"),
        ]
    )


def load_data_type(
    client: bigquery.Client,
    data_type: str,
    table_name: str,
    write_mode: str = "WRITE_TRUNCATE"
) -> bigquery.LoadJob:
    """
    Carrega dados de um tipo específico para o BigQuery
    """
    # Construir URIs para todos os anos e trimestres
    uris = []
    years = range(2020, 2026)  # 2020 até 2025
    quarters = [1, 2, 3, 4]
    
    for year in years:
        for quarter in quarters:
            uri = f"gs://{BUCKET_NAME}/{BASE_PATH}/{year}/{quarter}trimestre/{data_type}/*.csv"
            uris.append(uri)
    
    # Criar referência da tabela
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configurar job
    job_config = create_load_job_config(write_mode)
    
    # Iniciar job de carga
    load_job = client.load_table_from_uri(
        uris,
        table_ref,
        job_config=job_config
    )
    
    return load_job


def wait_for_job(load_job: bigquery.LoadJob, data_type: str) -> Dict:
    """
    Aguarda conclusão do job e retorna estatísticas
    """
    try:
        load_job.result()
        
        stats = {
            'data_type': data_type,
            'status': 'success',
            'output_rows': load_job.output_rows or 0,
            'job_id': load_job.job_id,
            'errors': None
        }
        
        return stats
        
    except Exception as e:
        stats = {
            'data_type': data_type,
            'status': 'error',
            'output_rows': 0,
            'job_id': load_job.job_id,
            'errors': str(e)
        }
        
        return stats


def load_all_data(write_mode: str = "WRITE_TRUNCATE") -> List[Dict]:
    """
    Carrega todos os tipos de dados da Fazenda Nacional para o BigQuery
    """
    # Inicializar cliente
    client = bigquery.Client(project=PROJECT_ID)
    
    # Lista para armazenar jobs
    jobs = []
    
    # Iniciar jobs de carga para cada tipo
    for data_type, table_name in DATA_TYPES.items():
        try:
            load_job = load_data_type(client, data_type, table_name, write_mode)
            jobs.append((load_job, data_type))
        except Exception as e:
            print(f"Erro ao iniciar job para {data_type}: {e}")
    
    # Aguardar conclusão de todos os jobs
    results = []
    for load_job, data_type in jobs:
        stats = wait_for_job(load_job, data_type)
        results.append(stats)
    
    return results


# =============================================================================
# CLOUD FUNCTION HANDLER
# =============================================================================

@functions_framework.cloud_event
def load_fazenda_bigquery(cloud_event):
    """
    Handler da Cloud Function - Carrega dados da Fazenda Nacional para BigQuery
    """
    try:
        # Decodificar mensagem do Pub/Sub
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        message_data = json.loads(message_data_str) if message_data_str else {}
        
        # Obter modo de escrita (padrão: WRITE_TRUNCATE)
        write_mode = message_data.get('mode', 'WRITE_TRUNCATE')
        
        print(f"Iniciando carga de dados da Fazenda Nacional para BigQuery")
        print(f"Projeto: {PROJECT_ID}")
        print(f"Dataset: {DATASET_ID}")
        print(f"Bucket: gs://{BUCKET_NAME}/{BASE_PATH}")
        print(f"Modo: {write_mode}")
        
        # Executar carga
        results = load_all_data(write_mode)
        
        # Calcular estatísticas
        total_rows = sum(r['output_rows'] for r in results)
        success_count = sum(1 for r in results if r['status'] == 'success')
        error_count = len(results) - success_count
        
        # Preparar resposta
        response = {
            'status': 'success' if error_count == 0 else 'partial',
            'write_mode': write_mode,
            'tables_processed': len(results),
            'tables_success': success_count,
            'tables_errors': error_count,
            'total_rows': total_rows,
            'results': results
        }
        
        print(f"Carga concluída: {success_count}/{len(results)} tabelas com sucesso")
        print(f"Total de linhas: {total_rows:,}")
        
        return response
        
    except Exception as e:
        error_response = {
            'status': 'error',
            'error': str(e)
        }
        print(f"Erro na carga: {e}")
        raise Exception(json.dumps(error_response))

