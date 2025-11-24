#!/usr/bin/env python3
"""
Loader para carregar dados da PGFN (Fazenda Nacional) do GCS para o BigQuery

Estrutura dos dados no bucket:
gs://bucket/fazenda_nacional/
├── 2020/
│   ├── 1trimestre/
│   │   ├── Nao_Previdenciario/*.csv
│   │   ├── FGTS/*.csv
│   │   └── Previdenciario/*.csv
│   └── 2trimestre/...
└── 2021/...

Cria 3 tabelas separadas no BigQuery:
- pgfn_nao_previdenciario
- pgfn_fgts
- pgfn_previdenciario
"""

from google.cloud import bigquery
from typing import Dict, List
import time


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PROJECT_ID = "trabalho-final-pdm-478021"
DATASET_ID = "main_database"
BUCKET_NAME = "dados-cnpjs"
BASE_PATH = "fazenda_nacional"

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
    
    Args:
        write_mode: WRITE_TRUNCATE (substitui) ou WRITE_APPEND (adiciona)
    """
    return bigquery.LoadJobConfig(
        autodetect=False,  # Auto-detecta schema
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        skip_leading_rows=1,  # Pula cabeçalho
        write_disposition=write_mode,
        allow_jagged_rows=True,  # Permite linhas com colunas faltando
        allow_quoted_newlines=True,  # Permite quebras de linha em campos entre aspas
        ignore_unknown_values=True,  # Ignora valores desconhecidos
        max_bad_records=1000,  # Permite até 1000 registros ruins
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
    Carrega dados de um tipo específico (Nao_Previdenciario, FGTS, ou Previdenciario)
    
    Consolida todos os CSVs de todos os anos e trimestres em uma única tabela.
    
    Args:
        client: Cliente do BigQuery
        data_type: Tipo de dado (ex: "Nao_Previdenciario")
        table_name: Nome da tabela no BigQuery
        write_mode: Modo de escrita (WRITE_TRUNCATE ou WRITE_APPEND)
    
    Returns:
        Job de carga do BigQuery
    """
    print(f"\n{'=' * 80}")
    print(f"📊 Carregando: {data_type}")
    print(f"{'=' * 80}")
    
    # BigQuery não suporta wildcards aninhados, então especificamos explicitamente
    # cada combinação de ano + trimestre
    uris = []
    years = range(2020, 2026)  # 2020 até 2025
    quarters = [1, 2, 3, 4]
    
    for year in years:
        for quarter in quarters:
            uri = f"gs://{BUCKET_NAME}/{BASE_PATH}/{year}/{quarter}trimestre/{data_type}/*.csv"
            uris.append(uri)
    
    print(f"URIs: {len(uris)} combinações (anos × trimestres)")
    print(f"  Anos: {min(years)} - {max(years)} ({len(list(years))} anos)")
    print(f"  Trimestres: {quarters}")
    print(f"  Exemplo: {uris[0]}")
    print(f"  Exemplo: {uris[-1]}")
    print(f"Tabela: {DATASET_ID}.{table_name}")
    print(f"Modo: {write_mode}")
    
    # Criar referência da tabela
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configurar job
    job_config = create_load_job_config(write_mode)
    
    # Iniciar job de carga com múltiplas URIs (consolida tudo em uma única tabela)
    load_job = client.load_table_from_uri(
        uris,  # Lista de URIs - BigQuery vai consolidar tudo em uma tabela
        table_ref,
        job_config=job_config
    )
    
    print(f"Job ID: {load_job.job_id}")
    print(f"Status: Carregando...")
    
    return load_job


def wait_for_job(load_job: bigquery.LoadJob, data_type: str) -> Dict:
    """
    Aguarda conclusão do job e retorna estatísticas
    
    Args:
        load_job: Job de carga
        data_type: Tipo de dado sendo carregado
    
    Returns:
        Dicionário com estatísticas
    """
    try:
        # Aguardar conclusão
        load_job.result()
        
        # Coletar estatísticas
        stats = {
            'data_type': data_type,
            'status': 'success',
            'output_rows': load_job.output_rows or 0,
            'job_id': load_job.job_id,
            'errors': None
        }
        
        print(f"✅ Sucesso!")
        print(f"   Linhas carregadas: {stats['output_rows']:,}")
        
        return stats
        
    except Exception as e:
        stats = {
            'data_type': data_type,
            'status': 'error',
            'output_rows': 0,
            'job_id': load_job.job_id,
            'errors': str(e)
        }
        
        print(f"❌ Erro: {e}")
        
        # Tentar obter mais detalhes do erro
        if hasattr(load_job, 'errors') and load_job.errors:
            print(f"   Detalhes: {load_job.errors[:3]}")  # Primeiros 3 erros
        
        return stats


def load_all_data(write_mode: str = "WRITE_TRUNCATE") -> List[Dict]:
    """
    Carrega todos os tipos de dados da Fazenda Nacional para o BigQuery
    
    Args:
        write_mode: Modo de escrita (WRITE_TRUNCATE ou WRITE_APPEND)
    
    Returns:
        Lista com estatísticas de cada carga
    """
    print("=" * 80)
    print("CARREGAR DADOS PGFN (FAZENDA NACIONAL) PARA BIGQUERY")
    print("=" * 80)
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Bucket: gs://{BUCKET_NAME}/{BASE_PATH}")
    print(f"Modo: {write_mode}")
    print()
    
    # Inicializar cliente
    client = bigquery.Client(project=PROJECT_ID)
    
    # Lista para armazenar jobs
    jobs = []
    
    # Iniciar jobs de carga para cada tipo (em paralelo)
    print("📤 Iniciando jobs de carga...")
    for data_type, table_name in DATA_TYPES.items():
        try:
            load_job = load_data_type(client, data_type, table_name, write_mode)
            jobs.append((load_job, data_type))
        except Exception as e:
            print(f"❌ Erro ao iniciar job para {data_type}: {e}")
    
    print(f"\n✅ {len(jobs)} jobs iniciados!")
    print("\n⏳ Aguardando conclusão dos jobs...\n")
    
    # Aguardar conclusão de todos os jobs
    results = []
    for load_job, data_type in jobs:
        stats = wait_for_job(load_job, data_type)
        results.append(stats)
    
    return results


def print_summary(results: List[Dict]):
    """Imprime resumo dos resultados"""
    print("\n" + "=" * 80)
    print("RESUMO")
    print("=" * 80)
    
    total_rows = 0
    success_count = 0
    error_count = 0
    
    for result in results:
        status_icon = "✅" if result['status'] == 'success' else "❌"
        print(f"{status_icon} {result['data_type']}: {result['output_rows']:,} linhas")
        
        total_rows += result['output_rows']
        if result['status'] == 'success':
            success_count += 1
        else:
            error_count += 1
            if result['errors']:
                print(f"   Erro: {result['errors'][:200]}")
    
    print()
    print(f"Total de tabelas: {len(results)}")
    print(f"✅ Sucesso: {success_count}")
    print(f"❌ Erros: {error_count}")
    print(f"📊 Total de linhas: {total_rows:,}")
    print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Função principal"""
    start_time = time.time()
    
    # Carregar dados (WRITE_TRUNCATE = substitui dados existentes)
    # Use WRITE_APPEND para adicionar dados sem apagar os existentes
    results = load_all_data(write_mode="WRITE_TRUNCATE")
    
    # Imprimir resumo
    print_summary(results)
    
    elapsed_time = time.time() - start_time
    print(f"⏱️  Tempo total: {elapsed_time:.1f}s ({elapsed_time/60:.1f} min)")
    print()
    print("🎉 Processo concluído!")


if __name__ == "__main__":
    main()