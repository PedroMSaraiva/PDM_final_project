#!/usr/bin/env python3
"""
Cloud Function para coletar indicadores econômicos do Banco Central
e carregar no BigQuery
"""
import os
import pandas as pd
import requests
import json
import base64
from datetime import datetime
from google.cloud import bigquery
import functions_framework


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PROJECT_ID = os.environ.get("PROJECT_ID", "trabalho-final-pdm-478021")
DATASET_ID = os.environ.get("DATASET_ID", "main_database")
TABLE_NAME = os.environ.get("TABLE_NAME", "banco_central_indicadores")

# --- Códigos das Séries do SGS do BCB (Mensais) ---
# Você pode buscar outros códigos na página do BCB/SGS
SERIES_BCB = {
    # === INDICADORES DE CUSTO ===
    4390: 'selic_meta_mensal',  # Taxa SELIC (custo de capital)

    # === INFLAÇÃO (afeta custos e margens) ===
    433: 'ipca_acumulado_12m',  # IPCA acumulado 12 meses
    13522: 'ipca_mensal',  # IPCA mensal (variação mais imediata)

    # === CRÉDITO E INADIMPLÊNCIA ===
    21082: 'inadimplencia_pj_livre',  # Inadimplência PJ - Recursos Livres
    20542: 'volume_credito_pj_total',  # Volume de crédito PJ total (R$ milhões)
    20714: 'spread_credito_pj',  # Spread médio das operações de crédito PJ

    # === CÂMBIO ===
    10813: 'cambio_dolar_media_mensal',  # Dólar - Média mensal de venda

    # === ATIVIDADE ECONÔMICA ===
    24363: 'ibc_br_dessazonalizado',  # IBC-Br (proxy do PIB mensal)

    # === CONFIANÇA E EXPECTATIVAS ===
    4394: 'icei',  # Índice de Confiança Empresarial (FGV)
    7341: 'nivel_utilizacao_capacidade',  # Nível de Utilização da Capacidade Instalada - Indústria

    # === MERCADO DE TRABALHO ===
    24369: 'taxa_desemprego',  # Taxa de desemprego (PNAD Contínua)
}

DATA_INICIO_COLETA = os.environ.get("DATA_INICIO", "01/01/2016")


# =============================================================================
# FUNÇÕES
# =============================================================================

def buscar_serie_temporal_bcb(codigo_serie, nome_coluna, data_inicio="01/01/2010"):
    """
    Busca uma série temporal no Banco Central do Brasil (BCB) via API do SGS.

    Args:
        codigo_serie (int): Código da série no SGS do BCB.
        nome_coluna (str): Nome a ser dado à coluna de dados no DataFrame.
        data_inicio (str): Data de início da busca no formato 'dd/mm/aaaa'.

    Returns:
        pd.DataFrame: DataFrame com as colunas 'ano_mes' e a série de dados.
    """
    # URL da API do SGS do BCB
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial={data_inicio}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        dados = response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar a série {nome_coluna} (Código {codigo_serie}): {e}")
        return pd.DataFrame()

    if not dados:
        print(f"A série {nome_coluna} (Código {codigo_serie}) retornou dados vazios.")
        return pd.DataFrame()

    # Cria o DataFrame a partir do JSON
    df = pd.DataFrame(dados)
    
    # Renomeia as colunas
    df.rename(columns={'valor': nome_coluna, 'data': 'data_completa'}, inplace=True)
    
    # Converte 'data' para o formato datetime
    df['data_completa'] = pd.to_datetime(df['data_completa'], format='%d/%m/%Y')
    
    # Cria a coluna 'ano_mes' no formato YYYY-MM
    df['ano_mes'] = df['data_completa'].dt.strftime('%Y-%m')
    
    # Converte valores para numérico
    df[nome_coluna] = pd.to_numeric(df[nome_coluna], errors='coerce')

    # ✅ SOLUÇÃO: Agrupa por ano_mes e pega a MÉDIA (ou último valor)
    # Para séries diárias, isso calcula a média mensal
    # Para séries já mensais, mantém o valor único
    df_mensal = df.groupby('ano_mes', as_index=False).agg({
        nome_coluna: 'mean'  # Usa 'mean' para média ou 'last' para último valor do mês
    })

    return df_mensal


def coletar_indicadores_economicos():
    """
    Coleta todos os indicadores econômicos do BCB
    
    Returns:
        pd.DataFrame: DataFrame consolidado com todos os indicadores
    """
    DATA_INICIO_COLETA = os.environ.get("DATA_INICIO", "01/01/2016")

    dfs_indicadores = []

    for codigo, nome in SERIES_BCB.items():
        print(f"Coletando série: {nome} (Código: {codigo})...")
        df_serie = buscar_serie_temporal_bcb(codigo, nome, DATA_INICIO_COLETA)
        if not df_serie.empty:
            dfs_indicadores.append(df_serie)

    if not dfs_indicadores:
        print("Nenhuma série foi coletada com sucesso.")
        return pd.DataFrame()

    # 1. Combina todos os DataFrames em um único
    df_final = dfs_indicadores[0]
    for i in range(1, len(dfs_indicadores)):
        df_final = pd.merge(df_final, dfs_indicadores[i], on='ano_mes', how='outer')

    # 2. Converte todas as colunas de valor para tipo numérico
    for col in df_final.columns:
        if col != 'ano_mes':
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # 3. Ordena o DataFrame por ano_mes
    df_final.sort_values(by='ano_mes', inplace=True)
    df_final.reset_index(drop=True, inplace=True)

    # ✅ VERIFICAÇÃO ADICIONAL: Remove duplicatas caso ainda existam
    df_final = df_final.drop_duplicates(subset=['ano_mes'], keep='first')

    print("\nColeta de Indicadores Econômicos Finalizada.")
    print(f"DataFrame Final (Shape: {df_final.shape}):")
    print(f"Período: {df_final['ano_mes'].min()} a {df_final['ano_mes'].max()}")
    print(f"Total de meses únicos: {df_final['ano_mes'].nunique()}")

    return df_final


def criar_schema_bigquery():
    """Cria schema da tabela no BigQuery"""
    schema = [
        bigquery.SchemaField("ano_mes", "STRING", mode="REQUIRED"),
    ]
    
    # Adicionar colunas para cada indicador
    for nome in SERIES_BCB.values():
        schema.append(
            bigquery.SchemaField(nome, "FLOAT64", mode="NULLABLE")
        )
    
    return schema


def carregar_no_bigquery(df, table_name=None, write_mode="WRITE_APPEND"):
    """
    Carrega DataFrame no BigQuery
    
    Args:
        df: DataFrame pandas com os dados
        table_name: Nome da tabela (opcional, usa TABLE_NAME se None)
        write_mode: Modo de escrita (WRITE_APPEND ou WRITE_TRUNCATE)
        
    Returns:
        Dict com status e estatísticas
    """
    if df.empty:
        print("⚠️  DataFrame vazio, nada para carregar")
        return {'status': 'skipped', 'rows': 0}
    
    # Se não foi especificado um nome de tabela, usa o padrão
    table_name = table_name or TABLE_NAME
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configurar job
    job_config = bigquery.LoadJobConfig(
        schema=criar_schema_bigquery(),
        write_disposition=write_mode,
        source_format=bigquery.SourceFormat.PARQUET,  # Mais eficiente que CSV
    )
    
    print(f"\n📊 Carregando {len(df)} linhas no BigQuery...")
    print(f"   Tabela: {table_ref}")
    print(f"   Modo: {write_mode}")
    
    # Converter DataFrame para Parquet em memória
    from io import BytesIO
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    # Upload para BigQuery
    load_job = client.load_table_from_file(
        buffer,
        table_ref,
        job_config=job_config
    )
    
    try:
        load_job.result()
        rows = load_job.output_rows or len(df)
        print(f"✅ Sucesso! {rows:,} linhas carregadas")
        
        return {
            'status': 'success',
            'rows': rows,
            'job_id': load_job.job_id,
            'table': table_name
        }
    except Exception as e:
        print(f"❌ Erro ao carregar no BigQuery: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'rows': 0,
            'table': table_name
        }


# =============================================================================
# CLOUD FUNCTION HANDLERS
# =============================================================================

@functions_framework.http
def banco_central_http(request):
    """Handler HTTP - processa indicadores econômicos"""
    print('=' * 80)
    print('COLETAR INDICADORES ECONÔMICOS - BANCO CENTRAL')
    print('=' * 80)
    
    # Coletar indicadores
    df = coletar_indicadores_economicos()
    
    if df.empty:
        return {'error': 'Nenhum dado coletado'}, 400
    
    print(f"\n📊 Dados coletados: {len(df)} linhas")
    print(f"   Período: {df['ano_mes'].min()} até {df['ano_mes'].max()}")
    
    # Determinar modo de escrita
    write_mode = request.args.get('mode', 'WRITE_APPEND')
    
    # Carregar versão bronze (dados brutos)
    bronze_result = carregar_no_bigquery(df, f"{TABLE_NAME}_bronze", write_mode)
    
    # Criar versão silver (dados tratados - preenchimento de nulos)
    df_silver = df.fillna(df.mean(numeric_only=True))
    silver_result = carregar_no_bigquery(df_silver, f"{TABLE_NAME}_silver", write_mode)
    
    return {
        'status': 'success' if bronze_result['status'] == 'success' and silver_result['status'] == 'success' else 'partial',
        'bronze': {
            'status': bronze_result['status'],
            'rows': bronze_result['rows']
        },
        'silver': {
            'status': silver_result['status'],
            'rows': silver_result['rows']
        },
        'data_shape': df.shape,
        'period': {
            'start': df['ano_mes'].min(),
            'end': df['ano_mes'].max()
        }
    }, 200


@functions_framework.cloud_event
def banco_central_pubsub(cloud_event):
    """Handler Pub/Sub - processa indicadores econômicos (para agendamento)"""
    try:
        message_data_str = base64.b64decode(
            cloud_event.data["message"]["data"]
        ).decode("utf-8")
        message_data = json.loads(message_data_str) if message_data_str else {}
    except Exception:
        message_data = {}
    
    print('=' * 80)
    print('COLETAR INDICADORES ECONÔMICOS - BANCO CENTRAL (Pub/Sub)')
    print('=' * 80)
    
    # Coletar indicadores
    df = coletar_indicadores_economicos()
    
    if df.empty:
        return {'status': 'error', 'error': 'Nenhum dado coletado'}
    
    print(f"\n📊 Dados coletados: {len(df)} linhas")
    print(f"   Período: {df['ano_mes'].min()} até {df['ano_mes'].max()}")
    
    # Modo de escrita da mensagem
    write_mode = message_data.get('mode', 'WRITE_APPEND')
    
    # Carregar versão bronze (dados brutos)
    bronze_result = carregar_no_bigquery(df, f"{TABLE_NAME}_bronze", write_mode)
    
    # Criar versão silver (dados tratados - preenchimento de nulos)
    df_silver = df.fillna(df.mean(numeric_only=True))
    silver_result = carregar_no_bigquery(df_silver, f"{TABLE_NAME}_silver", write_mode)
    
    return {
        'status': 'success' if bronze_result['status'] == 'success' and silver_result['status'] == 'success' else 'partial',
        'bronze': bronze_result,
        'silver': silver_result,
        'rows': len(df),
        'period': {
            'start': df['ano_mes'].min(),
            'end': df['ano_mes'].max()
        }
    }


if __name__ == '__main__':
    # Teste local
    from flask import Request
    request = Request.from_values()
    banco_central_http(request)