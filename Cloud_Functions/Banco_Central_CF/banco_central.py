import pandas as pd
import requests
import io
from datetime import datetime, timedelta

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
        response = requests.get(url)
        response.raise_for_status() # Lança exceção para status codes HTTP 4xx/5xx
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
    
    # Seleciona as colunas finais
    return df[['ano_mes', nome_coluna]]


# --- Códigos das Séries do SGS do BCB (Mensais) ---
# Você pode buscar outros códigos na página do BCB/SGS
SERIES_BCB = {
    # 1. Indicador de Custo (SELIC)
    4390: 'selic_meta_mensal', # Taxa de juros - Selic (Meta - ao ano)

    # 2. Indicador de Inflação
    433: 'ipca_acumulado_12m', # IPCA - Índice de Preços ao Consumidor Amplo (Acumulado em 12 meses)
    
    # 3. Indicador de Crédito/Inadimplência
    21082: 'inadimplencia_pj_livre', # Taxa de Inadimplência de Pessoa Jurídica - Recursos Livres
    
    # 4. Indicador de Câmbio (A partir de 1989 - Média de Venda)
    10813: 'cambio_dolar_venda', 
    
    # 5. Indicador de Atividade Econômica (Proxy do PIB)
    24363: 'ibc_br_dessazonalizado', # IBC-Br - Índice de Atividade Econômica do BC (dessazonalizado)
}


# --- Execução Principal ---
def coletar_indicadores_economicos():
    
    # Defina a data de início da coleta (ex: para incluir todo o histórico necessário)
    DATA_INICIO_COLETA = "01/01/2016"
    
    # Lista para armazenar todos os DataFrames de cada série
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
        # Faz o merge usando 'ano_mes' como chave
        df_final = pd.merge(df_final, dfs_indicadores[i], on='ano_mes', how='outer')

    # 2. Converte todas as colunas de valor para tipo numérico
    for col in df_final.columns:
        if col != 'ano_mes':
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # 3. Ordena o DataFrame por ano_mes
    df_final.sort_values(by='ano_mes', inplace=True)
    df_final.reset_index(drop=True, inplace=True)
    
    print("\nColeta de Indicadores Econômicos Finalizada.")
    print(f"DataFrame Final (Shape: {df_final.shape}):")
    return df_final

# Chama a função para obter o DataFrame final
df_indicadores = coletar_indicadores_economicos()

# Exibe o resultado
df_indicadores.head()