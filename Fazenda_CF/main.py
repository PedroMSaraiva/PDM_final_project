#!/usr/bin/env python3
"""
Cloud Function para baixar dados da PGFN (Procuradoria-Geral da Fazenda Nacional)
Salva diretamente no Google Cloud Storage
Baixa, extrai e deleta ZIPs automaticamente
"""

import os
import io
import zipfile
import json
import base64
from typing import List, Tuple, Dict
from pathlib import Path

import requests
from google.cloud import storage
import functions_framework


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
BASE_URL = "https://dadosabertos.pgfn.gov.br"
DESTINATION_BUCKET_NAME = os.environ.get('DESTINATION_BUCKET_NAME', 'seu-bucket-aqui')
BASE_PATH = os.environ.get('BASE_PATH', 'fazenda_nacional')  # Caminho base no bucket

# Tipos de dados disponíveis
DATA_TYPES = [
    "Dados_abertos_Nao_Previdenciario",
    "Dados_abertos_FGTS",
    "Dados_abertos_Previdenciario"
]

# Configurações de download
MAX_RETRIES = 3
TIMEOUT = (30, 500)  # (connect timeout, read timeout)
CHUNK_SIZE = 8192

# Anos e trimestres
START_YEAR = int(os.environ.get('START_YEAR', '2020'))
END_YEAR = int(os.environ.get('END_YEAR', '2025'))
# END_QUARTER = int(os.environ.get('END_QUARTER', '3'))  # Para o último ano

# Inicializar cliente do Storage
storage_client = storage.Client()
bucket = storage_client.bucket(DESTINATION_BUCKET_NAME)


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def get_downloads_list() -> List[Tuple[int, int, str]]:
    """
    Gera lista de downloads (ano, trimestre, tipo)
    Todos os anos: 4 trimestres cada (START_YEAR até END_YEAR inclusive)
    """
    downloads = []
    
    # Todos os anos completos (4 trimestres cada)
    for year in range(START_YEAR, END_YEAR + 1):  # +1 para incluir END_YEAR
        for quarter in range(1, 5):  # 4 trimestres
            for data_type in DATA_TYPES:
                downloads.append((year, quarter, data_type))
    
    return downloads


def build_url(year: int, quarter: int, data_type: str) -> str:
    """Constrói a URL de download baseada no padrão"""
    quarter_str = f"{quarter:02d}"
    return f"{BASE_URL}/{year}_trimestre_{quarter_str}/{data_type}.zip"


def get_blob_path(year: int, quarter: int, data_type: str, filename: str = None) -> str:
    """
    Retorna o caminho do blob no bucket
    Estrutura: BASE_PATH/ano/trimestre/tipo/arquivo.csv
    """
    type_short = data_type.replace("Dados_abertos_", "")
    
    if filename:
        return f"{BASE_PATH}/{year}/{quarter}trimestre/{type_short}/{filename}"
    else:
        return f"{BASE_PATH}/{year}/{quarter}trimestre/{type_short}"


def blob_exists(blob_path: str) -> bool:
    """Verifica se um blob existe no bucket"""
    blob = bucket.blob(blob_path)
    return blob.exists()


def check_extraction_marker(year: int, quarter: int, data_type: str) -> bool:
    """Verifica se existe marcador de extração"""
    marker_path = get_blob_path(year, quarter, data_type, '.extracted')
    return blob_exists(marker_path)


def create_extraction_marker(year: int, quarter: int, data_type: str):
    """Cria marcador de extração no bucket"""
    marker_path = get_blob_path(year, quarter, data_type, '.extracted')
    blob = bucket.blob(marker_path)
    blob.upload_from_string('extracted', content_type='text/plain')


def list_csv_files_in_path(year: int, quarter: int, data_type: str) -> List[str]:
    """Lista arquivos CSV já existentes no caminho do bucket"""
    prefix = get_blob_path(year, quarter, data_type, None)
    blobs = bucket.list_blobs(prefix=prefix)
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    return csv_files


def download_and_extract_to_gcs(
    url: str, 
    year: int, 
    quarter: int, 
    data_type: str,
    retry_count: int = 0
) -> Tuple[bool, bool]:
    """
    Baixa ZIP, extrai conteúdo para GCS e deleta ZIP (em memória)
    Retorna: (download_success, extraction_success)
    """
    filename = f"{data_type}.zip"
    
    try:
        # Verificar se já foi extraído
        if check_extraction_marker(year, quarter, data_type):
            print(f"   ✓ Já extraído anteriormente")
            return (True, True)
        
        # Verificar se já existem CSVs
        existing_csvs = list_csv_files_in_path(year, quarter, data_type)
        if existing_csvs:
            print(f"   ✓ {len(existing_csvs)} arquivos CSV já existem, pulando...")
            create_extraction_marker(year, quarter, data_type)
            return (True, True)
        
        retry_msg = f" (tentativa {retry_count + 1}/{MAX_RETRIES})" if retry_count > 0 else ""
        print(f"   ⬇️  Baixando{retry_msg}: {filename}")
        
        # Download do arquivo ZIP em memória
        response = requests.get(url, stream=True, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Ler conteúdo do ZIP em memória
        zip_content = io.BytesIO()
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                zip_content.write(chunk)
                downloaded += len(chunk)
                if total_size > 0 and downloaded % (CHUNK_SIZE * 100) == 0:
                    percent = (downloaded / total_size) * 100
                    print(f'   Progress: {percent:.1f}%')
        
        print(f"   ✓ Download concluído: {downloaded / 1024 / 1024:.1f} MB")
        
        # Verificar integridade do ZIP
        zip_content.seek(0)
        try:
            with zipfile.ZipFile(zip_content, 'r') as zf:
                if zf.testzip() is not None:
                    raise zipfile.BadZipFile("Arquivo corrompido")
        except zipfile.BadZipFile as e:
            print(f"   ✗ ZIP corrompido: {e}")
            
            # Tentar novamente se ainda tiver tentativas
            if retry_count < MAX_RETRIES - 1:
                print(f"   🔄 Tentando novamente...")
                return download_and_extract_to_gcs(url, year, quarter, data_type, retry_count + 1)
            else:
                return (False, False)
        
        # Extrair e fazer upload dos arquivos
        print(f"   📦 Extraindo e enviando para GCS...")
        zip_content.seek(0)
        
        with zipfile.ZipFile(zip_content, 'r') as zip_ref:
            members = zip_ref.namelist()
            files_uploaded = 0
            
            for member in members:
                if not member.endswith('/'):  # Ignorar diretórios
                    # Ler arquivo do ZIP
                    file_data = zip_ref.read(member)
                    
                    # Extrair apenas o nome do arquivo (sem caminhos internos do ZIP)
                    member_name = Path(member).name
                    
                    # Upload para GCS
                    blob_path = get_blob_path(year, quarter, data_type, member_name)
                    blob = bucket.blob(blob_path)
                    blob.upload_from_string(file_data, content_type='text/csv')
                    
                    files_uploaded += 1
                    
                    if files_uploaded % 5 == 0:
                        print(f'   ... {files_uploaded}/{len(members)} arquivos enviados')
            
            print(f"   ✅ Extraído: {files_uploaded} arquivos")
            
            # Criar marcador de extração
            create_extraction_marker(year, quarter, data_type)
        
        # ZIP é automaticamente deletado (estava em memória)
        print(f"   🗑️  ZIP removido da memória")
        
        return (True, True)
        
    except requests.exceptions.Timeout:
        print(f"   ✗ Timeout ao baixar {url}")
        
        # Tentar novamente
        if retry_count < MAX_RETRIES - 1:
            print(f"   🔄 Tentando novamente...")
            return download_and_extract_to_gcs(url, year, quarter, data_type, retry_count + 1)
        else:
            return (False, False)
            
    except requests.exceptions.RequestException as e:
        print(f"   ✗ Erro ao baixar {url}: {e}")
        
        # Tentar novamente
        if retry_count < MAX_RETRIES - 1:
            print(f"   🔄 Tentando novamente...")
            return download_and_extract_to_gcs(url, year, quarter, data_type, retry_count + 1)
        else:
            return (False, False)
            
    except Exception as e:
        print(f"   ✗ Erro inesperado: {str(e)[:100]}")
        return (False, False)


def process_downloads(downloads: List[Tuple[int, int, str]]) -> Dict[str, int]:
    """
    Processa lista de downloads
    Retorna estatísticas
    """
    total = len(downloads)
    stats = {
        'total': total,
        'successful_downloads': 0,
        'failed_downloads': 0,
        'successful_extractions': 0,
        'failed_extractions': 0
    }
    
    print(f"\n📋 Total de arquivos para processar: {total}\n")
    
    for idx, (year, quarter, data_type) in enumerate(downloads, 1):
        print(f"[{idx}/{total}] {year} - Trimestre {quarter} - {data_type}")
        
        url = build_url(year, quarter, data_type)
        download_ok, extract_ok = download_and_extract_to_gcs(url, year, quarter, data_type)
        
        if download_ok:
            stats['successful_downloads'] += 1
            if extract_ok:
                stats['successful_extractions'] += 1
            else:
                stats['failed_extractions'] += 1
        else:
            stats['failed_downloads'] += 1
        
        print()  # Linha em branco entre items
    
    return stats


# =============================================================================
# CLOUD FUNCTION HANDLERS
# =============================================================================

@functions_framework.http
def download_fazenda_http(request):
    """
    Handler HTTP - processa todos os downloads configurados
    """
    print('=' * 80)
    print('DOWNLOAD PGFN - CLOUD FUNCTION')
    print('=' * 80)
    print(f'Bucket destino: {DESTINATION_BUCKET_NAME}')
    print(f'Caminho base: {BASE_PATH}')
    print(f'Período: {START_YEAR} até {END_YEAR} (trimestre {END_QUARTER})')
    print()
    
    # Gerar lista de downloads
    downloads = get_downloads_list()
    
    # Processar downloads
    stats = process_downloads(downloads)
    
    # Resumo final
    print('=' * 80)
    print('RESUMO FINAL')
    print('=' * 80)
    print(f"📊 Estatísticas:")
    print(f"   Total de arquivos:        {stats['total']}")
    print(f"   ✅ Downloads bem-sucedidos: {stats['successful_downloads']}")
    print(f"   ❌ Downloads falhos:        {stats['failed_downloads']}")
    print(f"   📦 Extrações bem-sucedidas: {stats['successful_extractions']}")
    print(f"   ❌ Extrações falhas:        {stats['failed_extractions']}")
    print()
    print(f"📁 Arquivos salvos em: gs://{DESTINATION_BUCKET_NAME}/{BASE_PATH}/")
    
    if stats['failed_downloads'] > 0:
        print("\n⚠️  Alguns downloads falharam. Execute novamente para tentar os que falharam.")
    elif stats['failed_extractions'] > 0:
        print("\n⚠️  Alguns arquivos não puderam ser extraídos.")
    else:
        print("\n✅ Todos os arquivos foram baixados, extraídos e os ZIPs foram removidos!")
    
    return stats, 200


@functions_framework.cloud_event
def download_fazenda_pubsub(cloud_event):
    """
    Handler Pub/Sub - processa download específico ou todos
    Mensagem esperada: {"year": 2024, "quarter": 3, "data_type": "Dados_abertos_FGTS"}
    Ou mensagem vazia {} para processar todos
    """
    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        message_data = json.loads(message_data_str) if message_data_str else {}
    except Exception as e:
        print(f"Erro ao decodificar mensagem: {e}")
        message_data = {}
    
    print('=' * 80)
    print('DOWNLOAD PGFN - CLOUD FUNCTION (Pub/Sub)')
    print('=' * 80)
    print(f'Bucket destino: {DESTINATION_BUCKET_NAME}')
    print(f'Caminho base: {BASE_PATH}')
    
    # Se especificou download específico
    if all(k in message_data for k in ['year', 'quarter', 'data_type']):
        year = message_data['year']
        quarter = message_data['quarter']
        data_type = message_data['data_type']
        
        print(f'\n📥 Processando: {year} - Q{quarter} - {data_type}')
        
        url = build_url(year, quarter, data_type)
        download_ok, extract_ok = download_and_extract_to_gcs(url, year, quarter, data_type)
        
        if download_ok and extract_ok:
            print(f'\n✅ Arquivo processado com sucesso!')
        else:
            print(f'\n❌ Falha no processamento')
    else:
        # Processar todos
        downloads = get_downloads_list()
        print(f'\n📋 Processando todos os {len(downloads)} arquivos')
        stats = process_downloads(downloads)
        print(f'\n✅ Processamento concluído!')
        print(f'   Sucessos: {stats["successful_downloads"]}/{stats["total"]}')
    
    print('Processamento concluído.')


@functions_framework.http
def download_fazenda_single(request):
    """
    Handler HTTP para download único via query params
    Exemplo: ?year=2024&quarter=3&type=Dados_abertos_FGTS
    """
    try:
        year = int(request.args.get('year', START_YEAR))
        quarter = int(request.args.get('quarter', 1))
        data_type = request.args.get('type', DATA_TYPES[0])
        
        print(f'📥 Download único: {year} - Q{quarter} - {data_type}')
        
        url = build_url(year, quarter, data_type)
        download_ok, extract_ok = download_and_extract_to_gcs(url, year, quarter, data_type)
        
        result = {
            'year': year,
            'quarter': quarter,
            'data_type': data_type,
            'download_success': download_ok,
            'extraction_success': extract_ok
        }
        
        if download_ok and extract_ok:
            return result, 200
        else:
            return result, 500
            
    except Exception as e:
        return {'error': str(e)}, 400


if __name__ == '__main__':
    # Para testes locais
    class MockRequest:
        args = {}
    
    download_fazenda_http(MockRequest())

